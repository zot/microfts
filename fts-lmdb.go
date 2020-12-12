package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AskAlexSharov/lmdb-go/lmdb"
)

/*

* GRAMS
KEY IS THE GRAM
VALUE IS AN ARRAY OF 4-byte OIDs

* OBJECTS
KEY IS THE OID
VALUE IS [len][name][len][data][gram...]

* NAMES
KEY IS THE NAME
VALUE IS THE 4-byte OID

* COMPRESSED NUMBERS THAT COMPARE LEXICOGRAPHICALLY

 7 bits (1 byte):  0xxxxxxx
12 bits (2 bytes): 1000xxxx X
20 bits (3 bytes): 1001xxxx X X
28 bits (4 bytes): 1010xxxx X X X
36 bits (5 bytes): 1011xxxx X X X X
44 bits (6 bytes): 1100xxxx X X X X X
52 bits (7 bytes): 1101xxxx X X X X X X
60 bits (8 bytes): 1110xxxx X X X X X X X
64 bits (9 bytes): 1111---- X X X X X X X X

*/

const (
	// map_size 5GB max
	mapSize        = 1024 * 1024 * 1024 * 10
	dbs            = 10        // objects, names, grams
	gramSize       = 2         // bytes
	maxFileIDBytes = 65536 * 2 // number of bytes in a list of all file ids
	maxInt         = int(^uint(0) >> 1)
	null64         = 0xFFFFFFFFFFFFFFFF
)

const (
	valid = iota
	deleted
)

var groupValidity = []string{"valid", "changed", "deleted"}

var systemID = []byte{0}

type oidList [9][]byte

type chunk struct {
	gid       uint64
	data      []byte
	gramCount uint16
}

type groupStruct struct {
	groupName   string
	oidCount    uint64
	lastChanged time.Time
	validity    byte
}

type myBuf struct {
	bytes []byte
}

type lmdbConfigStruct struct {
	cmd           string   // arg 1
	args          []string // args 2 - N-1
	db            string   // arg N
	env           *lmdb.Env
	gramKeyBuf    []byte
	gramBuf       []byte
	txn           *lmdb.Txn
	chunkDb       lmdb.DBI
	groupDb       lmdb.DBI
	groupNameDb   lmdb.DBI
	gramDb        lmdb.DBI
	scratchBuf    *myBuf
	dirty         bool
	dirtyGroup    *groupStruct
	dirtyGroupGid uint64
	dirtyName     bool
	dirtyGrams    map[gram]*oidList
	oidBuf        []byte
	input         *bufio.Reader
	data          []byte
	// persisted values
	nextOID  uint64
	nextGID  uint64
	freeOids []byte // compressed oids
	freeGids []byte // compressed oids
	//options
	partial     bool
	org         bool
	gramSize    int
	delimiter   string
	gramHex     bool
	dataHex     bool
	dataString  string
	grams       bool
	candidates  bool
	separate    bool
	numbers     bool
	compression string
	force       bool
	test        bool
}

var lmdbConfig lmdbConfigStruct

func runLmdb() bool {
	if len(os.Args) == 1 {
		usage()
	}
	if cmds[os.Args[1]] == nil {return false}
	cfg := &lmdbConfig
	cfg.scratchBuf = new(myBuf)
	cfg.gramBuf = (&[3]byte{})[:]
	cfg.gramKeyBuf = (&[2]byte{})[:]
	cfg.db = flag.Args()[0]
	cfg.cmd = os.Args[1]
	cfg.args = flag.Args()[1:]
	cfg.oidBuf = make([]byte, 1024)
	cfg.clean()
	if cfg.cmd != "create" && cfg.cmd != "grams" {
		_, err := os.Stat(cfg.db)
		if err != nil {
			exitError(fmt.Sprintf("%s: DATABASE %s DOES NOT EXIST", cfg.cmd, cfg.db))
		}
	}
	cmds[cfg.cmd](cfg)
	return true
}

func (cfg *lmdbConfigStruct) numBytes(n uint64) []byte {
	cfg.scratchBuf.reset()
	cfg.scratchBuf.putNum(n)
	return cfg.scratchBuf.bytes
}

func (cfg *lmdbConfigStruct) open(write bool) {
	flags := uint(lmdb.NoSubdir)
	env, err := lmdb.NewEnv()
	check(err)
	check(env.SetMapSize(mapSize))
	check(env.SetMaxDBs(dbs))
	cfg.env = env
	if !write {
		flags |= lmdb.Readonly
	}
	err = env.Open(cfg.db, flags, 0644)
	check(err)
	if cfg.cmd == "create" {
		cfg.dirty = true
	} else {
		cfg.view(func() {
			cfg.load()
		})
	}
}

func cmdInfo(cfg *lmdbConfigStruct) {
	cfg.open(false)
	defer cfg.env.Close()
	cfg.view(func() {
		if len(cfg.args) == 0 {
			stat, err := cfg.txn.Stat(cfg.groupDb)
			check(err)
			groups := int(stat.Entries)
			stat, err = cfg.txn.Stat(cfg.groupNameDb)
			check(err)
			groupNames := stat.Entries
			stat, err = cfg.txn.Stat(cfg.chunkDb)
			check(err)
			chunks := float64(stat.Entries)
			stat, err = cfg.txn.Stat(cfg.gramDb)
			check(err)
			gramTot := stat.Entries
			gramTot--
			staleGroups := map[string]string{}
			staleGroupNames := []string{}
			deletedGroups := map[uint64]struct{}{}
			deletedChunks := 0
			cfg.iterate(cfg.groupDb, func(cur *lmdb.Cursor, k, v []byte) {
				gid, _ := getNumOrPanic(k)
				group := decodeGroup(v)
				if group.validity == deleted {
					deletedGroups[gid] = member
					return
				}
				stat, err := os.Stat(group.groupName)
				if os.IsNotExist(err) {
					staleGroups[group.groupName] = "File %s does not exist"
				} else if err != nil {
					staleGroups[group.groupName] = "Cannot open file %s"
				} else if stat.ModTime().After(group.lastChanged) {
					staleGroups[group.groupName] = "File %s has changes"
				}
			})
			cfg.iterate(cfg.chunkDb, func(cur *lmdb.Cursor, k, v []byte) {
				chunk := decodeChunk(v)
				if _, deleted := deletedGroups[chunk.gid]; deleted {
					deletedChunks++
				}
			})
			groups -= len(deletedGroups)
			groups -= len(staleGroups)
			chunks -= float64(deletedChunks)
			fmt.Printf("%-15s %d\n", "Group names:", groupNames)
			fmt.Printf("%-15s %d\n", "Groups:", groups)
			fmt.Printf("%-15s %d\n", "Deleted groups:", len(deletedGroups))
			fmt.Printf("%-15s %d\n", "Bad groups:", len(staleGroups))
			fmt.Printf("%-15s %d\n", "Chunks:", int64(chunks))
			fmt.Printf("%-15s %d\n", "Deleted chunks:", deletedChunks)
			fmt.Printf("%-15s %d\n", "Grams:", gramTot)
			if len(staleGroups) > 0 {
				for group := range staleGroups {
					staleGroupNames = append(staleGroupNames, group)
				}
				sort.Strings(staleGroupNames)
				for _, group := range staleGroupNames {
					fmt.Fprintf(os.Stderr, staleGroups[group]+"\n", group)
				}
			}
			if cfg.grams {
				maxOids := 0
				minOids := int(maxInt)
				amounts := []float64{
					0.99,
					0.95,
					0.90,
					0.80,
					0.75,
					0.70,
					0.30,
					0.20,
					0.10,
					0.05,
					0.01,
					0.001,
					0.0001,
					0.00001,
					0.000001,
				}
				coverage := map[float64]int{}
				for _, amt := range amounts {
					coverage[amt] = 0
				}
				totalBytes, chunkBytes, gramBytes := 0, 0, 0
				cur, err := cfg.txn.OpenCursor(cfg.chunkDb)
				check(err)
				k, v, err := cur.Get(nil, nil, lmdb.First)
				count := 0
				for err == nil {
					count++
					totalBytes += len(k) + len(v)
					chunkBytes += len(k) + len(v)
					k, v, err = cur.Get(nil, nil, lmdb.Next)
				}
				if err != nil && !lmdb.IsNotFound(err) {
					check(err)
				}
				cur.Close()
				cur, err = cfg.txn.OpenCursor(cfg.gramDb)
				check(err)
				_, _, err = cur.Get(nil, nil, lmdb.First)
				check(err)
				for {
					k, v, err := cur.Get(nil, nil, lmdb.Next) // skip first gram: system record
					if err != nil {break}
					oids := oidListFor(v)
					totalBytes += len(k) + len(v)
					gramBytes += len(k) + len(v)
					oidTot := oids.totalOids()
					if oidTot < minOids {
						minOids = oidTot
					}
					if oidTot > maxOids {
						maxOids = oidTot
					}
					for amt := range coverage {
						if float64(oidTot)/float64(chunks) <= amt {
							coverage[amt]++
						}
					}
				}
				if err != nil && !lmdb.IsNotFound(err) {
					check(err)
				}
				cur.Close()
				if minOids == maxInt {
					minOids = 0
				}
				fmt.Printf("total bytes: %d\n", totalBytes)
				fmt.Printf("chunk bytes: %d\n", chunkBytes)
				fmt.Printf("gram bytes: %d\n", gramBytes)
				fmt.Printf("max oids: %d\n", maxOids)
				fmt.Printf("min oids: %d\n", minOids)
				for _, amt := range amounts {
					fmt.Printf("%d grams are in %f%% of chunks\n", coverage[amt], 100*amt)
				}
			}
		} else if len(cfg.args) == 1 {
			_, group := cfg.getGroup(cfg.groupName())
			if group == nil {
				exitError(fmt.Sprintf("NO GROUP %s\n", cfg.groupName()))
			}

		}
	})
}

func cmdCreate(cfg *lmdbConfigStruct) {
	cfg.open(true)
	defer cfg.env.Close()
	cfg.env.Update(func(txn *lmdb.Txn) error {
		cfg.runTxn(txn, lmdb.Create, func() {
			cfg.store()
		})
		return nil
	})
}

func (cfg *lmdbConfigStruct) groupName() string {
	return cfg.args[0]
}

func cmdChunk(cfg *lmdbConfigStruct) {
	if len(cfg.args) != 2 {
		usage()
	}
	cfg.open(true)
	defer cfg.env.Close()
	if cfg.dataHex {
		bytes, err := hex.DecodeString(cfg.dataString)
		check(err)
		cfg.data = bytes
	} else {
		cfg.data = []byte(cfg.dataString)
	}
	cfg.update(func() {
		oid, d := cfg.initChunk()
		if cfg.gramHex {
			grams := make([]byte, len(cfg.args[1])/2)
			_, err := hex.Decode(grams, []byte(cfg.args[1]))
			check(err)
			for i := 0; i < len(grams); i += 2 {
				cfg.addGramEntry(gram((int(grams[i])<<8)|int(grams[i+1])), oid, d)
			}
		} else {
			grams := strings.Split(cfg.args[1], cfg.delimiter)
			for _, grm := range grams {
				cfg.addGramEntry(gramForUnicode(grm), oid, d)
			}
		}
		cfg.putChunk(oid, d)
	})
}

func cmdInput(cfg *lmdbConfigStruct) {
	if len(cfg.args) == 0 { // DATABASE and at least one GROUP
		usage()
	}
	cfg.open(true)
	defer cfg.env.Close()
	cfg.update(func() {
		for len(cfg.args) > 0 {
			cfg.index(cfg.args[0])
			cfg.storeDirtyGroup()
			cfg.args = cfg.args[1:]
		}
	})
}

func (cfg *lmdbConfigStruct) index(group string) {
	if cfg.org {
		cfg.indexOrg(group)
	} else {
		cfg.indexLines(group)
	}
}

func (cfg *lmdbConfigStruct) openInputFile(group string) (bool, time.Time) {
	stat, err := os.Stat(group)
	check(err)
	date := stat.ModTime()
	_, grp := cfg.getGroup(group)
	if grp != nil && grp.lastChanged.Equal(date) {return false, time.Now()}
	cfg.deleteGroupByName(group)
	input, err := os.OpenFile(group, os.O_RDONLY, 0)
	check(err)
	cfg.input = bufio.NewReader(input)
	return true, date
}

func (cfg *lmdbConfigStruct) indexOrg(group string) {
	contents, err := ioutil.ReadAll(cfg.input)
	check(err)
	str := string(contents)
	open, date := cfg.openInputFile(group)
	if !open {return}
	_, grp := cfg.getGroup(group)
	if grp != nil && grp.lastChanged.Equal(date) {return}
	forParts(str, func(line, typ, start, end int) {
		g := grams(str[start:end])
		if len(g) > 0 {
			buf := new(myBuf)
			buf.putNum(uint64(line))
			buf.putNum(uint64(start))
			buf.putNum(uint64(end - start))
			cfg.data = buf.bytes
			oid, d := cfg.initChunk() // make chunk for chunk
			for grm := range g {
				cfg.addGramEntry(grm, oid, d)
			}
			cfg.putChunk(oid, d)
		}
	})
	grp.lastChanged = date
}

func (cfg *lmdbConfigStruct) indexLines(group string) {
	open, date := cfg.openInputFile(group)
	if !open {return}
	pos := 0
	for lineNo := 1; ; lineNo++ {
		line, err := readLine(cfg.input)
		if err == io.EOF {break}
		buf := new(myBuf)
		buf.putNum(uint64(lineNo))
		buf.putNum(uint64(pos))
		buf.putNum(uint64(len(line)))
		cfg.data = buf.bytes
		oid, d := cfg.initChunk() // make chunk for line
		for grm := range grams(line) {
			cfg.addGramEntry(grm, oid, d)
		}
		cfg.putChunk(oid, d)
		pos += len(line)
	}
	cfg.dirtyGroup.lastChanged = date
}

func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadBytes('\n')
	if err == io.EOF {return "", err}
	check(err)
	return string(line), nil
}

func (cfg *lmdbConfigStruct) initChunk() ([]byte, *chunk) {
	d := new(chunk)
	d.data = cfg.data
	gid, group := cfg.getGroup(cfg.groupName())
	if group == nil {
		gid, group = cfg.createGroup()
	}
	d.gid = gid
	oid := cfg.getNewOid()
	cfg.dirtyGroupGid = gid
	cfg.dirtyGroup = group
	group.oidCount++
	return oid, d
}

//add the OID to the chunkBuf, mark it dirty, and add the gram to the chunk buf, returning the chunk buf
func (cfg *lmdbConfigStruct) addGramEntry(grm gram, oid []byte, d *chunk) {
	oids := cfg.getGram(grm)
	if oids == nil { // found an existing gram
		oids = new(oidList)
	}
	l := len(oid)
	oids[l-1] = cat(oids[l-1], oid)
	cfg.dirtyGrams[grm] = oids
	d.gramCount++
}

func cat(bytes1 []byte, bytes2 []byte) []byte {
	buf := newBuf(bytes1)
	buf.append(bytes2)
	return buf.bytes
}

func newBuf(bytes []byte) *myBuf {
	return &myBuf{bytes}
}

func (buf *myBuf) reset() {
	buf.bytes = buf.bytes[:0]
}

func (buf *myBuf) append(bytes []byte) {
	copy(buf.next(len(bytes)), bytes)
}

func (buf *myBuf) skip(n int) {
	buf.grow(n)
	buf.bytes = buf.bytes[:len(buf.bytes)+n]
}

func (buf *myBuf) next(n int) []byte {
	l := len(buf.bytes)
	buf.skip(n)
	return buf.bytes[l : l+n]
}

func (buf *myBuf) rest(min int) []byte {
	buf.grow(min)
	return buf.bytes[len(buf.bytes):cap(buf.bytes)]
}

func (buf *myBuf) grow(n int) {
	if cap(buf.bytes)-len(buf.bytes) < n {
		l := cap(buf.bytes)
		if l == 0 {
			l = 1
		}
		target := len(buf.bytes) + n
		for l < target {
			l <<= 1
		}
		old := buf.bytes
		buf.bytes = make([]byte, len(buf.bytes), l)
		copy(buf.bytes, old)
	}
}

func (buf *myBuf) len() int {
	return len(buf.bytes)
}

func (buf *myBuf) putCountedBytes(bytes []byte) {
	buf.putNum(uint64(len(bytes)))
	buf.append(bytes)
}

func (buf *myBuf) putNum(n uint64) {
	r1 := buf.rest(9)
	r2, _ := putNum(n, r1)
	buf.skip(len(r1) - len(r2))
}

func (buf *myBuf) appendOids(oids *oidList) {
	for i := 0; i < 9; i++ {
		buf.putNum(uint64(len(oids[i]) / (i + 1)))
	}
	for i := 0; i < 9; i++ {
		buf.append(oids[i])
	}
}

func (cfg *lmdbConfigStruct) getOidList(db lmdb.DBI, key []byte) *oidList {
	return oidListFor(cfg.get(db, key))
}

func oidListFor(bytes []byte) *oidList {
	if len(bytes) == 0 {return nil}
	oids := new(oidList)
	var rawOids uint64
	start := 0
	lengths := [9]int{}
	for i := 0; i < 9; i++ {
		rawOids, bytes = getNumOrPanic(bytes)
		l := int(rawOids)
		lengths[i] = l
	}
	for i := 0; i < 9; i++ {
		l := lengths[i]
		next := start + l*(i+1)
		if next-start > 0 {
			oids[i] = bytes[start:next:next]
		} else {
			oids[i] = bytes[0:0:0]
		}
		start = next
	}
	return oids
}

func (oids *oidList) total() int {
	total := 9
	for i := 0; i < 9; i++ {
		total += len(oids[i])
	}
	return total
}

func (oids *oidList) totalOids() int {
	total := 0
	for i := 0; i < 9; i++ {
		total += len(oids[i]) / (i + 1)
	}
	return total
}

func (oids *oidList) allOids() map[uint64]struct{} {
	result := make(map[uint64]struct{})
	for _, bytes := range oids {
		for len(bytes) > 0 {
			var n uint64
			n, bytes = getNumOrPanic(bytes)
			result[n] = member
		}
	}
	return result
}

func (oids *oidList) append(bytes []byte) {
	sz := len(bytes)
	oids[sz-1] = cat(oids[sz-1], bytes)
}

func (cfg *lmdbConfigStruct) gramKeyForGram(gram gram) []byte {
	cfg.gramKeyBuf[0] = byte(gram >> 8)
	cfg.gramKeyBuf[1] = byte(gram & 0xFF)
	return cfg.gramKeyBuf
}

func (cfg *lmdbConfigStruct) gramFor(str string) gram {
	if !cfg.gramHex {return gramForUnicode(str)}
	digit1, err := strconv.ParseInt(str[:2], 16, 8)
	check(err)
	digit2, err := strconv.ParseInt(str[2:4], 16, 8)
	check(err)
	return gram((digit1 << 8) | digit2)
}

func (cfg *lmdbConfigStruct) oidKey(oid uint64) []byte {
	return cfg.numBytes(oid)
}

func (cfg *lmdbConfigStruct) load() {
	buf := cfg.get(cfg.gramDb, systemID)
	if buf != nil {
		oid, rest := getNumOrPanic(buf)
		cfg.nextOID = oid
		oid, rest = getNumOrPanic(rest)
		cfg.nextGID = oid
		free, rest := getCountedBytes(rest)
		cfg.freeOids = free
		free, rest = getCountedBytes(rest)
		cfg.freeGids = free
	}
}

func (cfg *lmdbConfigStruct) store() {
	buf := new(myBuf)
	buf.putNum(cfg.nextOID)
	buf.putNum(cfg.nextGID)
	buf.putCountedBytes(cfg.freeOids)
	buf.putCountedBytes(cfg.freeGids)
	cfg.put(cfg.gramDb, systemID, buf.bytes)
}

func (cfg *lmdbConfigStruct) storeDirty() {
	if cfg.dirty {
		cfg.store()
	}
	cfg.storeDirtyGroup()
	if len(cfg.dirtyGrams) > 0 {
		for grm, oids := range cfg.dirtyGrams {
			cfg.putGram(cfg.gramKeyForGram(grm), oids)
		}
	}
	cfg.clean()
}

func (cfg *lmdbConfigStruct) storeDirtyGroup() {
	if cfg.dirtyGroup != nil {
		cfg.putGroup(cfg.dirtyGroupGid, cfg.dirtyGroup)
		if cfg.dirtyName {
			buf := new(myBuf)
			buf.putNum(cfg.dirtyGroupGid)
			cfg.put(cfg.groupNameDb, []byte(cfg.dirtyGroup.groupName), buf.bytes)
		}
		cfg.dirtyGroup = nil
		cfg.dirtyName = false
	}
}

func (cfg *lmdbConfigStruct) clean() {
	cfg.dirty = false
	cfg.dirtyGroup = nil
	cfg.dirtyName = false
	cfg.dirtyGrams = make(map[gram]*oidList)
}

func (cfg *lmdbConfigStruct) getNewOid() []byte {
	var oid uint64
	if len(cfg.freeOids) != 0 {
		var rest []byte
		oid, rest = getNumOrPanic(cfg.freeOids)
		cfg.freeOids = rest
	} else {
		oid = cfg.nextOID
		cfg.nextOID++
	}
	cfg.dirty = true
	return cfg.oidKey(oid)
}

func cmdGrams(cfg *lmdbConfigStruct) {
	if len(cfg.args) != 0 {
		usage()
	}
	first := true
	for grm := range grams(cfg.db) { // db is actually the phrase
		if first {
			first = false
		} else {
			fmt.Print(" ")
		}
		if cfg.gramHex {
			fmt.Printf("%s: %s%s",
				cfg.db,
				strconv.FormatUint(uint64(grm>>8), 16),
				strconv.FormatUint(uint64(grm&0xFF), 16))
		} else {
			fmt.Printf("%s", gramString(grm))
		}
	}
	fmt.Println()
}

func cmdDelete(cfg *lmdbConfigStruct) {
	if len(cfg.args) != 1 {
		usage()
	}
	cfg.open(true)
	defer cfg.env.Close()
	cfg.update(func() {
		cfg.deleteGroup()
	})
}

func (cfg *lmdbConfigStruct) iterate(dbi lmdb.DBI, code func(cur *lmdb.Cursor, key, value []byte)) {
	cur, err := cfg.txn.OpenCursor(dbi)
	check(err)
	k, v, err := cur.Get(nil, nil, lmdb.First)
	for err == nil {
		code(cur, k, v)
		k, v, err = cur.Get(nil, nil, lmdb.Next)
	}
	if err != nil && !lmdb.IsNotFound(err) {
		check(err)
	}
	cur.Close()
}

func cmdCompact(cfg *lmdbConfigStruct) {
	if len(cfg.args) != 0 {
		usage()
	}
	cfg.open(true)
	defer cfg.env.Close()
	cfg.update(func() {
		deletedGroups := map[uint64]struct{}{}
		deletedChunks := map[uint64]struct{}{}
		cfg.iterate(cfg.groupDb, func(cur *lmdb.Cursor, k, v []byte) {
			gid, _ := getNumOrPanic(k)
			group := decodeGroup(v)
			if group.validity == deleted {
				deletedGroups[gid] = member
				cur.Del(0)
				buf := &myBuf{cfg.freeGids}
				buf.putNum(gid)
				cfg.freeGids = buf.bytes
				cfg.dirty = true
			}
		})
		cfg.iterate(cfg.chunkDb, func(cur *lmdb.Cursor, k, v []byte) {
			did, _ := getNumOrPanic(k)
			chunk := decodeChunk(v)
			if _, exists := deletedGroups[chunk.gid]; exists {
				deletedChunks[did] = member
				cur.Del(0)
				buf := &myBuf{cfg.freeOids}
				buf.putNum(did)
				cfg.freeOids = buf.bytes
				cfg.dirty = true
			}
		})
		first := true
		cfg.iterate(cfg.gramDb, func(cur *lmdb.Cursor, k, v []byte) {
			if first { // skip system record
				first = false
				return
			}
			oids := oidListFor(v)
			dirty := false
			for i, bytes := range oids {
				newBytes := new(myBuf)
				for len(bytes) > 0 {
					did, rest := getNumOrPanic(bytes)
					if _, exists := deletedChunks[did]; exists {
						dirty = true
					} else {
						newBytes.append(bytes[:len(bytes)-len(rest)])
					}
					bytes = rest
				}
				oids[i] = newBytes.bytes
			}
			if dirty {
				if oids.totalOids() == 0 {
					cur.Del(0)
				} else {
					buf := new(myBuf)
					buf.appendOids(oids)
					cur.Put(k, buf.bytes, lmdb.Current)
				}
			}
		})
	})
}

func cmdUpdate(cfg *lmdbConfigStruct) {
	if len(cfg.args) != 0 {
		usage()
	}
	cfg.open(true)
	defer cfg.env.Close()
	cfg.update(func() {
		deleteGroups := map[string]struct{}{}
		updateGroups := map[string]struct{}{}
		cfg.iterate(cfg.groupDb, func(cur *lmdb.Cursor, k, v []byte) {
			group := decodeGroup(v)
			stat, err := os.Stat(group.groupName)
			if err != nil {
				deleteGroups[group.groupName] = member
			} else if stat.ModTime().After(group.lastChanged) {
				updateGroups[group.groupName] = member
			}
		})
		for group := range deleteGroups {
			if cfg.test {
				fmt.Printf("Delete '%s' because it is missing", group)
			} else {
				cfg.deleteGroupByName(group)
			}
		}
		for group := range updateGroups {
			if cfg.test {
				fmt.Printf("Input '%s' because it has changed", group)
			} else {
				cfg.args = []string{group}
				cfg.index(group)
				cfg.storeDirtyGroup()
			}
		}
	})
}

func cmdEmpty(cfg *lmdbConfigStruct) {
	if len(cfg.args) == 0 {
		usage()
	}
	cfg.open(true)
	cfg.update(func() {
		for len(cfg.args) > 0 {
			_, grp := cfg.getGroup(cfg.groupName())
			if grp == nil {
				cfg.createGroup()
				cfg.storeDirtyGroup()
			}
			cfg.args = cfg.args[1:]
		}
	})
}

func cmdSearch(cfg *lmdbConfigStruct) {
	if len(cfg.args) == 0 {
		usage()
	}
	inputGrams := grams(strings.Join(cfg.args, " "))
	if len(inputGrams) == 0 {
		os.Exit(1)
	}
	cfg.open(false)
	defer cfg.env.Close()
	hits := make(map[uint64][][]byte)
	groups := make([]string, 0, 16)
	gids := make(map[string]uint64)
	groupStructs := map[string]*groupStruct{}
	deletedGroups := map[uint64]struct{}{}
	cfg.view(func() {
		results := cfg.intersectGrams(inputGrams)
		for oid := range results {
			d := cfg.getChunk(cfg.oidKey(oid))
			hits[d.gid] = append(hits[d.gid], d.data)
		}
		for gid := range hits {
			group := cfg.getGroupWithGid(gid)
			if group != nil && group.validity != deleted {
				gids[group.groupName] = gid
				groups = append(groups, group.groupName)
				groupStructs[group.groupName] = group
			} else {
				deletedGroups[gid] = member
			}
		}
	})
	sort.Strings(groups)
	if cfg.candidates {
		for _, grpNm := range groups {
			if _, deleted := deletedGroups[gids[grpNm]]; deleted {continue}
			var grams []string
			for _, data := range hits[gids[grpNm]] {
				grams = append(grams, string(data))
			}
			sort.Strings(grams)
			if cfg.separate {
				for _, data := range grams {
					fmt.Printf("%s: %s\n", grpNm, data)
				}
			} else {
				fmt.Printf("%s:", grpNm)
				for _, data := range grams {
					fmt.Printf(" %s", data)
				}
				fmt.Println()
			}
		}
	} else {
		badFiles := map[string]string{}
		bad := func(msg string, group string) {
			delete(hits, gids[group])
			delete(gids, group)
			delete(groupStructs, group)
			badFiles[group] = fmt.Sprintf(msg, group)
		}
		for _, group := range groups {
			if _, deleted := deletedGroups[gids[group]]; deleted {continue}
			stat, err := os.Stat(group)
			if err != nil && cfg.force {
				bad("Skipping '%s' because it is missing", group)
			} else if err != nil {
				exitError("Could not read file " + group)
			} else if stat.ModTime().After(groupStructs[group].lastChanged) {
				if cfg.force {
					bad("Skipping '%s' because it has changed", group)
				} else {
					exitError(fmt.Sprintf("File has changed since indexing: %s", group))
				}
			}
		}
		for _, group := range groups {
			if _, deleted := deletedGroups[gids[group]]; deleted {continue}
			if msg, isBad := badFiles[group]; isBad {
				fmt.Fprintln(os.Stderr, msg)
				continue
			}
			var chunkStarts []int
			lineNos := make(map[uint64]uint64)
			chunkEnds := make(map[uint64]uint64)
			for _, data := range hits[gids[group]] {
				lineNo, data := getNumOrPanic(data)
				start, data := getNumOrPanic(data)
				len, _ := getNumOrPanic(data)
				lineNos[start] = lineNo
				chunkStarts = append(chunkStarts, int(start))
				chunkEnds[start] = start + len
			}
			sort.Ints(chunkStarts)
			contents, err := ioutil.ReadFile(group)
			if err != nil {
				exitError(fmt.Sprintf("Could not read file: %s", group))
			}
			out := ""
			if cfg.numbers {
				out = group + ":"
			}
		eachChunk:
			for _, st := range chunkStarts {
				start := uint64(st)
				chunk := strings.ToLower(string(contents[start:chunkEnds[start]]))
			args:
				for _, arg := range cfg.args {
					testChunk := chunk
					for len(testChunk) > 0 {
						i := strings.Index(testChunk, strings.ToLower(arg))
						if i == -1 {break}
						if cfg.partial || ((i == 0 || !isGramChar(testChunk[i-1])) &&
							(i+len(arg) == len(testChunk) || !isGramChar(testChunk[i+len(arg)]))) {
							continue args // found a hit for this arg
						}
						testChunk = testChunk[len(arg):]
					}
					continue eachChunk // if it made it to here, the arg isn't in the line
				}
				if cfg.numbers {
					out = fmt.Sprintf("%s %d", out, lineNos[start])
				} else {
					if chunk != "" && chunk[len(chunk)-1] == '\n' {
						chunk = chunk[:len(chunk)-1]
					}
					fmt.Printf("%s:%d:%s\n", group, lineNos[start], chunk)
				}
			}
			if cfg.numbers {
				fmt.Println(out)
			}
		}
	}
}

func isGramChar(c byte) bool {
	return ('0' <= c && c <= '9') || ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z')
}

func (cfg *lmdbConfigStruct) intersectGrams(inputGrams map[gram]struct{}) map[uint64]struct{} {
	hits := make(map[gram]*oidList)
	smallest := gram(0)
	smallestSize := maxInt
	var results map[uint64]struct{}

	for grm := range inputGrams {
		oids := cfg.getGram(grm)
		if oids == nil {
			os.Exit(1)
		}
		hits[grm] = oids
		if oids.totalOids() < smallestSize {
			smallestSize = oids.totalOids()
			smallest = grm
		}
	}
	results = hits[smallest].allOids()
	for grm, oids := range hits { // intersect the grams' oids
		if grm == smallest {continue}
		cur := oids.allOids()
		for oid := range results {
			_, includes := cur[oid]
			if !includes { // filter oids that are not common to all grams out of results
				delete(results, oid)
				if len(results) == 0 {
					os.Exit(1)
				}
			}
		}
	}
	return results
}

func (cfg *lmdbConfigStruct) getGram(grm gram) *oidList {
	if cfg.dirtyGrams[grm] != nil {return cfg.dirtyGrams[grm]}
	return cfg.getOidList(cfg.gramDb, cfg.gramKeyForGram(grm))
}

func (cfg *lmdbConfigStruct) putGram(key []byte, oids *oidList) {
	buf := new(myBuf)
	buf.appendOids(oids)
	cfg.put(cfg.gramDb, key, buf.bytes)
}

func (cfg *lmdbConfigStruct) getGroup(name string) (uint64, *groupStruct) {
	if cfg.dirtyGroup != nil && cfg.dirtyGroup.groupName == name {return cfg.dirtyGroupGid, cfg.dirtyGroup}
	bytes := cfg.get(cfg.groupNameDb, []byte(name))
	if bytes == nil {return 0, nil}
	gid, _ := getNumOrPanic(bytes)
	return gid, cfg.getGroupWithGidBytes(bytes)
}

func (cfg *lmdbConfigStruct) getGroupWithGid(gid uint64) *groupStruct {
	return cfg.getGroupWithGidBytes(cfg.numBytes(gid))
}

func (cfg *lmdbConfigStruct) getGroupWithGidBytes(gid []byte) *groupStruct {
	return decodeGroup(cfg.get(cfg.groupDb, gid))
}

func decodeGroup(bytes []byte) *groupStruct {
	if bytes == nil {return nil}
	group := new(groupStruct)
	nm, bytes := getCountedBytes(bytes)
	group.groupName = string(nm)
	oidCount, bytes := getNumOrPanic(bytes)
	group.oidCount = oidCount
	group.validity = bytes[0]
	lastModSec, bytes := getNumOrPanic(bytes[1:])
	lastModNanos, _ := getNumOrPanic(bytes)
	group.lastChanged = time.Unix(int64(lastModSec), int64(lastModNanos))
	return group
}

func (cfg *lmdbConfigStruct) putGroup(gid uint64, grp *groupStruct) {
	buf := new(myBuf)
	buf.putCountedBytes([]byte(grp.groupName))
	buf.putNum(grp.oidCount)
	buf.next(1)[0] = grp.validity
	buf.putNum(uint64(grp.lastChanged.Unix()))
	buf.putNum(uint64(grp.lastChanged.Nanosecond()))
	cfg.put(cfg.groupDb, cfg.numBytes(gid), buf.bytes)
}

func (cfg *lmdbConfigStruct) createGroup() (gid uint64, group *groupStruct) {
	if len(cfg.freeGids) > 0 {
		var rest []byte
		gid, rest = getNumOrPanic(cfg.freeGids)
		cfg.freeGids = rest
	} else {
		gid = cfg.nextGID
		cfg.nextGID++
		cfg.dirty = true
	}
	group = new(groupStruct)
	group.groupName = cfg.groupName()
	cfg.dirtyGroupGid = gid
	cfg.dirtyGroup = group
	cfg.dirtyName = true
	return
}

func (cfg *lmdbConfigStruct) deleteGroup() {
	cfg.deleteGroupByName(cfg.groupName())
}

func (cfg *lmdbConfigStruct) deleteGroupByName(name string) {
	gid, grp := cfg.getGroup(name)
	if grp == nil {return}
	grp.validity = deleted
	cfg.putGroup(gid, grp)
	cfg.del(cfg.groupNameDb, []byte(name))
}

func (cfg *lmdbConfigStruct) getChunk(key []byte) *chunk {
	return decodeChunk(cfg.get(cfg.chunkDb, key))
}

func decodeChunk(bytes []byte) *chunk {
	result := new(chunk)
	result.gid, bytes = getNumOrPanic(bytes)
	data, bytes := getCountedBytes(bytes)
	result.data = data
	g, bytes := getNumOrPanic(bytes)
	result.gramCount = uint16(g)
	return result
}

func (cfg *lmdbConfigStruct) putChunk(key []byte, d *chunk) {
	buf := new(myBuf)
	buf.putNum(d.gid)
	buf.putCountedBytes(d.data)
	buf.putNum(uint64(d.gramCount))
	cfg.put(cfg.chunkDb, key, buf.bytes)
}

func (cfg *lmdbConfigStruct) get(db lmdb.DBI, key []byte) []byte {
	buf, err := cfg.txn.Get(db, key)
	if lmdb.IsErrno(err, lmdb.NotFound) {return nil}
	check(err)
	return buf
}

func (cfg *lmdbConfigStruct) put(db lmdb.DBI, key []byte, val []byte) {
	err := cfg.txn.Put(db, key, val, 0)
	check(err)
}

func (cfg *lmdbConfigStruct) del(db lmdb.DBI, key []byte) {
	err := cfg.txn.Del(db, key, nil)
	check(err)
}

func (cfg *lmdbConfigStruct) update(code func()) {
	err := cfg.env.Update(func(txn *lmdb.Txn) error {
		cfg.runTxn(txn, 0, func() {
			code()
			cfg.storeDirty()
		})
		return nil
	})
	check(err)
	cfg.env.Close()
}

func (cfg *lmdbConfigStruct) view(code func()) {
	err := cfg.env.View(func(txn *lmdb.Txn) error {
		cfg.runTxn(txn, 0, code)
		return nil
	})
	check(err)
}

func (cfg *lmdbConfigStruct) runTxn(txn *lmdb.Txn, flags uint, code func()) {
	defer func() {
		cfg.txn = nil
		cfg.chunkDb = lmdb.DBI(0xFFFFFFFF)
		cfg.groupDb = lmdb.DBI(0xFFFFFFFF)
		cfg.groupNameDb = lmdb.DBI(0xFFFFFFFF)
		cfg.gramDb = lmdb.DBI(0xFFFFFFFF)
	}()
	cfg.txn = txn
	chunks, err := txn.OpenDBI("chunks", flags)
	check(err)
	grams, err := txn.OpenDBI("grams", flags)
	check(err)
	groups, err := txn.OpenDBI("groups", flags)
	check(err)
	groupNames, err := txn.OpenDBI("groupNames", flags)
	check(err)
	cfg.chunkDb = chunks
	cfg.groupDb = groups
	cfg.groupNameDb = groupNames
	cfg.gramDb = grams
	code()
}

func numSize(number uint64) int {
	if number < 1<<7 {return 1}
	offset := 0
	for tmp := number >> 12; tmp > 0; offset++ {
		tmp >>= 8
	}
	return offset + 2
}

//stores a number high-low
func putNum(number uint64, buf []byte) ([]byte, error) {
	if number < 1<<7 {
		if len(buf) < 1 {return nil, io.EOF}
		buf[0] = byte(number & 0xFF)
		return buf[1:], nil
	}
	offset := 0
	for tmp := number >> 12; tmp > 0; offset++ {
		tmp >>= 8
	}
	var first = 0x80 | byte(offset<<4)
	offset++
	count := offset
	if len(buf) < count {return nil, io.EOF}
	for ; offset > 0; offset-- {
		buf[offset] = byte(number & 0xFF)
		number >>= 8
	}
	buf[0] = first | byte(number&0xF)
	return buf[count+1:], nil
}

func getCountedBytes(bytes []byte) (result []byte, rest []byte) {
	len, bytes := getNumOrPanic(bytes)
	result = bytes[:len]
	rest = bytes[len:]
	return
}

func getNumOrPanic(bytes []byte) (uint64, []byte) {
	result, bytes, err := getNum(bytes)
	check(err)
	return result, bytes
}

func getNum(buf []byte) (uint64, []byte, error) {
	if len(buf) == 0 {return 0, nil, io.EOF}
	if buf[0]&0x80 == 0 {return uint64(buf[0]), buf[1:], nil}
	bytes := int((buf[0]>>4)&0x7) + 2
	if len(buf) < bytes {return 0, nil, io.EOF}
	result := uint64(buf[0] & 0xF)
	for count := 1; count < bytes; count++ {
		result = (result << 8) | uint64(buf[count])
	}
	return result, buf[bytes:], nil
}

var cmds = map[string]func(*lmdbConfigStruct){
	"create":  cmdCreate,
	"chunk":   cmdChunk,
	"input":   cmdInput,
	"delete":  cmdDelete,
	"search":  cmdSearch,
	"grams":   cmdGrams,
	"info":    cmdInfo,
	"compact": cmdCompact,
	"update":  cmdUpdate,
	"empty":   cmdEmpty,
}
