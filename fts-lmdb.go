package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unsafe"

	"github.com/AskAlexSharov/lmdb-go/lmdb"
	//"github.com/bmatsuo/lmdb-go/lmdb"
)

/*

* TAGS
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
	dbs            = 10        // objects, names, tags
	tagSize        = 2         // bytes
	maxFileIDBytes = 65536 * 2 // number of bytes in a list of all file ids
)

type oidList [9][]byte

type doc struct {
	original  []byte
	groupName string
	data      []byte
	tags      []uint16
}

type lmdbConfigStruct struct {
	cmd       string   // arg 1
	args      []string // args 2 - N-1
	db        string   // arg N
	env       *lmdb.Env
	tagBuf    []byte
	txn       *lmdb.Txn
	objectDb  lmdb.DBI
	groupDb   lmdb.DBI
	tagsDb    lmdb.DBI
	oidKeyBuf []byte
	// persisted values
	nextOID  uint64
	freeOids []byte // compressed oids
	//options
	gramSize  int
	delimiter string
	nameHex   bool
	tagHex    bool
	dataHex   bool
	data      string
	tags      bool
	group     string
}

var lmdbConfig lmdbConfigStruct

func runLmdb() bool {
	testNums()
	fmt.Fprintf(os.Stderr, "ARGS:'%s'\n", strings.Join(flag.Args(), "' '"))
	if len(flag.Args()) == 0 || cmds[flag.Args()[0]] == nil {return false}
	if len(flag.Args()) == 1 {
		usage()
	}
	cfg := &lmdbConfig
	cfg.oidKeyBuf = new([9]byte)[:]
	cfg.tagBuf = make([]byte, 0, 1024)
	cfg.db = flag.Args()[len(flag.Args())-1]
	cfg.cmd = flag.Args()[0]
	cfg.args = flag.Args()[1 : len(flag.Args())-1]
	fmt.Fprintln(os.Stderr, "DB:", cfg.db)
	_, err := os.Stat(cfg.db)
	exists := err == nil
	if cfg.cmd != "create" && !exists {
		panic(fmt.Sprintf("%s: DATABASE %s DOES NOT EXIST", cfg.cmd, cfg.db))
	}
	cmds[cfg.cmd](cfg)
	return true
}

func program() string {
	p, err := filepath.Abs(os.Args[0])
	if err != nil {return fmt.Sprintf("<BAD PROGRAM PATH: %s>", os.Args[0])}
	return p
}

func lmdbOpen(write bool) {
	flags := uint(lmdb.NoSubdir)
	env, err := lmdb.NewEnv()
	check(err)
	err = env.SetMapSize(mapSize)
	check(err)
	err = env.SetMaxDBs(dbs)
	check(err)
	lmdbConfig.env = env
	if !write {
		flags |= lmdb.Readonly
	}
	fmt.Fprintln(os.Stderr, "Opening", lmdbConfig.db)
	err = env.Open(lmdbConfig.db, flags, 0644)
	check(err)
	fmt.Println(env)
}

func cmdStat(cfg *lmdbConfigStruct) {
	lmdbOpen(false)
	defer cfg.env.Close()
	fmt.Println("STAT")
	cfg.env.View(func(txn *lmdb.Txn) error {
		objects, err := txn.OpenDBI("objects", 0)
		check(err)
		stat, err := txn.Stat(objects)
		check(err)
		fmt.Printf("Objects: %d\n", stat.Entries)
		return nil
	})
}

func cmdCreate(cfg *lmdbConfigStruct) {
	lmdbOpen(true)
	defer cfg.env.Close()
	fmt.Fprintf(os.Stderr, "CREATE, env: %v\n", cfg.env)
	cfg.env.Update(func(txn *lmdb.Txn) error {
		for _, name := range strings.Split("objects,names,tags", ",") {
			fmt.Fprintf(os.Stderr, "Creating db %s\n", name)
			db, err := txn.OpenDBI(name, lmdb.Create)
			check(err)
			stat, err := txn.Stat(db)
			check(err)
			fmt.Printf("%s: %d\n", name, stat.Entries)
		}
		return nil
	})
}

func cmdTags(cfg *lmdbConfigStruct) {
	if len(cfg.args) != 2 {
		usage()
	}
	cfg.group = cfg.args[0]
	lmdbOpen(false)
	defer cfg.env.Close()
	fmt.Fprint(os.Stderr, "DEFINE TAGS FOR OBJECT '%s'\n", cfg.args[0])
	cfg.update(func() {
		fmt.Println(cfg.objectDb, cfg.tags, cfg.groupDb)
		tags := strings.Split(cfg.args[1], cfg.delimiter)
		oid, objectBuf, objectTags := cfg.initDoc(len(tags))
		for _, tag := range tags {
			cfg.addTagEntry(cfg.tagKey(tag), oid, objectTags)
		}
		cfg.putObject(oid, objectBuf)
	})
}

func (cfg *lmdbConfigStruct) initDoc(numTags int) ([]byte, []byte, []byte) {
	data := []byte(nil) // get data from command line args
	oidBuf := cfg.getGroup([]byte(cfg.group))
	var oid []byte
	if oidBuf != nil {
		oid = oidBuf
		cfg.deleteObjectContents(oid)
	} else {
		oid = cfg.getNewOid()
	}
	objectBuf := make([]byte, 0, 18+len(cfg.group)+len(data)+numTags*2)
	rest, _ := putNum(uint64(len(cfg.group)), objectBuf)
	copy(rest, cfg.group)
	rest = rest[len(cfg.group):]
	rest, _ = putNum(uint64(len(data)), rest)
	copy(rest, data)
	rest = rest[len(data):]
	total := len(objectBuf) - len(rest)
	result := objectBuf[:total+numTags*2]
	return oid, result, result[total:]
}

//add the OID to the entryBuf, store it the database, and add the tag to the doc buf
func (cfg *lmdbConfigStruct) addTagEntry(key []byte, oid []byte, docBuf []byte) []byte {
	oids := cfg.getTag(key)
	if oids == nil { // found an existing tag
		oids = new(oidList)
	}
	l := len(oid)
	if cap(oids[l]) >= len(oids[l])+l {
		oids[l] = oids[l][0 : len(oids[l])+l : cap(oids[l])]
	} else {
		oldOids := oids[l]
		oids[l] = make([]byte, len(oids[l])+l)
		copy(oids[l], oldOids)
	}
	copy(oids[l][len(oids[l])-l:], oid)
	cfg.putTag(key, oids)
	docBuf[0] = key[0]
	docBuf[1] = key[1]
	return docBuf[2:]
}

func readOidList(oids *oidList, bytes []byte) {
	start := 0
	for i := 0; i < 9; i++ {
		rawOids, bytes := getNumOrPanic(bytes)
		numOids := int(rawOids)
		oids[i] = bytes[start : start+numOids*(i+1) : start+numOids*(i+1)]
		start += numOids * (i + 1)
	}
}

func (oids *oidList) total() int {
	total := 0
	for i := 0; i < 9; i++ {
		total += len(oids[i]) * (i + 1)
	}
	return total
}

func (oids *oidList) appendTo(bytes []byte) []byte {
	total := oids.total()
	l := len(bytes)
	if cap(bytes) >= l+total {
		bytes = bytes[0 : l+total : cap(bytes)]
	} else {
		oldBytes := bytes
		bytes = make([]byte, l+total)
		copy(bytes, oldBytes)
	}
	for i := 0; i < 9; i++ {
		copy(bytes[l:], oids[i])
		l = len(oids[i]) * (i + 1)
	}
	return bytes
}

func (cfg *lmdbConfigStruct) tagKey(tag string) []byte {
	tagBuf := cfg.tagBuf
	if lmdbConfig.tagHex {
		digit, err := strconv.ParseInt(tag[0:2], 16, 8)
		check(err)
		tagBuf[0] = byte(digit)
		digit, err = strconv.ParseInt(tag[2:4], 16, 8)
		check(err)
		tagBuf[1] = byte(digit)
	} else {
		if len(tag) != 3 {
			panic(fmt.Sprintf("Unicode tag is not a trigram: '%s'", tag))
		}
		gram := uint16(0)
		for i := 0; i < 3; i++ {
			c := uint16(0)
			if '0' <= tag[i] && tag[i] <= '9' {
				c = uint16(tag[i] - '0')
			} else if 'A' <= tag[i] && tag[i] <= 'Z' {
				c = uint16(tag[i] - 'A')
			} else if 'a' <= tag[i] && tag[i] <= 'z' {
				c = uint16(tag[i] - 'a')
			}
			if gram%GRAM_BASE == 0 && c == 0 {continue}
			if gram%GRAM_BASE == 0 { // starting a word
				gram = c
			} else {
				gram = ((gram * GRAM_BASE) + c) % GRAM_3_BASE
			}
		}
		tagBuf[0] = byte(gram >> 8)
		tagBuf[1] = byte(gram & 0xFF)
	}
	return tagBuf
}

func (cfg *lmdbConfigStruct) oidKey(oid uint64) []byte {
	return putNumOrPanic(oid, cfg.oidKeyBuf)
}

func (cfg *lmdbConfigStruct) deleteObjectContents(oidKey []byte) {
	doc := cfg.getObject(oidKey)
	for _, tag := range doc.tags {
		cfg.deleteTagOid(tag, oidKey)
	}
}

func (cfg *lmdbConfigStruct) deleteTagOid(tag uint16, oidKey []byte) {

}

func (cfg *lmdbConfigStruct) read() {
	buf := cfg.get(cfg.objectDb, cfg.oidKey(0))
	if buf != nil {
		oid, rest := getNumOrPanic(buf)
		cfg.nextOID = oid
		free, _ := getCountedBytes(rest)
		cfg.freeOids = free
	}
}

func (cfg *lmdbConfigStruct) store() {
	buf := make([]byte, 9+len(cfg.freeOids))
	rest := putNumOrPanic(cfg.nextOID, buf)
	copy(rest, cfg.freeOids)
	cfg.put(cfg.objectDb, cfg.oidKey(0), buf)
}

func (cfg *lmdbConfigStruct) getNewOid() []byte {
	var oid uint64
	if cfg.freeOids != nil {
		var rest []byte
		oid, rest = getNumOrPanic(cfg.freeOids)
		cfg.freeOids = rest
	} else {
		oid = cfg.nextOID
		cfg.nextOID++
	}
	cfg.store()
	return cfg.oidKey(oid)
}

func cmdText(cfg *lmdbConfigStruct) {
	fmt.Println("TEXT")
}

func cmdDelete(cfg *lmdbConfigStruct) {
	fmt.Println("DELETE")
}

func cmdSearch(cfg *lmdbConfigStruct) {
	fmt.Println("SEARCH")
}

func cmdData(cfg *lmdbConfigStruct) {

	fmt.Println("DATA")
}

func (cfg *lmdbConfigStruct) getTag(key []byte) *oidList {
	result := new(oidList)
	bytes := cfg.get(cfg.tagsDb, key)
	readOidList(result, bytes)
	return result
}

func (cfg *lmdbConfigStruct) getObject(key []byte) *doc {
	bytes := cfg.get(cfg.objectDb, key)
	result := new(doc)
	result.original = bytes
	groupName, bytes := getCountedBytes(bytes)
	result.groupName = string(groupName)
	data, bytes := getCountedBytes(bytes)
	result.data = data
	result.tags = (*[1 << 48]uint16)(unsafe.Pointer(&bytes[0]))[0 : len(bytes)/2 : len(bytes)/2]
	return result
}

func (cfg *lmdbConfigStruct) getGroup(key []byte) *oidList {
	result := new(oidList)
	bytes := cfg.get(cfg.groupDb, key)
	readOidList(result, bytes)
	return result
}

func (cfg *lmdbConfigStruct) get(db lmdb.DBI, key []byte) []byte {
	buf, err := cfg.txn.Get(db, key)
	if err == lmdb.NotFound {return nil}
	check(err)
	return buf
}

func (cfg *lmdbConfigStruct) putObject(key []byte, val []byte) {
	cfg.put(cfg.objectDb, key, val)
}

func (cfg *lmdbConfigStruct) putName(key []byte, val []byte) {
	cfg.put(cfg.groupDb, key, val)
}

func (cfg *lmdbConfigStruct) putTag(key []byte, oids *oidList) {
	cfg.put(cfg.tagsDb, key, oids.appendTo(make([]byte, oids.total())))
}

func (cfg *lmdbConfigStruct) put(db lmdb.DBI, key []byte, val []byte) {
	err := cfg.txn.Put(db, key, val, 0)
	check(err)
}

func (cfg *lmdbConfigStruct) update(code func()) {
	cfg.env.Update(func(txn *lmdb.Txn) error {
		defer func() {
			cfg.txn = nil
			cfg.objectDb = lmdb.DBI(0xFFFFFFFF)
			cfg.groupDb = lmdb.DBI(0xFFFFFFFF)
			cfg.tagsDb = lmdb.DBI(0xFFFFFFFF)
		}()
		cfg.txn = txn
		objects, err := txn.OpenDBI("objects", 0)
		check(err)
		tags, err := txn.OpenDBI("tags", 0)
		check(err)
		names, err := txn.OpenDBI("names", 0)
		check(err)
		cfg.objectDb = objects
		cfg.groupDb = names
		cfg.tagsDb = tags
		code()
		return nil
	})
}

func putNumOrPanic(number uint64, buf []byte) []byte {
	buf, err := putNum(number, buf)
	check(err)
	return buf
}

//stores a number high-low
func putNum(number uint64, buf []byte) ([]byte, error) {
	if number < 1<<7 {
		if len(buf) < 1 {return nil, io.EOF}
		buf[0] = byte(number & 0xFF)
		return buf[1:], nil
	}
	offset := 0
	tmp := number >> 12
	for tmp > 0 {
		offset++
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
	result = bytes[0:len]
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

func testNums() {
	nums := []uint64{
		0,
		127,
		128,
		4095,
		4096,
		1048575,
		1048576,
		268435455,
		268435456,
		68719476735,
		68719476736,
		17592186044415,
		17592186044416,
		4503599627370495,
		4503599627370496,
		1152921504606846975,
		1152921504606846976,
		18446744073709551615,
	}
	for b := 0; b < len(nums); b++ {
		buf := make([]byte, 9)
		next, err := putNum(nums[b], buf)
		if err != nil {
			panic(err)
		}
		verify, gNext, err := getNum(buf)
		if verify != nums[b] {
			panic(fmt.Sprintf("ERROR: expected <%d> but got <%d>, buf = %v", nums[b], verify, buf))
		} else if len(buf)-len(next) != b/2+1 {
			panic(fmt.Sprintf("ERROR: expected number of length <%d> but got <%d>", b/2+1, len(buf)-len(next)))
		} else if len(gNext) != len(next) {
			panic(fmt.Sprintf("ERROR: expected bytes written to be <%d> but was <%d>", b/2+1, len(buf)-len(gNext)))
		}
	}
}

var cmds = map[string]func(*lmdbConfigStruct){
	"stat":   cmdStat,
	"create": cmdCreate,
	"grams":  cmdTags,
	"doc":    cmdText,
	"delete": cmdDelete,
	"search": cmdSearch,
	"data":   cmdData,
}
