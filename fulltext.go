package main

import (
	"database/sql"
	_ "database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	_ "github.com/pkg/profile"
)

const (
	part = "" +
		"(?im)^(" +
		"\\*.*" + // headline
		"|((?s)[ \\t]*#\\+begin_.*?\\n#\\+end_[^\\n]*)" + // block
		"|((?s)[ \\t]*#\\+begin:.*?\\n#\\+end:[ \\t]*)" + // dynblock
		"|[ \\t]*#\\+[a-z0-9_]+:.*" + // keyword
		"|((?s)[ \\t]*:[a-z0-9_]+[ \\t]*:.*?\\n:end:[ \\t]*)" + // drawer
		"|[ \\t]*\\[fn:[^]].*" + // footnote
		"|([ \\t]*[-+]|[ \\t]+\\*|[ \\t]*([0-9]+|[a-z])\\.)([ \t].*|$)" + // list item
		"|[ \\t]*(\\|.*\\||\\+-.*\\+).*" + // table line
		"|[ \\t]*-----+" + // hr
		"|((?s)[ \\t]*\\\\begin\\{.*?\\n[ \\t]\\\\end\\{).*" + // latex env
		"|^[ \\t]*" + // blank line
		")(\\n|$)" +
		""
)

const (
	PARAGRAPH = iota
	HEADLINE
	BLOCK
	DYNBLOCK
	KEYWORD
	DRAWER
	FOOTNOTE
	LIST_ITEM
	TABLE_LINE
	HR
	LATEXT_ENV
	BLANK
)

var types = []string{
	"PARAGRAPH",
	"HEADLINE",
	"BLOCK",
	"DYNBLOCK",
	"KEYWORD",
	"DRAWER",
	"FOOTNOTE",
	"LIST_ITEM",
	"TABLE_LINE",
	"HR",
	"LATEXT_ENV",
	"BLANK",
}

//remove [ \\t] from starts
var partPat = compile(part)
var blockPat = compile("^(?i)[ \\t]*#\\+begin(_[a-z0-9_]+)")
var dynblockPat = compile("^(?i)[ \\t]*#\\+begin:")
var keywordPat = compile("^[ \\t]*#\\+[a-zA-Z0-9_]+:")
var drawerPat = compile("^(?i)[ \\t]*:[a-z0-9_]+[ \\t]*:")
var footnotePat = compile("^(?i)[ \\t]*\\[fn:[^]]")
var listItemPat = compile("^(?i)([ \\t]*[+-]|[ \\t]+\\*|[ \\t]*([0-9]*|[a-z])\\.)([ \\t]|$)")
var tableLinePat = compile("^(?i)[ \\t]*(\\||\\+-)")
var hrPat = compile("^(?i)[ \\t]*-----+")
var latexEnvPat = compile("^(?i)[ \\t]*\\\\begin\\{.*?\\n[ \\t]\\\\end\\{")
var blankLine = compile("^[ \\t]*\\n$")
var member struct{}
var fileID = 0
var genSQL = false
var diag = true

func compile(str string) *regexp.Regexp {
	pat, err := regexp.Compile(str)
	if err == nil {return pat}
	panic(err)
}

func forParts(str string, code func(int, int, int)) {
	para := 0
	lineEnd := 0
	for pos := 0; pos < len(str); pos += lineEnd + 1 {
		curStr := str[pos:]
		lineEnd = strings.IndexByte(curStr, '\n')
		if lineEnd < 0 {
			lineEnd = len(curStr)
		}
		typ, blockEnd := classify(curStr[:lineEnd])
		if typ == LIST_ITEM {
			item := curStr[:lineEnd]
			offset := len(item) - len(strings.TrimLeft(item, " \t"))
			nextStr := curStr[lineEnd:]
			listEnd := lineEnd
			for {
				nextEnd := strings.IndexByte(nextStr, '\n')
				if nextEnd < 0 {
					nextEnd = len(nextStr)
				}
				if nextEnd == listEnd {break}
				nextItem := nextStr[:nextEnd]
				itemType, _ := classify(nextItem)
				if itemType == PARAGRAPH {
					nextOffset := len(nextItem) - len(strings.TrimLeft(nextItem, " \t"))
					if nextOffset <= offset {break}
				} else if itemType != BLANK {
					break
				}
				listEnd += nextEnd + 1
				nextStr = nextStr[nextEnd+1:]
			}
			lineEnd = listEnd
		} else if blockEnd != "" { // move lineEnd to the end of the block
			for {
				sub := curStr[lineEnd+1:]
				subEnd := strings.IndexByte(sub, '\n')
				if subEnd < 0 {
					subEnd = len(sub)
				}
				trimmed := strings.ToLower(strings.Trim(sub[:subEnd], " \t"))
				lineEnd += 1 + subEnd
				if lineEnd >= len(curStr) || trimmed == blockEnd {break}
			}
		}
		if typ != PARAGRAPH {
			if para < pos {
				code(PARAGRAPH, para, pos)
			}
			if typ != BLANK {
				code(typ, pos, pos+lineEnd)
			}
			para = pos + lineEnd + 1
		}
	}
	if para < len(str) {
		code(PARAGRAPH, para, len(str))
	}
}

func classify(curStr string) (int, string) {
	trimmed := strings.TrimLeft(curStr, " \t")
	if trimmed == "" {
		return BLANK, ""
	} else if curStr[0] == '*' {
		return HEADLINE, ""
	} else if trimmed[0] == '[' && footnotePat.MatchString(trimmed) {
		return FOOTNOTE, ""
	} else if len(trimmed) > 4 && strings.HasPrefix(trimmed, "-----") && hrPat.MatchString(trimmed) {
		return HR, ""
	} else if len(trimmed) > 7 && strings.HasPrefix(trimmed, "\\begin") && latexEnvPat.MatchString(trimmed) {
		return LATEXT_ENV, ""
	} else if trimmed[0] == ':' && drawerPat.MatchString(trimmed) {
		return DRAWER, ":end:"
	} else if len(trimmed) > 2 && strings.HasPrefix(trimmed, "#+") {
		if match := blockPat.FindStringSubmatch(trimmed); match != nil {
			return BLOCK, fmt.Sprintf("#+end_%s", strings.ToLower(match[1]))
		} else if dynblockPat.MatchString(trimmed) {
			return DYNBLOCK, "#+end:"
		} else if keywordPat.MatchString(trimmed) {
			return KEYWORD, ""
		}
		return PARAGRAPH, ""
	} else if listItemPat.MatchString(trimmed) {
		return LIST_ITEM, ""
	} else if tableLinePat.MatchString(trimmed) {
		return TABLE_LINE, ""
	}
	return PARAGRAPH, ""
}

func check(err error) {
	if err != nil {
		panic(fmt.Sprintf("Error: %s, args: %v", err.Error(), flag.Args()))
	}
}

func index(db *sql.DB, file string) {
	//f := bufio.NewWriter(os.Stdout)
	//defer func() { _ = f.Flush() }()
	f := os.Stdout
	contents, err := ioutil.ReadFile(file)
	check(err)
	str := string(contents)
	var itemIDHolder *int
	itemID := 0
	if genSQL {
		err = db.QueryRow("select max(rowid) from items;").Scan(&itemIDHolder)
		check(err)
		if itemIDHolder != nil {
			itemID = *itemIDHolder
		} else {
			itemID = 1
		}
		_, _ = fmt.Fprintln(f, "begin transaction;")
	}
	forParts(str, func(typ int, start int, end int) {
		if start < end && str[end-1] == '\n' {
			end--
		}
		if diag {
			_, _ = fmt.Fprintf(f, "@@@ %s: '%s'\n", types[typ], str[start:end])
		}
		if genSQL {
			g := grams(str[start:end])
			if len(g) > 0 {
				//_, _ = fmt.Fprintf(f, "/* item: %s, pos: %d, length: %d */\n", types[typ], start, end-start)
				_, _ = fmt.Fprintf(f, "insert into items values (%d, %d, %d, %d);\n", itemID, fileID, start+
					1, end+1)
				_, _ = fmt.Fprint(f, "insert into grams values ")
				itemID++
				first := true
				for gram := range g {
					if first {
						first = false
					} else {
						_, _ = fmt.Fprint(f, ", ")
					}
					_, _ = fmt.Fprintf(f, "(null, %d, %d)", itemID, gram)
				}
				_, _ = fmt.Fprintln(f, ";")
			}
		}
	})
	if genSQL {
		_, _ = fmt.Fprintln(f, "end transaction;")
		_, _ = fmt.Fprintln(f, "vacuum;")
		check(err)
	}
}

const (
	GRAM_ZERO   = 1
	GRAM_A      = 11
	GRAM_BASE   = GRAM_A + 26
	GRAM_2_BASE = GRAM_BASE * GRAM_BASE
	GRAM_3_BASE = GRAM_2_BASE * GRAM_BASE
)

// 3 digits in base 37 fits into two bytes
func grams(str string) map[int]struct{} {
	str = str + " "
	crackers := make(map[string]struct{})
	cracker := "..."
	result := make(map[int]struct{})
	gram := 0
	v := 0
	count := 0
	for _, c := range str {
		if '0' <= c && c <= '9' {
			v = int(c-'0') + GRAM_ZERO
		} else if 'A' <= c && c <= 'Z' {
			v = int(c-'A') + GRAM_A
		} else if 'a' <= c && c <= 'z' {
			v = int(c-'a') + GRAM_A
			c = c - 'a' + 'A'
		} else {
			if gram%GRAM_BASE == 0 {continue} // don't append more than one space
			v = 0
			c = '.'
		}
		if gram%GRAM_BASE == 0 { // starting a word
			cracker = fmt.Sprintf("..%c", c)
			gram = v
		} else {
			cracker = fmt.Sprintf("%c%c%c", cracker[1], cracker[2], c)
			gram = ((gram * GRAM_BASE) + v) % GRAM_3_BASE
		}
		if gram >= GRAM_BASE {
			crackers[cracker] = member
			result[gram] = member
		}
		count++
	}
	//fmt.Printf("/* grams[%d, %d]:", len(result), len(crackers))
	//for crumb := range crackers {
	//	fmt.Printf(" %s", crumb)
	//}
	//fmt.Printf(" */\n")
	return result
}

func main() {
	flag.Usage = printUsage
	flag.BoolVar(&diag, "v", false, "verbose")
	flag.IntVar(&lmdbConfig.gramSize, "s", 0, "gram size")
	flag.StringVar(&lmdbConfig.delimiter, "d", ",", "delimiter for unicode tags")
	flag.BoolVar(&lmdbConfig.nameHex, "nx", false, "use hex instead of unicode for the document name")
	flag.BoolVar(&lmdbConfig.tagHex, "tx", false, "use hex instead of unicode for tags")
	flag.BoolVar(&lmdbConfig.tagHex, "dx", false, "use hex instead of unicode for object data")
	flag.StringVar(&lmdbConfig.data, "data", "", "data to define for object")
	flag.BoolVar(&lmdbConfig.tags, "tags", false, "get: specify tags for intead of text\n"+
		"stat: output tags for object")
	flag.Parse()
	args := flag.Args()
	if len(args) > 0 && args[0] == "sql" {
		if len(args) != 3 {
			usage()
		}
		if !genSQL {
			diag = true
		}
		//defer profile.Start().Stop()
		db, err := sql.Open("sqlite3", args[2])
		check(err)
		fileID, err = strconv.Atoi(args[1])
		check(err)
		defer func() {
			_ = db.Close()
		}()
		index(db, args[0])
	} else if !runLmdb() {
		usage()
	}
}

func usage() {
	printUsage()
	os.Exit(1)
}

func printUsage() {
	prog, err := filepath.Abs(os.Args[0])
	if err == nil {
		prog = filepath.Base(prog)
	} else {
		prog = fmt.Sprintf("<BAD PROGRAM PATH: %s>", os.Args[0])
	}
	fmt.Fprintf(flag.CommandLine.Output(),
		`Usage:
   `+prog+` sql [-v] FILE FILE-ID DB                                    - generage SQL for FILE
   `+prog+` stat [-dx | -nx | -tags | -tx] [GROUP] DB                   - print stats for GROUP or whole database
   `+prog+` create [-s GRAMSIZE] DB                                     - create DATABASE if it does not exist
   `+prog+` grams [-nx | -d DELIM | -tx | -data D | -dx] GROUP GRAMS DB - add GRAMS to GROUP
   `+prog+` doc [-nx | -data D | -dx] GROUP DOC DB                      - add tags for DOC to GROUP
   `+prog+` delete [-nx] GROUP DB                                       - delete GROUP, its docs, and tag entries
   `+prog+` search TEXT DB                                              - query with TEXT for objects
   `+prog+` search -tags [-d D | -tx] TAGS DB                           - find objects that have tags TAGS
   `+prog+` data [-nx | -dx] GROUP DB                                   - get data for each doc in GROUP

   `+prog+` is targeted for groups of small documents, like lines in a file.`)
	flag.PrintDefaults()
}
