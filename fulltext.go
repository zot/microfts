package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/pkg/profile"
)

const (
	orgPatStr = "" +
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
	ERROR = iota + 1
	ERROR_FILE_MISSING
	ERROR_FILE_CHANGED
	ERROR_FILE_UNREADABLE
	ERROR_FILE_NOT_IN_DB
	ERROR_DB_MISSING
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

type gram uint16

const (
	GRAM_ZERO   = gram(1)
	GRAM_A      = gram(11)
	GRAM_BASE   = gram(GRAM_A + 26)
	GRAM_2_BASE = gram(GRAM_BASE * GRAM_BASE)
	GRAM_3_BASE = gram(GRAM_2_BASE * GRAM_BASE)
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
var orgPat = compile(orgPatStr)
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
var genSQL = true
var diag = false
var customFts = false

func compile(str string) *regexp.Regexp {
	pat, err := regexp.Compile(str)
	if err == nil {return pat}
	panic(err)
}

func forParts(str string, code func(int, int, int, int)) {
	var pos, typ, start, end, prev int
	for line := 1; pos < len(str); pos = end {
		typ, start, end = orgPart(pos, str)
		line += strings.Count(str[prev:start], "\n")
		code(line, typ, start, end-(len(str[start:end])-len(strings.TrimRight(str[start:end], " \t\n"))))
		prev = start
	}
}
func orgPart(pos int, str string) (int, int, int) {
	para := pos
	lineEnd := 0
	for ; pos < len(str); pos += lineEnd + 1 {
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
				if nextEnd+1 >= len(nextStr) {break}
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
			if para < pos {return PARAGRAPH, para, pos}
			if typ != BLANK {return typ, pos, pos + lineEnd}
			para = pos + lineEnd + 1
		} else if pos-para > 1 && str[pos-2:pos] == "\n\n" {
			return PARAGRAPH, para, pos
		}
	}
	if para < len(str) {return PARAGRAPH, para, len(str)}
	return BLANK, len(str), len(str)
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

// 3 digits in base 37 fits into two bytes
func grams(partial bool, args ...string) map[gram]struct{} {
	result := map[gram]struct{}{}
	if partial {
		for _, term := range args {
			addGrams(true, term, result)
		}
	} else {
		addGrams(false, " "+strings.Join(args, " ")+" ", result)
	}
	return result
}

// 3 digits in base 37 fits into two bytes
func addGrams(partial bool, str string, result map[gram]struct{}) {
	var grm gram
	for _, c := range str {
		v := gramForChar(c)
		if v == 0 && grm%GRAM_BASE == 0 {continue} // don't append more than one space
		if grm%GRAM_BASE == 0 { // starting a word
			grm = v
		} else {
			grm = gram(((int(grm) * int(GRAM_BASE)) + int(v)) % int(GRAM_3_BASE))
		}
		if partial && (grm/GRAM_2_BASE == 0 || grm/GRAM_BASE%GRAM_BASE == 0 || grm%GRAM_BASE == 0) {continue}
		if grm >= GRAM_BASE { // don't track grams with two leading spaces
			result[grm] = member
		}
	}
}

func gramString(grm gram) string {
	g1 := charForGram((grm / GRAM_2_BASE) % GRAM_BASE)
	g2 := charForGram((grm / GRAM_BASE) % GRAM_BASE)
	g3 := charForGram(grm % GRAM_BASE)
	return string([]byte{g1, g2, g3})
}

func charForGram(grm gram) byte {
	if grm == 0 {
		return '.'
	} else if grm < GRAM_A {
		return '0' + byte(grm-GRAM_ZERO)
	}
	return 'A' + byte(grm-GRAM_A)
}

func gramForChar(c rune) gram {
	if '0' <= c && c <= '9' {
		return gram(c-'0') + GRAM_ZERO
	} else if 'A' <= c && c <= 'Z' {
		return gram(c-'A') + GRAM_A
	} else if 'a' <= c && c <= 'z' {
		return gram(c-'a') + GRAM_A
	}
	return 0
}

func gramForUnicode(str string) gram {
	if len(str) != 3 {
		exitError(fmt.Sprintf("Unicode gram is not a trigram: '%s'", str), 1)
	}
	var grm gram
	for i := 0; i < 3; i++ {
		c := gramForChar(rune(str[i]))
		if grm%GRAM_BASE == 0 && c == 0 {continue}
		if grm%GRAM_BASE == 0 { // starting a word
			grm = c
		} else {
			grm = ((grm * GRAM_BASE) + c) % GRAM_3_BASE
		}
	}
	return grm
}

func exitError(arg interface{}, code int) {
	fmt.Fprintln(os.Stderr, arg)
	os.Exit(code)
}

func main() {
	//testGrams()
	//testNums()
	if len(os.Args) == 1 {
		usage()
	}
	flag.Usage = printUsage
	prof := false
	flag.BoolVar(&prof, "prof", false, "profile cpu")
	flag.BoolVar(&diag, "v", false, "verbose")
	flag.IntVar(&lmdbConfig.gramSize, "s", 0, "gram size")
	flag.StringVar(&lmdbConfig.delimiter, "d", ",", "delimiter for unicode tags")
	flag.BoolVar(&lmdbConfig.gramHex, "gx", false, "use hex instead of unicode for grams")
	flag.BoolVar(&lmdbConfig.gramDec, "gd", false, "use decimal instead of unicode for grams")
	flag.BoolVar(&lmdbConfig.dataHex, "dx", false, "use hex instead of unicode for object data")
	flag.StringVar(&lmdbConfig.dataString, "data", "", "data to define for object")
	flag.BoolVar(&lmdbConfig.candidates, "candidates", false, "return docs with grams for search")
	flag.BoolVar(&lmdbConfig.separate, "sep", false, "print candidates on separate lines")
	flag.BoolVar(&lmdbConfig.numbers, "n", false, "only print line numbers for search")
	flag.IntVar(&lmdbConfig.limit, "limit", maxInt, "search: limit the number of results")
	flag.BoolVar(&lmdbConfig.org, "org", false, "index org-mode chunks instead of lines")
	flag.BoolVar(&lmdbConfig.sexp, "sexp", false, `search: output matches as an s-expression ((FILE (POS LINE OFFSET chunk) ... ) ... )
POS is the 1-based character position of the chunk in the file
LINE is the 1-based line of the chunk in the file
OFFSET is the 0-based offset of the first match in the chunk`)
	flag.BoolVar(&lmdbConfig.partial, "partial", false, "search: allow partial matches in search")
	flag.BoolVar(&lmdbConfig.force, "f", false, "search: skip changed and missing files instead of exiting")
	flag.BoolVar(&lmdbConfig.test, "t", false, "update: do a test run, printing what would have happened")
	flag.BoolVar(&lmdbConfig.autoupdate, "u", false, "search: update the database before searching")
	flag.Float64Var(&lmdbConfig.fuzzy, "fuzzy", 0, "search: specify a percentage fuzzy match")
	flag.StringVar(&lmdbConfig.compression, "comp", "", "compression type to use when creating a database")
	flag.BoolVar(&lmdbConfig.groups, "groups", false, "info: display information for each group")
	flag.StringVar(&lmdbConfig.filter, "filter", "", "search: filter results that match REGEXP")
	flag.BoolVar(&lmdbConfig.chunks, "chunks", false, "info DB GROUP: display all of a group's chunks")
	flag.BoolVar(&lmdbConfig.file, "file", false, "search: display files rather than chunks")
	flag.BoolVar(&lmdbConfig.sort, "sort", false, `search -fuzzy: sort all matches
This ignores start-format and end-format because it sorts all matches, regardless of
which file they come from.`)
	flag.StringVar(&lmdbConfig.format, "format", lineFormat, `search: Go format string for each result
Args to printf are FILE POSITION LINE OFFSET PERCENTAGE CHUNK
FILE (string) is the name of the file
POSITION (int) is the 1-based character position of the chunk in the file
LINE (int) is the 1-based line of the chunk in the file
OFFSET (int) is the 0-based offset of the first match in the chunk
PERCENTAGE (float) is the percentage of a fuzzy match
Note that you can place [ARGNUM] after the % to pick a particular arg to format
The default format is %s:%[2]s:%[5]s\n
-sexp sets format to (:filename "%s" :line %[3]d :offset %[4]d :text "%[6]s" :percent %[5]f)
  Note that this will cause all matches to be on one (potentially large) line of output`)
	flag.StringVar(&lmdbConfig.startFormat, "start-format", groupStart, `search: Go format string for the start of a group
Arg to printf is the FILE
The default value is ""
Not used with search -fuzzy -sort`)
	flag.StringVar(&lmdbConfig.endFormat, "end-format", groupEnd, `search: Go format string for the end of a group
Arg to printf is the FILE
The default value is ""
if -sexp is provided and -end-format is not, the default is "\n"
Not used with search -fuzzy -sort`)
	flag.BoolVar(&lmdbConfig.grams, "grams", false, "get: specify tags for intead of text\n"+
		"info: print gram coverage\n"+
		"search: specify grams instead of search terms")
	flag.CommandLine.Parse(os.Args[2:])
	if prof {
		defer profile.Start().Stop()
		//defer profile.Start(profile.MemProfile).Stop()
		//defer profile.Start(profile.TraceProfile).Stop()
		for i := 0; i < 1000; i++ {
			runLmdb()
		}
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
   %[1]s info -groups DB
                   print information about each group in the database,
                   whether it is missing or changed
                   whether it is an org-mode entry
   %[1]s info [-chunks] DB GROUP
                   print info for a GROUP
                   -chunks also prints the chunks in GROUP if it has a corresponding file
   %[1]s info [-grams] DB
                   print info for database
                   displays any groups which do not exist as files
                   displays any groups which refer to files that have changed
                   -grams displays distribution information about the trigram index
   %[1]s create [-s GRAMSIZE] DB
                   create DATABASE if it does not exist
   %[1]s chunk [-nx | -data D | -dx] -d DELIM DB GROUP GRAMS
   %[1]s chunk [-nx | -data D | -dx] -gx DB GROUP GRAMS
                   ADD a chunk to GROUP with GRAMS.
                   -d means use DELIM to split GRAMS.
                   -gx means GRAMS is hex encoded with two bytes for each gram using base 37.
   %[1]s grams [-gx] CHUNK
                   output grams for CHUNK
   %[1]s input [-nx | -dx | -org] DB FILE...
                   For each FILE, create a group with its name and add a CHUNK for each chunk of input.
                   Chunk data is the line number, offset, and length for each chunk (starting at 1).
                   -org means chunks are org elements, otherwise chunks are lines
   %[1]s delete [-nx] DB GROUP
                   delete GROUP, its chunks, and tag entries.
                   NOTE: THIS DOES NOT RECLAIM SPACE! USE COMPACT FOR THAT
   %[1]s compact DB
                   Reclaim space for deleted groups
   %[1]s search [-n | -partial | -f | - limit N | -filter REGEXP | -u] DB TEXT ...
                   query with TEXT for objects
                   -f force search to skip changed and missing files instead of exiting
                   -filter makes search only return chunks that match the REGEXP
                   REGEXP syntax is here: https://golang.org/pkg/regexp/syntax/
   %[1]s search -candidates [-grams | -gx | -gd | -n | -f | -limit N | -dx | -u] DB TERM1 ...
                   dispay all candidates with the grams for TERMS without filtering
                   -grams indicates TERMS are grams, otherwise extract grams from TERMS
                   -gx: grams are in hex, -gd: grams are in decimal, otherwise they are 3-char strings
   %[1]s data [-nx | -dx] DB GROUP
                   get data for each doc in GROUP
   %[1]s update [-t] DB
                   reinput files that have changed
                   delete files that have been removed
                   -t means do a test run, printing what would have happened
   %[1]s empty DB GROUP...
                   Create empty GROUPs, ignoring existing ones

   %[1]s is targeted for groups of small documents, like lines in a file.

`, prog)
	flag.PrintDefaults()
}
