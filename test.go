package main

import (
	"fmt"
	"os"
	"strings"
)

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
		verify, gNext, _ := getNum(buf)
		if verify != nums[b] {
			panic(fmt.Sprintf("ERROR: expected <%d> but got <%d>, buf = %v", nums[b], verify, buf))
		} else if len(buf)-len(next) != b/2+1 {
			panic(fmt.Sprintf("ERROR: expected number of length <%d> but got <%d>", b/2+1, len(buf)-len(next)))
		} else if len(gNext) != len(next) {
			panic(fmt.Sprintf("ERROR: expected bytes written to be <%d> but was <%d>", b/2+1, len(buf)-len(gNext)))
		}
	}
}

func testGrams() {
	for _, k := range strings.Split(".th,thi,his,hi.,.is,is.,.a.,.te,tes,est,st.", ",") {
		if strings.ToUpper(k) != gramString(gramForUnicode(k)) {
			fmt.Fprintf(os.Stderr, "Testing gram '%': failed, got '%s'\n", strings.ToUpper(k), gramString(gramForUnicode(k)))
		} else {
			fmt.Fprintf(os.Stderr, "Testing gram '%s': passed\n", strings.ToUpper(k))
		}
	}
}
