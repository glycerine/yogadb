package main

import (
	"fmt"
	"os"
	"time"

	"github.com/glycerine/yogadb"
)

var newline = []byte("\n")
var colonarrow = []byte(" ::: ")

const cmd = "ydiff"

func main() {

	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "%v error: provide paths to the two databases\n", cmd)
		os.Exit(1)
	}
	dbPathA := os.Args[1]
	if !dirExists(dbPathA) {
		fmt.Fprintf(os.Stderr, "%v error: path to database does not exist: '%v'\n", cmd, dbPathA)
		os.Exit(1)
	}

	dbPathB := os.Args[2]
	if !dirExists(dbPathB) {
		fmt.Fprintf(os.Stderr, "%v error: path to database does not exist: '%v'\n", cmd, dbPathB)
		os.Exit(1)
	}

	cfg := &yogadb.Config{
		OmitFlexSpaceOpsRedoLog: true,
		OmitMemWalFsync:         true,
	}
	dbA, err := yogadb.OpenFlexDB(dbPathA, cfg)
	panicOn(err)

	dbB, err := yogadb.OpenFlexDB(dbPathB, cfg)
	panicOn(err)

	t0 := time.Now()
	_ = t0

	// skip db.Close to avoid stalling on fsyncing the files again.

	diff := yogadb.FirstDiff(dbA, dbB)
	fmt.Printf("elapsed %v\n diff: \n%v\n", time.Since(t0), diff)
}
