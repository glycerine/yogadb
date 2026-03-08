package main

import (
	"fmt"
	"os"
	"time"

	"github.com/glycerine/yogadb"
)

var newline = []byte("\n")
var colonarrow = []byte(" ::: ")

const cmd = "yview"

func main() {

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "%v error: provide path to database to dump as only argument.\n", cmd)
		os.Exit(1)
	}
	dbPath := os.Args[1]
	if !dirExists(dbPath) {
		fmt.Fprintf(os.Stderr, "%v error: path to database does not exist: '%v'\n", cmd, dbPath)
		os.Exit(1)
	}

	cfg := &yogadb.Config{
		OmitFlexSpaceOpsRedoLog: true,
		OmitMemWalFsync:         true,
	}
	db, err := yogadb.OpenFlexDB(dbPath, cfg)
	panicOn(err)

	t0 := time.Now()

	// skip db.Close to avoid stalling on fsyncing the files again.
	if false {
		defer func() {
			finMetrics := db.Close()
			fmt.Fprintf(os.Stderr, "time to dump: %v, finMetrics = %v\n", time.Since(t0), finMetrics)
		}()
	}

	justShowAll(db, dbPath)
}

func justShowAll(db *yogadb.FlexDB, dbPath string) {
	saw := 0
	buf := make([]byte, 0, 4<<20)
	db.Ascend(nil, func(key, value []byte) bool {
		need := 2 + len(key) + len(value)
		if len(buf)+need <= cap(buf) {
			// fine. write below.
		} else {
			os.Stdout.Write(buf)
			buf = buf[:0]
		}
		buf = append(buf, key...)
		if len(value) > 0 {
			buf = append(buf, colonarrow...)
			buf = append(buf, value...)
		}
		buf = append(buf, newline...)

		saw++
		return true
	})
	if len(buf) > 0 {
		os.Stdout.Write(buf)
	}
	os.Stdout.Sync()
	fmt.Fprintf(os.Stderr, "YogaDB saw %v key-value pair(s) in database '%v'.\n", saw, dbPath)
}
