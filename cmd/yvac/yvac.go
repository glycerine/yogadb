package main

import (
	"fmt"
	"os"
	"time"

	"github.com/glycerine/yogadb"
)

var newline = []byte("\n")
var colon = []byte(":")

const cmd = "yvac"

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

	// VacuumVLOG will generate garbage into KV128_BLOCKS, so
	// do it first.

	fmt.Fprintf(os.Stderr, "starting db.VacuumVLOG()...\n")
	stats, err := db.VacuumVLOG()
	fmt.Fprintf(os.Stderr, "db.VacuumVLOG() gave err='%v'; stats = '%v'\n", err, stats)
	panicOn(err)

	fmt.Fprintf(os.Stderr, "starting db.VacuumKV()...\n")
	stats2, err := db.VacuumKV()
	fmt.Fprintf(os.Stderr, "db.VacuumKV() gave err='%v'; stats = '%v'\n", err, stats2)
	panicOn(err)

	finMetrics := db.Close()
	fmt.Fprintf(os.Stderr, "vacuum of VLOG and KV took %v. finMetrics = %v\n", time.Since(t0), finMetrics)
}
