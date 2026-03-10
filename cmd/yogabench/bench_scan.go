package main

// bench_scan.go — Range scan benchmark (equivalent to C's read.sh scan part / Fig 11d).
//
// Tests seek+next with various scan lengths (10, 20, 50, 100).
// Database is pre-filled, then scans run for 60 seconds.

import (
	"fmt"
	"math/rand"
	"os"

	yogadb "github.com/glycerine/yogadb"
)

func runScanBench(args []string) {
	cf, _ := parseCommonFlags(args)

	// Parse -scanlen from args manually (flag package already consumed common flags)
	scanLen := intFlagFromArgs(args, "-scanlen", 0)

	printHeader(fmt.Sprintf("SCAN-%d", scanLen), cf)

	db, err := openDB(cf.Dir, cf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}

	p := cf.Profile
	maxKey := uint64(cf.Count - 1)
	fillThreads := cf.Threads
	if fillThreads < 4 {
		fillThreads = 4
	}

	// Fill
	fmt.Println("--- Fill ---")
	parallelFill(db, cf.Count, fillThreads, p.KeyLen, p.ValLen)

	// Scan with varying lengths
	scanLens := []int{10, 20, 50, 100}
	if scanLen > 0 {
		scanLens = []int{scanLen}
	}

	for _, sl := range scanLens {
		fmt.Printf("--- Scan length=%d ---\n", sl)
		scanLenLocal := sl
		RunTimed(fmt.Sprintf("scan-%d", sl), cf.Threads, cf.Duration,
			func(workerID int, rng *rand.Rand, db *yogadb.FlexDB, ops int64) int64 {
				zipf := NewZipfian(rng, 0, maxKey)
				keyBuf := make([]byte, p.KeyLen)
				success := int64(0)
				for i := int64(0); i < ops; i++ {
					k := string(hexKeyBuf(keyBuf, zipf.Next(), p.KeyLen))
					db.View(func(ro *yogadb.ReadOnlyTx) error {
						it := ro.NewIter()
						it.Seek(k)
						for j := 0; j < scanLenLocal; j++ {
							if !it.Valid() {
								break
							}
							it.Next()
						}
						if it.Valid() {
							success++
						}
						return nil
					})
				}
				return success
			}, db)
	}

	closeDB(db)
}
