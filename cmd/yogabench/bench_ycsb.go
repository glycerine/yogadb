package main

// bench_ycsb.go — YCSB workload benchmarks (equivalent to C's ycsb.sh / Fig 12a).
//
// Workloads:
//   A: 50% read, 50% update
//   B: 95% read, 5% update
//   C: 100% read
//   D: 95% read, 5% insert (latest distribution)
//   E: 95% scan, 5% insert
//   F: 50% read, 50% RMW (read-modify-write)

import (
	"fmt"
	"math/rand"
	"os"
	"strings"

	yogadb "github.com/glycerine/yogadb"
)

// YCSBWorkload defines the operation mix for a YCSB workload.
type YCSBWorkload struct {
	Name      string
	PctGet    int  // percentage of read (get) ops
	PctScan   int  // percentage of scan ops
	PctSet    int  // percentage of set (insert/update) ops
	PctRMW    int  // percentage of read-modify-write ops
	ScanLen   int  // keys per scan
	UseLatest bool // use latest distribution instead of Zipfian
}

var ycsbWorkloads = map[string]YCSBWorkload{
	"A": {Name: "A", PctGet: 50, PctScan: 0, PctSet: 50, PctRMW: 0, ScanLen: 50},
	"B": {Name: "B", PctGet: 95, PctScan: 0, PctSet: 5, PctRMW: 0, ScanLen: 50},
	"C": {Name: "C", PctGet: 100, PctScan: 0, PctSet: 0, PctRMW: 0, ScanLen: 50},
	"D": {Name: "D", PctGet: 95, PctScan: 0, PctSet: 5, PctRMW: 0, ScanLen: 50, UseLatest: true},
	"E": {Name: "E", PctGet: 0, PctScan: 95, PctSet: 5, PctRMW: 0, ScanLen: 50},
	"F": {Name: "F", PctGet: 50, PctScan: 0, PctSet: 0, PctRMW: 50, ScanLen: 50},
}

func runYCSBBench(args []string) {
	cf, _ := parseCommonFlags(args)

	workloadName := stringFlagFromArgs(args, "-workload", "ALL")

	p := cf.Profile
	maxKey := uint64(cf.Count - 1)

	// Determine which workloads to run
	var workloads []YCSBWorkload
	if strings.ToUpper(workloadName) == "ALL" {
		for _, name := range []string{"A", "B", "C", "D", "E", "F"} {
			workloads = append(workloads, ycsbWorkloads[name])
		}
	} else {
		w, ok := ycsbWorkloads[strings.ToUpper(workloadName)]
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown YCSB workload: %s\n", workloadName)
			os.Exit(1)
		}
		workloads = []YCSBWorkload{w}
	}

	fmt.Printf("=== YCSB === dataset=%s\n", p.Name)

	db, err := openDB(cf.Dir, cf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}

	// Fill
	fmt.Println("--- Fill ---")
	parallelFill(db, cf.Count, cf.Threads, p.KeyLen, p.ValLen)

	// Run each workload
	for _, w := range workloads {
		fmt.Printf("--- YCSB Workload %s: get=%d%% scan=%d%% set=%d%% rmw=%d%% ---\n",
			w.Name, w.PctGet, w.PctScan, w.PctSet, w.PctRMW)

		wLocal := w // capture for closure

		RunTimed(fmt.Sprintf("ycsb-%s", w.Name), cf.Threads, cf.Duration,
			func(workerID int, rng *rand.Rand, db *yogadb.FlexDB, ops int64) int64 {
				var readDist Distribution
				var writeDist Distribution

				if wLocal.UseLatest {
					lg := NewLatest(rng, 1000)
					readDist = &latestReadAdapter{lg}
					writeDist = &latestWriteAdapter{lg}
				} else {
					readDist = NewZipfian(rng, 0, maxKey)
					writeDist = NewZipfian(rng, 0, maxKey)
				}

				keyBuf := make([]byte, p.KeyLen)
				val := makeValue(p.ValLen)
				success := int64(0)

				// Pre-compute thresholds (scaled to 65536 like C code)
				threshGet := uint32(wLocal.PctGet) * 65536 / 100
				threshScan := uint32(wLocal.PctGet+wLocal.PctScan) * 65536 / 100
				threshSet := uint32(wLocal.PctGet+wLocal.PctScan+wLocal.PctSet) * 65536 / 100

				for i := int64(0); i < ops; i++ {
					roll := uint32(rng.Int63()) & 0xffff

					if roll < threshGet {
						// GET
						k := string(hexKeyBuf(keyBuf, readDist.Next(), p.KeyLen))
						if _, found := db.Get(k); found {
							success++
						}
					} else if roll < threshScan {
						// SCAN
						k := string(hexKeyBuf(keyBuf, readDist.Next(), p.KeyLen))
						db.View(func(ro *yogadb.ReadOnlyTx) error {
							it := ro.NewIter()
							it.Seek(k)
							for j := 0; j < wLocal.ScanLen; j++ {
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
					} else if roll < threshSet {
						// SET
						k := string(hexKeyBuf(keyBuf, writeDist.Next(), p.KeyLen))
						if err := db.Put(k, val); err == nil {
							success++
						}
					} else {
						// RMW (read-modify-write)
						k := string(hexKeyBuf(keyBuf, writeDist.Next(), p.KeyLen))
						err := db.Merge(k, func(oldVal []byte, exists bool) ([]byte, bool, bool) {
							return val, true, false
						})
						if err == nil {
							success++
						}
					}
				}
				return success
			}, db)
	}

	closeDB(db)
}

// latestReadAdapter adapts LatestGen for the Distribution interface (reads).
type latestReadAdapter struct{ lg *LatestGen }

func (a *latestReadAdapter) Next() uint64 { return a.lg.NextRead() }

// latestWriteAdapter adapts LatestGen for the Distribution interface (writes).
type latestWriteAdapter struct{ lg *LatestGen }

func (a *latestWriteAdapter) Next() uint64 { return a.lg.NextWrite() }
