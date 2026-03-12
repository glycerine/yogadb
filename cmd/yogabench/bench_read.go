package main

// bench_read.go — Point read benchmark (equivalent to C's read.sh / Fig 11c).
//
// Phase 1: Parallel sequential fill (4 threads)
// Phase 2: Warmup scan (1 thread, single pass)
// Phase 3: Point reads with Zipfian distribution
// Phase 4: Point reads with clustered Zipfian distribution

import (
	"fmt"
	"math/rand"
	"os"

	yogadb "github.com/glycerine/yogadb"
)

func runReadBench(args []string) {
	cf, _ := parseCommonFlags(args)
	printHeader("READ", cf)

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

	// Phase 1: Fill
	fmt.Println("--- Phase 1: Sequential Fill ---")
	parallelFill(db, cf.Count, fillThreads, p.KeyLen, p.ValLen)

	// Phase 2: Warmup — single-threaded Zipfian writes to warm caches
	fmt.Println("--- Phase 2: Warmup ---")
	warmupOps := cf.Count / int64(fillThreads*16)
	if warmupOps < 10000 {
		warmupOps = 10000
	}
	RunParallel("warmup", 1, warmupOps, func(workerID int, rng *rand.Rand, db *yogadb.FlexDB, ops int64) int64 {
		zipf := NewZipfian(rng, 0, maxKey)
		keyBuf := make([]byte, p.KeyLen)
		val := makeValue(p.ValLen)
		success := int64(0)
		for i := int64(0); i < ops; i++ {
			k := string(hexKeyBuf(keyBuf, zipf.Next(), p.KeyLen))
			if err := db.Put(k, val); err == nil {
				success++
			}
		}
		return success
	}, db)

	// Phase 3: Zipfian point reads (timed, 60s)
	fmt.Println("--- Phase 3: Zipfian Point Read ---")
	RunTimed("get-zipf", cf.Threads, cf.Duration, func(workerID int, rng *rand.Rand, db *yogadb.FlexDB, ops int64) int64 {
		zipf := NewZipfian(rng, 0, maxKey)
		keyBuf := make([]byte, p.KeyLen)
		success := int64(0)
		for i := int64(0); i < ops; i++ {
			k := string(hexKeyBuf(keyBuf, zipf.Next(), p.KeyLen))
			if _, found, _ := db.Get(k); found {
				success++
			}
		}
		return success
	}, db)

	// Phase 4: Clustered Zipfian point reads (timed, 60s)
	fmt.Println("--- Phase 4: Clustered Zipfian Point Read ---")
	RunTimed("get-czipf", cf.Threads, cf.Duration, func(workerID int, rng *rand.Rand, db *yogadb.FlexDB, ops int64) int64 {
		czipf := NewClusteredZipfian(rng, 0, maxKey, 1000)
		keyBuf := make([]byte, p.KeyLen)
		success := int64(0)
		for i := int64(0); i < ops; i++ {
			k := string(hexKeyBuf(keyBuf, czipf.Next(), p.KeyLen))
			if _, found, _ := db.Get(k); found {
				success++
			}
		}
		return success
	}, db)

	closeDB(db)
}
