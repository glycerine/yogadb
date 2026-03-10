package main

// bench_scale.go — Scalability benchmarks (equivalent to C's scale.sh + scale-write.sh / Fig 11e).
//
// Write scalability: vary goroutine count from 1 to 8.
// Read scalability: fill once, then test reads at each thread count.

import (
	"fmt"
	"math/rand"
	"os"

	yogadb "github.com/glycerine/yogadb"
)

func runScaleWriteBench(args []string) {
	cf, _ := parseCommonFlags(args)

	threadListStr := stringFlagFromArgs(args, "-thread-list", "1,2,3,4,5,6,7,8")
	threads := parseThreadList(threadListStr)
	if len(threads) == 0 {
		threads = []int{1, 2, 3, 4, 5, 6, 7, 8}
	}

	p := cf.Profile
	maxKey := uint64(cf.Count - 1)
	val := makeValue(p.ValLen)

	fmt.Printf("=== SCALE-WRITE === dataset=%s\n", p.Name)

	for _, nt := range threads {
		// Fresh DB per thread count
		db, err := openDB(cf.Dir, cf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open db: %v\n", err)
			os.Exit(1)
		}

		opsPerThread := cf.Count / int64(nt)

		RunParallel(fmt.Sprintf("scale-write-t%d", nt), nt, opsPerThread*int64(nt),
			func(workerID int, rng *rand.Rand, db *yogadb.FlexDB, ops int64) int64 {
				zipf := NewZipfian(rng, 0, maxKey)
				keyBuf := make([]byte, p.KeyLen)
				success := int64(0)
				for i := int64(0); i < ops; i++ {
					k := string(hexKeyBuf(keyBuf, zipf.Next(), p.KeyLen))
					if err := db.Put(k, val); err == nil {
						success++
					}
				}
				return success
			}, db)

		closeDB(db)
	}
}

func runScaleReadBench(args []string) {
	cf, _ := parseCommonFlags(args)

	threadListStr := stringFlagFromArgs(args, "-thread-list", "1,2,3,4,5,6,7,8")
	threads := parseThreadList(threadListStr)
	if len(threads) == 0 {
		threads = []int{1, 2, 3, 4, 5, 6, 7, 8}
	}

	p := cf.Profile
	maxKey := uint64(cf.Count - 1)

	fmt.Printf("=== SCALE-READ === dataset=%s\n", p.Name)

	db, err := openDB(cf.Dir, cf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}

	// Fill once
	fmt.Println("--- Fill ---")
	fillThreads := 4
	parallelFill(db, cf.Count, fillThreads, p.KeyLen, p.ValLen)

	// Warmup
	fmt.Println("--- Warmup ---")
	RunParallel("warmup", 1, cf.Count/64, func(workerID int, rng *rand.Rand, db *yogadb.FlexDB, ops int64) int64 {
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

	// Point read scalability
	for _, nt := range threads {
		RunTimed(fmt.Sprintf("scale-get-t%d", nt), nt, cf.Duration,
			func(workerID int, rng *rand.Rand, db *yogadb.FlexDB, ops int64) int64 {
				zipf := NewZipfian(rng, 0, maxKey)
				keyBuf := make([]byte, p.KeyLen)
				success := int64(0)
				for i := int64(0); i < ops; i++ {
					k := string(hexKeyBuf(keyBuf, zipf.Next(), p.KeyLen))
					if _, found := db.Get(k); found {
						success++
					}
				}
				return success
			}, db)
	}

	// Scan scalability (scan length 50)
	for _, nt := range threads {
		RunTimed(fmt.Sprintf("scale-scan50-t%d", nt), nt, cf.Duration,
			func(workerID int, rng *rand.Rand, db *yogadb.FlexDB, ops int64) int64 {
				zipf := NewZipfian(rng, 0, maxKey)
				keyBuf := make([]byte, p.KeyLen)
				success := int64(0)
				for i := int64(0); i < ops; i++ {
					k := string(hexKeyBuf(keyBuf, zipf.Next(), p.KeyLen))
					db.View(func(ro *yogadb.ReadOnlyTx) error {
						it := ro.NewIter()
						it.Seek(k)
						for j := 0; j < 50; j++ {
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
