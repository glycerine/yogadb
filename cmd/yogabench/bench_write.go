package main

// bench_write.go — Write throughput benchmark (equivalent to C's write.sh / Fig 11a).
//
// Tests:
//   - Sequential fill (S): parallel sequential key insertion
//   - Zipfian writes (s): random writes following Zipfian distribution
//   - Clustered Zipfian writes (s): random writes following clustered Zipfian

import (
	"fmt"
	"math/rand"
	"os"

	yogadb "github.com/glycerine/yogadb"
)

func runWriteBench(args []string) {
	cf, _ := parseCommonFlags(args)
	printHeader("WRITE", cf)

	db, err := openDB(cf.Dir, cf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}

	p := cf.Profile
	val := makeValue(p.ValLen)
	maxKey := uint64(cf.Count - 1)

	switch cf.Dist {
	case "seq":
		// Sequential parallel fill — each goroutine fills a contiguous range.
		parallelFill(db, cf.Count, cf.Threads, p.KeyLen, p.ValLen)

	case "zipf":
		// Zipfian writes — each goroutine gets its own Zipfian generator.
		RunParallel("write-zipf", cf.Threads, cf.Count, func(workerID int, rng *rand.Rand, db *yogadb.FlexDB, ops int64) int64 {
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

	case "czipf":
		// Clustered Zipfian writes — aggregated hot spots.
		RunParallel("write-czipf", cf.Threads, cf.Count, func(workerID int, rng *rand.Rand, db *yogadb.FlexDB, ops int64) int64 {
			czipf := NewClusteredZipfian(rng, 0, maxKey, 1000)
			keyBuf := make([]byte, p.KeyLen)
			success := int64(0)
			for i := int64(0); i < ops; i++ {
				k := string(hexKeyBuf(keyBuf, czipf.Next(), p.KeyLen))
				if err := db.Put(k, val); err == nil {
					success++
				}
			}
			return success
		}, db)

	default:
		fmt.Fprintf(os.Stderr, "unknown distribution: %s\n", cf.Dist)
		os.Exit(1)
	}

	closeDB(db)
}
