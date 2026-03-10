package main

// bench_gc.go — GC overhead benchmark (equivalent to C's gc.sh / Fig 11f).
//
// Phase 1: Sequential fill with 4 threads
// Phase 2: 100M overwrites with Zipfian or clustered Zipfian
//          (this creates garbage that triggers GC)

import (
	"fmt"
	"math/rand"
	"os"

	yogadb "github.com/glycerine/yogadb"
)

func runGCBench(args []string) {
	cf, _ := parseCommonFlags(args)
	printHeader("GC", cf)

	// Enable piggyback GC for this benchmark
	db, err := openGCDB(cf.Dir, cf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}

	p := cf.Profile
	maxKey := uint64(cf.Count - 1)
	val := makeValue(p.ValLen)

	// Phase 1: Fill
	fmt.Println("--- Phase 1: Sequential Fill ---")
	parallelFill(db, cf.Count, 4, p.KeyLen, p.ValLen)

	// Phase 2: Overwrites to create garbage.
	// Default 100M, but can be reduced via -count for quick testing.
	overwriteOps := int64(100_000_000)
	if cf.Count < overwriteOps {
		overwriteOps = cf.Count // use -count if smaller
	}
	fmt.Printf("--- Phase 2: %s overwrites (%d ops) ---\n", cf.Dist, overwriteOps)

	switch cf.Dist {
	case "zipf":
		RunParallel("gc-overwrite-zipf", 4, overwriteOps,
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

	case "czipf":
		RunParallel("gc-overwrite-czipf", 4, overwriteOps,
			func(workerID int, rng *rand.Rand, db *yogadb.FlexDB, ops int64) int64 {
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
	}

	closeDB(db)
}

func openGCDB(dir string, cf *CommonFlags) (*yogadb.FlexDB, error) {
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0o755)

	cfg := &yogadb.Config{
		CacheMB:                    cf.CacheMB,
		NoDisk:                     cf.NoDisk,
		OmitMemWalFsync:           cf.OmitWALSync,
		PiggybackGC_on_SyncOrFlush: true,
		GCGarbagePct:               0.50,
	}
	return yogadb.OpenFlexDB(dir, cfg)
}
