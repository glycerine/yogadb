package main

// runner.go — Benchmark runner: manages goroutines, timing, and reporting.
// Matches the C "forker" framework's output format for comparison.

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	yogadb "github.com/glycerine/yogadb"
)

// BenchResult holds the result of a benchmark run.
type BenchResult struct {
	Label    string
	TotalOps int64
	Duration time.Duration
	Mops     float64 // million ops/sec
	Success  int64   // successful ops
}

func (r BenchResult) String() string {
	return fmt.Sprintf("%s: ops %d  %.4f Mops  (%.2fs)",
		r.Label, r.TotalOps, r.Mops, r.Duration.Seconds())
}

// WorkerFunc is called by each goroutine with its worker ID, per-worker RNG,
// the database, and the number of operations to perform.
// Returns the number of successful operations.
type WorkerFunc func(workerID int, rng *rand.Rand, db *yogadb.FlexDB, ops int64) int64

// RunParallel runs fn across nThreads goroutines, each doing opsPerThread operations.
// For parallel fill (S/G mode), each goroutine handles a contiguous key range.
func RunParallel(label string, nThreads int, totalOps int64, fn WorkerFunc, db *yogadb.FlexDB) BenchResult {
	opsPerThread := totalOps / int64(nThreads)
	var wg sync.WaitGroup
	var totalSuccess atomic.Int64

	runtime.GC() // clean slate before timing

	start := time.Now()

	for i := 0; i < nThreads; i++ {
		wg.Add(1)
		workerOps := opsPerThread
		if i == nThreads-1 {
			workerOps = totalOps - opsPerThread*int64(nThreads-1) // last worker gets remainder
		}
		go func(id int, n int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id) * 12345))
			s := fn(id, rng, db, n)
			totalSuccess.Add(s)
		}(i, workerOps)
	}

	wg.Wait()
	elapsed := time.Since(start)

	mops := float64(totalOps) / elapsed.Seconds() / 1e6

	result := BenchResult{
		Label:    label,
		TotalOps: totalOps,
		Duration: elapsed,
		Mops:     mops,
		Success:  totalSuccess.Load(),
	}
	fmt.Println(result)
	return result
}

// RunTimed runs fn for a fixed duration, reporting throughput.
func RunTimed(label string, nThreads int, duration time.Duration, fn WorkerFunc, db *yogadb.FlexDB) BenchResult {
	var wg sync.WaitGroup
	var totalOps atomic.Int64
	var totalSuccess atomic.Int64

	runtime.GC()

	deadline := time.Now().Add(duration)
	start := time.Now()

	for i := 0; i < nThreads; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id) * 12345))
			ops := int64(0)
			succ := int64(0)
			batchSize := int64(1 << 14) // 16384, matching C
			for time.Now().Before(deadline) {
				s := fn(id, rng, db, batchSize)
				ops += batchSize
				succ += s
			}
			totalOps.Add(ops)
			totalSuccess.Add(succ)
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	ops := totalOps.Load()
	mops := float64(ops) / elapsed.Seconds() / 1e6

	result := BenchResult{
		Label:    label,
		TotalOps: ops,
		Duration: elapsed,
		Mops:     mops,
		Success:  totalSuccess.Load(),
	}
	fmt.Println(result)
	return result
}

// openDB opens a YogaDB at the given directory with the given config.
func openDB(dir string, cf *CommonFlags) (*yogadb.FlexDB, error) {
	os.RemoveAll(dir) // fresh DB each run
	os.MkdirAll(dir, 0o755)

	cfg := &yogadb.Config{
		CacheMB:         cf.CacheMB,
		NoDisk:          cf.NoDisk,
		OmitMemWalFsync: cf.OmitWALSync,
	}
	return yogadb.OpenFlexDB(dir, cfg)
}

// closeDB closes the database and prints final metrics.
func closeDB(db *yogadb.FlexDB) {
	m := db.Close()
	if m != nil {
		fmt.Fprintf(os.Stderr, "metrics: logical_written=%d physical_written=%d live=%d free_in_blocks=%d blocks=%d write_amp=%.2f\n",
			m.LogicalBytesWritten, m.TotalBytesWritten,
			m.TotalLiveBytes, m.TotalFreeBytesInBlocks, m.BlocksInUse, m.WriteAmp)
	}
}

// parallelFill does a sequential parallel fill of nKeys keys.
// Each goroutine fills its contiguous range [id*chunk, (id+1)*chunk).
// Uses Batch API for efficiency — each batch commits 1024 keys.
func parallelFill(db *yogadb.FlexDB, nKeys int64, nThreads int, klen, vlen int) BenchResult {
	val := makeValue(vlen)
	const batchCommitSize = 1024

	return RunParallel("fill", nThreads, nKeys, func(workerID int, rng *rand.Rand, db *yogadb.FlexDB, ops int64) int64 {
		chunk := nKeys / int64(nThreads)
		start := int64(workerID) * chunk
		end := start + ops
		keyBuf := make([]byte, klen)
		success := int64(0)

		batch := db.NewBatch()
		defer batch.Close()

		for i := start; i < end; i++ {
			k := string(hexKeyBuf(keyBuf, uint64(i), klen))
			batch.Set(k, val)
			success++

			if (i-start+1)%batchCommitSize == 0 || i == end-1 {
				batch.Commit(false)
				batch.Reset()
			}
		}
		return success
	}, db)
}

// parallelFillBatch does a sequential parallel fill using the Batch API.
// This is much faster than individual Put calls for large fills.
func parallelFillBatch(db *yogadb.FlexDB, nKeys int64, nThreads int, klen, vlen int) BenchResult {
	val := makeValue(vlen)
	const batchSize = 1024

	return RunParallel("fill-batch", nThreads, nKeys, func(workerID int, rng *rand.Rand, db *yogadb.FlexDB, ops int64) int64 {
		chunk := nKeys / int64(nThreads)
		start := int64(workerID) * chunk
		end := start + ops
		keyBuf := make([]byte, klen)
		success := int64(0)

		batch := db.NewBatch()
		defer batch.Close()

		for i := start; i < end; i++ {
			k := string(hexKeyBuf(keyBuf, uint64(i), klen))
			batch.Set(k, val)
			success++

			if (i-start+1)%batchSize == 0 || i == end-1 {
				batch.Commit(false) // no fsync per batch
				batch.Reset()
			}
		}
		return success
	}, db)
}

// printHeader prints a header for benchmark output.
func printHeader(benchName string, cf *CommonFlags) {
	fmt.Printf("=== %s === dataset=%s klen=%d vlen=%d threads=%d count=%d dist=%s\n",
		benchName, cf.Profile.Name, cf.Profile.KeyLen, cf.Profile.ValLen,
		cf.Threads, cf.Count, cf.Dist)
}
