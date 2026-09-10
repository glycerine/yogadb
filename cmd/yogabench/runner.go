package main

// runner.go — Benchmark runner: manages goroutines, timing, and reporting.
// Matches the C "forker" framework's output format for comparison.

import (
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
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
	vv("openDB dir='%v'", dir)

	cfg := &yogadb.Config{
		CacheMB:         cf.CacheMB,
		NoDisk:          cf.NoDisk,
		OmitMemWalFsync: cf.OmitWALSync,

		//	DisableVLOG bool
		//	OmitFlexSpaceOpsRedoLog bool
		//	LowBlockUtilizationPct float64
		//	OmitMemWalFsync bool
		//	PiggybackGC_on_SyncOrFlush bool
		//	GCGarbagePct float64
	}
	return yogadb.OpenFlexDB(dir, cfg)
}

// closeDB closes the database and prints final metrics.
func closeDB(db *yogadb.FlexDB) {
	m := db.Close()
	if m != nil {
		fmt.Fprintf(os.Stderr, "metrics: logical_written=%d physical_written=%d live=%d free_in_blocks=%d blocks=%d write_amp=%.2f fsyncs=%d\n",
			m.LogicalBytesWritten, m.TotalBytesWritten,
			m.KVBlocksTotalLiveBytes, m.TotalFreeBytesInBlocks, m.BlocksInUse, m.WriteAmp, m.TotalFsyncs)
	}
}

func mustFillAndAllowReads(db *yogadb.FlexDB, cf *CommonFlags, nThreads int, p DatasetProfile) BenchResult {
	result, err := fillAndAllowReads(db, cf, nThreads, p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fill: %v\n", err)
		os.Exit(1)
	}
	return result
}

func fillAndAllowReads(db *yogadb.FlexDB, cf *CommonFlags, nThreads int, p DatasetProfile) (BenchResult, error) {
	if !cf.CountExplicit && !cf.NoDisk && cf.FillTargetBytes > 0 {
		return parallelFillUntilDirSizeAndAllowReads(db, cf, nThreads, p)
	}

	result := parallelFill(db, cf.Count, nThreads, p.KeyLen, p.ValLen)
	db.AllowReads()
	cf.Count = result.TotalOps
	return result, nil
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
			batch.Set(k, val, 0)
			success++

			if (i-start+1)%batchCommitSize == 0 || i == end-1 {
				batch.Commit(false)
				batch.Reset()
			}
		}
		return success
	}, db)
}

const (
	fillBatchCommitSize       = 1024
	fillTargetSlottedPageSize = 4 << 10
	fillTargetPageOverhead    = 27 + 4
	fillTargetInlineThreshold = 64
	fillTargetVPtrSize        = 16
)

func parallelFillUntilDirSizeAndAllowReads(db *yogadb.FlexDB, cf *CommonFlags, nThreads int, p DatasetProfile) (BenchResult, error) {
	if nThreads < 1 {
		nThreads = 1
	}
	runtime.GC()
	startTime := time.Now()

	ops, err := parallelFillUntilProjectedDirSize(db, cf.Dir, cf.FillTargetBytes, nThreads, p.KeyLen, p.ValLen)
	if err != nil {
		return BenchResult{}, err
	}

	db.AllowReads()

	finalBytes, err := dirSizeBytes(cf.Dir)
	if err != nil {
		return BenchResult{}, err
	}
	for finalBytes < cf.FillTargetBytes {
		remaining := cf.FillTargetBytes - finalBytes
		perKey := estimatedFillKVBytes(1, p.KeyLen, p.ValLen)
		n := int64(fillBatchCommitSize)
		if perKey > 0 {
			needed := remaining/perKey + 1
			if needed < n {
				n = needed
			}
		}
		if n < 1 {
			n = 1
		}
		if err := fillKeyRange(db, ops, n, p.KeyLen, p.ValLen); err != nil {
			return BenchResult{}, err
		}
		ops += n
		if err := db.Sync(); err != nil {
			return BenchResult{}, err
		}
		finalBytes, err = dirSizeBytes(cf.Dir)
		if err != nil {
			return BenchResult{}, err
		}
	}

	elapsed := time.Since(startTime)
	result := BenchResult{
		Label:    "fill",
		TotalOps: ops,
		Duration: elapsed,
		Mops:     float64(ops) / elapsed.Seconds() / 1e6,
		Success:  ops,
	}
	fmt.Println(result)
	fmt.Printf("fill-target: requested_gb=%g target_bytes=%d final_bytes=%d objects_created=%d repeat_with=\"-count %d\"\n",
		cf.GB, cf.FillTargetBytes, finalBytes, ops, ops)
	cf.Count = ops
	return result, nil
}

func parallelFillUntilProjectedDirSize(db *yogadb.FlexDB, dir string, targetBytes int64, nThreads int, klen, vlen int) (int64, error) {
	initialBytes, err := dirSizeBytes(dir)
	if err != nil {
		return 0, err
	}
	if initialBytes >= targetBytes {
		targetBytes = initialBytes + 1
	}

	val := makeValue(vlen)
	var nextKey atomic.Int64
	var committed atomic.Int64
	var pendingKVBytes atomic.Int64
	var stop atomic.Bool
	var checkMu sync.Mutex
	var errMu sync.Mutex
	var firstErr error

	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
		stop.Store(true)
	}

	var wg sync.WaitGroup
	for i := 0; i < nThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			keyBuf := make([]byte, klen)
			batch := db.NewBatch()
			defer batch.Close()

			for !stop.Load() {
				startKey := nextKey.Add(fillBatchCommitSize) - fillBatchCommitSize
				for j := int64(0); j < fillBatchCommitSize; j++ {
					k := string(hexKeyBuf(keyBuf, uint64(startKey+j), klen))
					if err := batch.Set(k, val, 0); err != nil {
						recordErr(err)
						return
					}
				}
				if _, err := batch.Commit(false); err != nil {
					recordErr(err)
					return
				}
				batch.Reset()
				committed.Add(fillBatchCommitSize)
				pendingKVBytes.Add(estimatedFillKVBytes(fillBatchCommitSize, klen, vlen))

				checkMu.Lock()
				if !stop.Load() {
					diskBytes, err := dirSizeBytes(dir)
					if err != nil {
						recordErr(err)
					} else if diskBytes+pendingKVBytes.Load() >= targetBytes {
						stop.Store(true)
					}
				}
				checkMu.Unlock()
			}
		}()
	}
	wg.Wait()

	errMu.Lock()
	defer errMu.Unlock()
	return committed.Load(), firstErr
}

func fillKeyRange(db *yogadb.FlexDB, startKey, nKeys int64, klen, vlen int) error {
	val := makeValue(vlen)
	keyBuf := make([]byte, klen)
	batch := db.NewBatch()
	defer batch.Close()

	for i := int64(0); i < nKeys; i++ {
		k := string(hexKeyBuf(keyBuf, uint64(startKey+i), klen))
		if err := batch.Set(k, val, 0); err != nil {
			return err
		}
		if (i+1)%fillBatchCommitSize == 0 || i == nKeys-1 {
			if _, err := batch.Commit(false); err != nil {
				return err
			}
			batch.Reset()
		}
	}
	return nil
}

func estimatedFillKVBytes(nKeys int64, klen, vlen int) int64 {
	if nKeys <= 0 {
		return 0
	}
	valueBytes := vlen
	if vlen > fillTargetInlineThreshold {
		valueBytes = fillTargetVPtrSize
	}
	entryBytes := int64(4 + 1 + klen + valueBytes)
	if entryBytes < 1 {
		entryBytes = 1
	}
	pagePayload := int64(fillTargetSlottedPageSize - fillTargetPageOverhead)
	keysPerPage := pagePayload / entryBytes
	if keysPerPage < 1 {
		keysPerPage = 1
	}
	pages := (nKeys + keysPerPage - 1) / keysPerPage
	return nKeys*entryBytes + pages*fillTargetPageOverhead
}

func dirSizeBytes(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
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
			batch.Set(k, val, 0)
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
	countLabel := "count_estimate"
	if cf.CountExplicit {
		countLabel = "count"
	}
	fmt.Printf("=== %s === dataset=%s klen=%d vlen=%d threads=%d %s=%d gb=%g target_bytes=%d dist=%s\n",
		benchName, cf.Profile.Name, cf.Profile.KeyLen, cf.Profile.ValLen,
		cf.Threads, countLabel, cf.Count, cf.GB, cf.FillTargetBytes, cf.Dist)
}
