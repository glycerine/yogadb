package yogadb

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/glycerine/vfs"
)

// BenchmarkDeleteRange measures DeleteRange throughput after batch-loading N keys.
// Currently in memory while we tune to avoid fsync wear and tear.
func BenchmarkDeleteRange(b *testing.B) {
	for _, totalKeys := range []int{10_000} { // 1_000, 10_000} {
		for _, deletePercent := range []int{10} { // , 50, 100} {
			b.Run(fmt.Sprintf("keys=%d/del=%d%%", totalKeys, deletePercent), func(b *testing.B) {
				// Use in-memory VFS to avoid fsync overhead in setup.
				cfg := &Config{NoDisk: true}
				db, err := OpenFlexDB("bench", cfg)
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()

				// Compute range to delete.
				delCount := totalKeys * deletePercent / 100
				start := (totalKeys - delCount) / 2
				end := start + delCount - 1
				startKey := []byte(fmt.Sprintf("k%08d", start))
				endKey := []byte(fmt.Sprintf("k%08d", end))

				// Pre-generate keys and values.
				keys := make([][]byte, totalKeys)
				vals := make([][]byte, totalKeys)
				for k := 0; k < totalKeys; k++ {
					keys[k] = []byte(fmt.Sprintf("k%08d", k))
					vals[k] = []byte(fmt.Sprintf("v%08d", k))
				}

				// Each iteration: insert all keys, then DeleteRange.
				// We measure the whole cycle since StopTimer/StartTimer
				// causes b.N to ramp excessively when setup >> measured work.
				for i := 0; i < b.N; i++ {
					batch := db.NewBatch()
					for k := 0; k < totalKeys; k++ {
						batch.Set(keys[k], vals[k])
					}
					batch.Commit(false)

					n, _, err := db.DeleteRange(true, startKey, endKey, true, true)
					if err != nil {
						b.Fatal(err)
					}
					if i == 0 {
						b.ReportMetric(float64(n), "tombstones")
					}
				}
			})
		}
	}
}

// BenchmarkBatchPutDeleteCycle measures write path under put+delete workload.
func BenchmarkBatchPutDeleteCycle(b *testing.B) {
	for _, batchSize := range []int{1_000} { // , 10_000} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			dir := b.TempDir()
			cfg := &Config{FS: vfs.Default}
			db, err := OpenFlexDB(dir, cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			b.ResetTimer()
			var totalTombstones int64
			for i := 0; i < b.N; i++ {
				// Batch put
				batch := db.NewBatch()
				for k := 0; k < batchSize; k++ {
					key := fmt.Sprintf("c%08d_%08d", i, k)
					val := fmt.Sprintf("v%08d", k)
					batch.Set([]byte(key), []byte(val))
				}
				if _, err := batch.Commit(false); err != nil {
					b.Fatal(err)
				}

				// Delete first half
				startKey := fmt.Sprintf("c%08d_%08d", i, 0)
				endKey := fmt.Sprintf("c%08d_%08d", i, batchSize/2-1)
				n, _, err := db.DeleteRange(true, []byte(startKey), []byte(endKey), true, true)
				if err != nil {
					b.Fatal(err)
				}
				totalTombstones += n
			}
			b.StopTimer()

			diskBytes, _ := dirSize(vfs.Default, dir)
			b.ReportMetric(float64(diskBytes), "disk_bytes")
			b.ReportMetric(float64(totalTombstones)/float64(b.N), "tombstones/op")
		})
	}
}

// eagerFsync is out here for consistency of benchmarking
// BigRandomRWBatch for pebble and yogadb; so we only
// need to change it in one place to change both.
// eagerFsync true  => fsync at the end of each batch, at batch.Commit().
// eagerFsync false => just one fsync at the end, after all batches.
const eagerFsync = false

/* Just writes:
eagerFsync = false:

go test -v -run=xxx -bench BigRandomRWBatch
goos: darwin
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
BenchmarkYogaDB_BigRandomRWBatch
BenchmarkYogaDB_BigRandomRWBatch/batch_1000/redo=0/write=1_
BenchmarkYogaDB_BigRandomRWBatch/batch_1000/redo=0/write=1_-8         	  313790	      3427 ns/op	      1000 batch_size	  41277492 disk_bytes	  17572240 logical_bytes	      3427 our_PUT_ns/op	         2.349 write_amp	 103005932 yoga_total_physical_bytes_written	         5.862 yoga_write_amp
BenchmarkYogaDB_BigRandomRWBatch/batch_1000/redo=1/write=1_
BenchmarkYogaDB_BigRandomRWBatch/batch_1000/redo=1/write=1_-8         	  369402	      2955 ns/op	      1000 batch_size	  48995128 disk_bytes	  20686512 logical_bytes	      2955 our_PUT_ns/op	         2.368 write_amp	 121808896 yoga_total_physical_bytes_written	         5.888 yoga_write_amp
BenchmarkPebble_BigRandomRWBatch
BenchmarkPebble_BigRandomRWBatch/batch_1000_write=1
2026/03/05 20:42:57 Found 0 WALs
2026/03/05 20:42:57 Found 0 WALs
2026/03/05 20:42:57 Found 0 WALs
2026/03/05 20:42:58 Found 0 WALs
2026/03/05 20:42:58 Found 0 WALs
2026/03/05 20:42:59 Found 0 WALs
2026/03/05 20:42:59 Found 0 WALs
BenchmarkPebble_BigRandomRWBatch/batch_1000_write=1-8                 	  168066	      6011 ns/op	      1000 batch_size	  14579319 disk_bytes	   9411696 logical_bytes	      6011 our_PUT_ns/op	         1.926 pebble_write_amp	         1.549 write_amp
PASS
ok  	github.com/glycerine/yogadb	12.551s

Compilation finished at Thu Mar  5 17:43:01

----------
If we fsync after each batch of 1000, then the fsync time dominates
and the write advantage of yogadb vs pebble mostly evaporates.

eagerFsync = true:

go test -v -run=xxx -bench BigRandomRWBatch
goos: darwin
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
BenchmarkYogaDB_BigRandomRWBatch
BenchmarkYogaDB_BigRandomRWBatch/batch_1000/redo=0/write=1_
BenchmarkYogaDB_BigRandomRWBatch/batch_1000/redo=0/write=1_-8         	   45889	     25761 ns/op	      1000 batch_size	   3539942 disk_bytes	   2569784 logical_bytes	     25761 our_PUT_ns/op	         1.378 write_amp	   3213394 yoga_total_physical_bytes_written	         1.250 yoga_write_amp
BenchmarkYogaDB_BigRandomRWBatch/batch_1000/redo=1/write=1_
BenchmarkYogaDB_BigRandomRWBatch/batch_1000/redo=1/write=1_-8         	   47041	     25860 ns/op	      1000 batch_size	   3620582 disk_bytes	   2634296 logical_bytes	     25860 our_PUT_ns/op	         1.374 write_amp	   3294034 yoga_total_physical_bytes_written	         1.250 yoga_write_amp
BenchmarkPebble_BigRandomRWBatch
BenchmarkPebble_BigRandomRWBatch/batch_1000_write=1
2026/03/05 20:44:01 Found 0 WALs
2026/03/05 20:44:02 Found 0 WALs
2026/03/05 20:44:02 Found 0 WALs
2026/03/05 20:44:02 Found 0 WALs
2026/03/05 20:44:03 Found 0 WALs
BenchmarkPebble_BigRandomRWBatch/batch_1000_write=1-8                 	   43288	     27480 ns/op	      1000 batch_size	   2861809 disk_bytes	   2424128 logical_bytes	     27480 our_PUT_ns/op	         1.000 pebble_write_amp	         1.181 write_amp
PASS
ok  	github.com/glycerine/yogadb	10.164s

*/

// compare with FLEXSPACE.REDO.LOG on or off. benchmark puts(random writes) and gets(random reads)
func BenchmarkYogaDB_BigRandomRWBatch(b *testing.B) {

	for write := 0; write < 2; write++ {
		if write != 1 {
			continue
		}
		for redo := 0; redo < 2; redo++ {
			cfg := &Config{}
			if redo == 0 {
				cfg.OmitFlexSpaceOpsRedoLog = true
			}

			for _, batchSize := range []int{1_000} { // 1000, 100_000, 10_000_000} {

				b.Run(fmt.Sprintf("batch_%d/redo=%v/write=%v\n", batchSize, redo, write), func(b *testing.B) {

					dir, err := os.MkdirTemp("", "BenchmarkYogaDB_BigRandomRWBatch*")
					panicOn(err)
					defer os.Remove(dir)

					db, err := OpenFlexDB(dir, cfg)
					if err != nil {
						b.Fatalf("OpenFlexDB: %v", err)
					}
					defer db.Close()

					//value := newValue(123)
					//rng := rand.New(rand.NewSource(testBenchSeed))

					var logicalBytes int64

					var seed [32]byte
					prng := newPRNG(seed)
					// Pre-generate all keys to avoid measuring key generation.
					totalOps := b.N // * batchSize
					keys := make([][]byte, totalOps)
					dup := make(map[string]bool)
					for i := range keys {
						// only write unique keys for now.
						var cid string
						for {
							cid = prng.NewCallID()
							if dup[cid] {
								continue
							}
							dup[cid] = true
							break
						}
						keys[i] = []byte(cid)
						logicalBytes += int64(len(cid) * 2)
					}

					b.ResetTimer()
					t0 := time.Now()
					ki := 0
					batch := db.NewBatch()
					needCommit := false
					for i := 0; i < b.N; i++ {

						if i%batchSize == 0 && i > 0 {
							batch.Commit(eagerFsync)
							needCommit = false
						}
						batch.Set(keys[ki], keys[ki])
						ki++
						needCommit = true
					}
					if needCommit {
						batch.Commit(eagerFsync)
					}
					if write == 0 {
						// above was the write benchmark, but we want to benchmark reading back actually
						// read benchmark:
						b.ResetTimer()
						t0 = time.Now()
						for cid := range dup {
							val, found := db.Get([]byte(cid))
							if !found {
								// we might not write all now that b.N controls how many we write.
								//b.Fatalf("why not found '%v'", cid)
							} else {
								sval := string(val)
								if sval != cid {
									b.Fatalf("why val not match key? val='%v'; key='%v'", sval, cid)
								}
							}
						}
					}

					if !eagerFsync {
						db.Sync() // flush memtable to disk for accurate size measurement + safety.
					}
					elap := time.Since(t0)
					ourRate := float64(int64(elap)) / float64(totalOps)
					b.StopTimer()

					m := db.SessionMetrics()

					//m := db.Close()

					diskBytes := mustDirSize(vfs.Default, dir)
					wa := float64(diskBytes) / float64(logicalBytes)

					rateName := "our_GET_ns/op"
					if write == 1 {
						rateName = "our_PUT_ns/op"
					}
					b.ReportMetric(ourRate, rateName)
					b.ReportMetric(float64(logicalBytes), "logical_bytes")
					b.ReportMetric(float64(diskBytes), "disk_bytes")
					b.ReportMetric(wa, "write_amp")
					b.ReportMetric(float64(batchSize), "batch_size")
					b.ReportMetric(m.WriteAmp, "yoga_write_amp")
					b.ReportMetric(float64(m.TotalBytesWritten), "yoga_total_physical_bytes_written")
				})
			}
		}
	}
}

// BenchmarkYogaDB_Put measures individual Put throughput for comparison.
func BenchmarkYogaDB_Put(b *testing.B) {
	dir := b.TempDir()
	//vv("temp dir = '%v'", dir)

	db, err := OpenFlexDB(dir, nil)
	if err != nil {
		b.Fatalf("OpenFlexDB: %v", err)
	}
	defer db.Close()

	value := newValue(123)
	rng := rand.New(rand.NewSource(testBenchSeed))

	keys := make([][]byte, b.N)
	for i := range keys {
		k := make([]byte, 8)
		binary.LittleEndian.PutUint32(k[0:4], rng.Uint32())
		binary.LittleEndian.PutUint32(k[4:8], rng.Uint32())
		keys[i] = k
	}
	logicalBytes := int64(b.N) * int64(8+len(value))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Put(keys[i], value)
	}
	b.StopTimer()

	db.Sync()

	m := db.SessionMetrics()
	diskBytes := mustDirSize(vfs.Default, dir)
	wa := float64(diskBytes) / float64(logicalBytes)

	b.ReportMetric(float64(logicalBytes), "logical_bytes")
	b.ReportMetric(float64(diskBytes), "disk_bytes")
	b.ReportMetric(wa, "write_amp")
	b.ReportMetric(m.WriteAmp, "yoga_write_amp")
}

// BenchmarkBatchPebble measures Pebble batch-write throughput for comparison.
func BenchmarkBatchPebble(b *testing.B) {
	for _, batchSize := range []int{1_000} { // 1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			dir := b.TempDir()
			db, err := pebble.Open(dir, &pebble.Options{})
			if err != nil {
				b.Fatalf("pebble.Open: %v", err)
			}
			defer db.Close()

			value := newValue(123)
			rng := rand.New(rand.NewSource(testBenchSeed))

			totalOps := b.N * batchSize
			keys := make([][]byte, totalOps)
			for i := range keys {
				k := make([]byte, 8)
				binary.LittleEndian.PutUint32(k[0:4], rng.Uint32())
				binary.LittleEndian.PutUint32(k[4:8], rng.Uint32())
				keys[i] = k
			}
			logicalBytes := int64(0)
			for _, k := range keys {
				logicalBytes += int64(len(k) + len(value))
			}

			b.ResetTimer()
			ki := 0
			/*
				for i := 0; i < b.N; i++ {
					batch := db.NewBatch()
					for j := 0; j < batchSize; j++ {
						batch.Set(keys[ki], value, nil)
						ki++
					}
					batch.Commit(pebble.NoSync)
					batch.Close()
				}
			*/
			batch := db.NewBatch()
			for i := 0; i < b.N; i++ {

				if i%batchSize == 0 && i > 0 {
					_ = batch.Commit(pebble.Sync)
					batch = db.NewBatch()
				}
				batch.Set(keys[ki], keys[ki], pebble.NoSync)
				ki++
			}
			batch.Commit(pebble.Sync)

			b.StopTimer()

			db.Flush()

			metrics := db.Metrics()
			levelMetrics := metrics.Total()

			diskBytes := mustDirSize(vfs.Default, dir)
			wa := float64(diskBytes) / float64(logicalBytes)

			b.ReportMetric(float64(logicalBytes), "logical_bytes")
			b.ReportMetric(float64(diskBytes), "disk_bytes")
			b.ReportMetric(wa, "write_amp")
			b.ReportMetric(float64(batchSize), "batch_size")
			b.ReportMetric(levelMetrics.WriteAmp(), "pebble_write_amp")
		})
	}
}

// BenchmarkPutPebble measures individual Pebble Put throughput for comparison.
func BenchmarkPutPebble(b *testing.B) {
	dir := b.TempDir()
	vv("temp dir = '%v'", dir)

	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		b.Fatalf("pebble.Open: %v", err)
	}
	defer db.Close()

	value := newValue(123)
	rng := rand.New(rand.NewSource(testBenchSeed))

	keys := make([][]byte, b.N)
	for i := range keys {
		k := make([]byte, 8)
		binary.LittleEndian.PutUint32(k[0:4], rng.Uint32())
		binary.LittleEndian.PutUint32(k[4:8], rng.Uint32())
		keys[i] = k
	}
	logicalBytes := int64(b.N) * int64(8+len(value))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Set(keys[i], value, pebble.NoSync)
	}
	b.StopTimer()

	db.Flush()

	diskBytes := mustDirSize(vfs.Default, dir)
	wa := float64(diskBytes) / float64(logicalBytes)

	b.ReportMetric(float64(logicalBytes), "logical_bytes")
	b.ReportMetric(float64(diskBytes), "disk_bytes")
	b.ReportMetric(wa, "write_amp")
}

/* yoga_write_amp (accurate measurement) shows write amplifiation of 3.7x currently.

-*- mode: compilation; default-directory: "~/yogadb/" -*-
Compilation started at Mon Mar  2 17:00:55

go test -v -run=xxxx -bench=BenchmarkBatchYogaDB
goos: darwin
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
BenchmarkBatchYogaDB
BenchmarkBatchYogaDB/batch_1
BenchmarkBatchYogaDB/batch_1-8         	 1000000	      1454 ns/op	         1.000 batch_size	  35796779 disk_bytes	  13000000 logical_bytes	         2.754 write_amp	  48000561 yoga_total_physical_bytes_written	         3.692 yoga_write_amp
BenchmarkBatchYogaDB/batch_10
BenchmarkBatchYogaDB/batch_10-8        	  160545	     14439 ns/op	        10.00 batch_size	  57174723 disk_bytes	  20870850 logical_bytes	         2.739 write_amp	  77062157 yoga_total_physical_bytes_written	         3.692 yoga_write_amp
BenchmarkBatchYogaDB/batch_100
BenchmarkBatchYogaDB/batch_100-8       	   10000	    121207 ns/op	       100.0 batch_size	  35796779 disk_bytes	  13000000 logical_bytes	         2.754 write_amp	  48000561 yoga_total_physical_bytes_written	         3.692 yoga_write_amp
BenchmarkBatchYogaDB/batch_1000
BenchmarkBatchYogaDB/batch_1000-8      	    1945	   1374598 ns/op	      1000 batch_size	  69777975 disk_bytes	  25285000 logical_bytes	         2.760 write_amp	  93360561 yoga_total_physical_bytes_written	         3.692 yoga_write_amp
BenchmarkBatchYogaDB/batch_10000
BenchmarkBatchYogaDB/batch_10000-8     	     100	  10880392 ns/op	     10000 batch_size	  35796779 disk_bytes	  13000000 logical_bytes	         2.754 write_amp	  48000561 yoga_total_physical_bytes_written	         3.692 yoga_write_amp
PASS
ok  	github.com/glycerine/yogadb	17.606s

Compilation finished at Mon Mar  2 17:01:16
*/

// BenchmarkYogaDB_Batch measures batch-write throughput for YogaDB.
// It reports bytes written per operation and write amplification.
func BenchmarkYogaDB_Batch(b *testing.B) {

	for redo := 0; redo < 2; redo++ {
		cfg := &Config{}
		if redo == 0 {
			cfg.OmitFlexSpaceOpsRedoLog = true
		}

		for _, batchSize := range []int{1, 10, 100, 1000, 10000} {

			b.Run(fmt.Sprintf("batch_%d_redo=%v", batchSize, redo), func(b *testing.B) {

				//dir := b.TempDir() // racy, probably buggy.
				//dir, fs := newTestFS(t) no t available!
				dir, err := os.MkdirTemp("", "BenchmarkYogaDB_Batch*")
				panicOn(err)
				defer os.Remove(dir)

				db, err := OpenFlexDB(dir, cfg)
				if err != nil {
					b.Fatalf("OpenFlexDB: %v", err)
				}
				defer db.Close()

				value := newValue(123)
				rng := rand.New(rand.NewSource(testBenchSeed))

				// Pre-generate all keys to avoid measuring key generation.
				totalOps := b.N * batchSize
				keys := make([][]byte, totalOps)
				for i := range keys {
					k := make([]byte, 8)
					binary.LittleEndian.PutUint32(k[0:4], rng.Uint32())
					binary.LittleEndian.PutUint32(k[4:8], rng.Uint32())
					keys[i] = k
				}
				logicalBytes := int64(0)
				for _, k := range keys {
					logicalBytes += int64(len(k) + len(value))
				}

				b.ResetTimer()
				ki := 0
				for i := 0; i < b.N; i++ {
					batch := db.NewBatch()
					for j := 0; j < batchSize; j++ {
						batch.Set(keys[ki], value)
						ki++
					}
					batch.Commit(true)
					batch.Close()
				}
				b.StopTimer()

				db.Sync() // flush memtable to disk for accurate size measurement

				m := db.SessionMetrics()
				diskBytes := mustDirSize(vfs.Default, dir)
				wa := float64(diskBytes) / float64(logicalBytes)

				b.ReportMetric(float64(logicalBytes), "logical_bytes")
				b.ReportMetric(float64(diskBytes), "disk_bytes")
				b.ReportMetric(wa, "write_amp")
				b.ReportMetric(float64(batchSize), "batch_size")
				b.ReportMetric(m.WriteAmp, "yoga_write_amp")
				b.ReportMetric(float64(m.TotalBytesWritten), "yoga_total_physical_bytes_written")
			})
		}
	}
}

// pebble version, random writes, random reads.

func BenchmarkPebble_BigRandomRWBatch(b *testing.B) {

	for write := 0; write < 2; write++ {
		if write != 1 {
			continue
		}
		for _, batchSize := range []int{1_000} { // 1000, 100_000, 10_000_000} {

			b.Run(fmt.Sprintf("batch_%d_write=%v", batchSize, write), func(b *testing.B) {

				//dir := b.TempDir() // racy, probably buggy.

				dir, err := os.MkdirTemp("", "BenchmarkPebble_BigRandomRWBatch*")
				panicOn(err)
				defer os.Remove(dir)

				//vv("From b.TempDir(): dir = '%v'", dir)
				db, err := pebble.Open(dir, &pebble.Options{})
				if err != nil {
					b.Fatalf("pebble.Open: %v", err)
				}
				defer db.Close()

				var logicalBytes int64

				var seed [32]byte
				prng := newPRNG(seed)
				// Pre-generate all keys to avoid measuring key generation.
				totalOps := b.N //* batchSize
				keys := make([][]byte, totalOps)
				dup := make(map[string]bool)
				for i := range keys {
					// only write unique keys for now.
					var cid string
					for {
						cid = prng.NewCallID()
						if dup[cid] {
							continue
						}
						dup[cid] = true
						break
					}
					keys[i] = []byte(cid)
					logicalBytes += int64(len(cid) * 2)
				}

				b.ResetTimer()
				t0 := time.Now()
				ki := 0

				/*				for i := 0; i < b.N; i++ {
									batch := db.NewBatch()
									for j := 0; j < batchSize; j++ {
										batch.Set(keys[ki], keys[ki], nil)
										ki++
									}
									batch.Commit(pebble.NoSync)
									batch.Close()
								}
				*/
				batch := db.NewBatch()
				for i := 0; i < b.N; i++ {

					if i%batchSize == 0 && i > 0 {
						if eagerFsync {
							batch.Commit(pebble.Sync)
						} else {
							batch.Commit(pebble.NoSync)
						}
						batch = db.NewBatch()
					}
					batch.Set(keys[ki], keys[ki], pebble.NoSync)
					ki++
				}

				// Pebble's Batch.Commit(pebble.Sync) does wait for fdatasync
				// before returning. Here's the condensed path:
				//
				// 1. Batch.Commit(opts) → db.Apply() → db.applyInternal(sync=true, noSyncWait=false)
				// 2. commitPipeline.Commit() calls prepare() which creates a sync.WaitGroup
				//    with count 2 (one for publish, one for WAL sync)
				// 3. The batch is written to the WAL via LogWriter.SyncRecord(), which
				//    enqueues the WaitGroup into a sync queue and signals a background
				//    flushLoop goroutine
				// 4. flushLoop calls flushPending() → syncWithLatency() → w.s.Sync() which is:
				//    - Linux: unix.Fdatasync(fd) (vfs/default_linux.go:74)
				//    - macOS: os.File.Sync() → fsync (vfs/default_unix.go:43)
				// 5. After fdatasync returns, syncQueue.pop() calls wg.Done() on the batch's WaitGroup
				// 6. Back in the commit pipeline, publish() calls b.commit.Wait(), which
				//    blocks until both Done() calls fire (publish + WAL sync)
				// 7. Only then does Commit() return
				//
				// So the internal mechanism is async (a dedicated flush goroutine batches
				// multiple syncs), but from the caller's perspective it's synchronous --
				// Commit(Sync) blocks until fdatasync completes. Similarly, yogadb Batch.Commit(true)
				// now has the same durability guarantee. So we are apples-to-apples.
				if eagerFsync {
					batch.Commit(pebble.Sync)
				} else {
					batch.Commit(pebble.NoSync)
				}

				if write == 0 {
					// above was the write benchmark, but we want to benchmark reading back actually
					// read benchmark:
					b.ResetTimer()
					t0 = time.Now()
					for cid := range dup {
						val, closer, err := db.Get([]byte(cid))
						if err == nil {
							//panicOn(err)
							sval := string(val)
							if sval != cid {
								b.Fatalf("why val not match key? val='%v'; key='%v'", sval, cid)
							}
						}
						closer.Close()
					}
				}

				if eagerFsync {
					// no final fsync needed.
				} else {
					db.Flush()
				}

				elap := time.Since(t0)
				ourRate := float64(int64(elap)) / float64(totalOps)
				b.StopTimer()

				//db.Sync() // flush memtable to disk for accurate size measurement
				//m := db.SessionMetrics()

				metrics := db.Metrics()
				levelMetrics := metrics.Total()

				//vv("dir is now '%v'", dir)
				diskBytes, err := dirSize(vfs.Default, dir)
				var wa float64
				if err != nil {
					// already gone, maybe the benching stopped us early?? after b.StopTimer() above!?!
					wa = math.NaN()
				} else {
					wa = float64(diskBytes) / float64(logicalBytes)
				}
				rateName := "our_GET_ns/op"
				if write == 1 {
					rateName = "our_PUT_ns/op"
				}
				b.ReportMetric(ourRate, rateName)

				b.ReportMetric(float64(logicalBytes), "logical_bytes")
				b.ReportMetric(float64(diskBytes), "disk_bytes")
				b.ReportMetric(wa, "write_amp")
				b.ReportMetric(float64(batchSize), "batch_size")
				b.ReportMetric(levelMetrics.WriteAmp(), "pebble_write_amp")

			})
		}
	}
}
