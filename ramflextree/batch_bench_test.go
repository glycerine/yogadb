package ramflextree

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

const testBenchSeed = 1

func newValue(v int) []byte {
	return []byte(fmt.Sprintf("%05d", v))
}

// BenchmarkDeleteRange measures DeleteRange throughput after batch-loading N keys.
func BenchmarkDeleteRange(b *testing.B) {
	for _, totalKeys := range []int{10_000} {
		for _, deletePercent := range []int{10} {
			b.Run(fmt.Sprintf("keys=%d/del=%d%%", totalKeys, deletePercent), func(b *testing.B) {
				cfg := &Config{}
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
				for i := 0; i < b.N; i++ {
					batch := db.NewBatch()
					for k := 0; k < totalKeys; k++ {
						batch.Set(string(keys[k]), vals[k])
					}
					batch.Commit(false)

					n, _, err := db.DeleteRange(true, string(startKey), string(endKey), true, true)
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
	for _, batchSize := range []int{1_000} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			cfg := &Config{}
			db, err := OpenFlexDB("", cfg)
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
					batch.Set(key, []byte(val))
				}
				if _, err := batch.Commit(false); err != nil {
					b.Fatal(err)
				}

				// Delete first half
				startKey := fmt.Sprintf("c%08d_%08d", i, 0)
				endKey := fmt.Sprintf("c%08d_%08d", i, batchSize/2-1)
				n, _, err := db.DeleteRange(true, startKey, endKey, true, true)
				if err != nil {
					b.Fatal(err)
				}
				totalTombstones += n
			}
			b.StopTimer()

			b.ReportMetric(float64(totalTombstones)/float64(b.N), "tombstones/op")
		})
	}
}

// BenchmarkYogaDB_BigRandomRWBatch benchmarks puts (random writes) and gets (random reads).
func BenchmarkYogaDB_BigRandomRWBatch(b *testing.B) {

	for write := 0; write < 2; write++ {
		if write != 1 {
			continue
		}

		cfg := &Config{}

		for _, batchSize := range []int{1_000} {

			b.Run(fmt.Sprintf("batch_%d/write=%v\n", batchSize, write), func(b *testing.B) {

				db, err := OpenFlexDB("", cfg)
				if err != nil {
					b.Fatalf("OpenFlexDB: %v", err)
				}
				defer db.Close()

				var logicalBytes int64

				var seed [32]byte
				prng := newPRNG(seed)
				// Pre-generate all keys to avoid measuring key generation.
				totalOps := b.N
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
						batch.Commit(false)
						needCommit = false
					}
					batch.Set(string(keys[ki]), keys[ki])
					ki++
					needCommit = true
				}
				if needCommit {
					batch.Commit(false)
				}
				if write == 0 {
					// read benchmark
					b.ResetTimer()
					t0 = time.Now()
					for cid := range dup {
						val, found, _ := db.Get(cid)
						if found {
							sval := string(val)
							if sval != cid {
								b.Fatalf("why val not match key? val='%v'; key='%v'", sval, cid)
							}
						}
					}
				}

				db.Sync()
				elap := time.Since(t0)
				ourRate := float64(int64(elap)) / float64(totalOps)
				b.StopTimer()

				m := db.SessionMetrics()

				rateName := "our_GET_ns/op"
				if write == 1 {
					rateName = "our_PUT_ns/op"
				}
				b.ReportMetric(ourRate, rateName)
				b.ReportMetric(float64(logicalBytes), "logical_bytes")
				b.ReportMetric(float64(batchSize), "batch_size")
				b.ReportMetric(m.WriteAmp, "yoga_write_amp")
				b.ReportMetric(float64(m.TotalBytesWritten), "yoga_total_physical_bytes_written")
			})
		}
	}
}

// BenchmarkYogaDB_Put measures individual Put throughput.
func BenchmarkYogaDB_Put(b *testing.B) {
	db, err := OpenFlexDB("", nil)
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
		db.Put(string(keys[i]), value)
	}
	b.StopTimer()

	db.Sync()

	m := db.SessionMetrics()

	b.ReportMetric(float64(logicalBytes), "logical_bytes")
	b.ReportMetric(m.WriteAmp, "yoga_write_amp")
}

// BenchmarkYogaDB_Batch measures batch-write throughput for YogaDB.
// It reports bytes written per operation and write amplification.
func BenchmarkYogaDB_Batch(b *testing.B) {

	for _, batchSize := range []int{1, 10, 100, 1000, 10000} {

		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {

			cfg := &Config{}
			db, err := OpenFlexDB("", cfg)
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
					batch.Set(string(keys[ki]), value)
					ki++
				}
				batch.Commit(false)
				batch.Close()
			}
			b.StopTimer()

			db.Sync()

			m := db.SessionMetrics()

			b.ReportMetric(float64(logicalBytes), "logical_bytes")
			b.ReportMetric(float64(batchSize), "batch_size")
			b.ReportMetric(m.WriteAmp, "yoga_write_amp")
			b.ReportMetric(float64(m.TotalBytesWritten), "yoga_total_physical_bytes_written")
		})
	}
}
