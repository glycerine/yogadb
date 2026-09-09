//go:build linux && cgo && rocksdb

package yogadb

import (
	"testing"
	"time"
)

func Benchmark_Iter_RocksDB_Ascend(b *testing.B) {
	dir := b.TempDir()
	db := openBenchRocksDB(dir)
	defer closeBenchRocksDB(db)

	keys := generateBenchKeys()
	writeOpts := newBenchRocksDBWriteOptions(false)
	defer closeBenchRocksDBWriteOptions(writeOpts)

	t0 := time.Now()
	for start := 0; start < len(keys); start += 10000 {
		end := start + 10000
		if end > len(keys) {
			end = len(keys)
		}
		rocksDBPutBatch(db, writeOpts, keys, start, end)
	}
	flushBenchRocksDB(db)
	insertElapsed := time.Since(t0)

	b.ReportMetric(float64(insertElapsed.Nanoseconds())/float64(len(keys)), "insert_ns/key")
	vv("rocksdb insert %v", insertElapsed)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t0 := time.Now()
		count := countBenchRocksDB(db)
		elapsed := time.Since(t0)
		if count != len(keys) {
			b.Fatalf("rocksdb iterator saw %v keys, want %v", count, len(keys))
		}
		if count > 0 {
			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(count), "iter_ns/key")
		}
	}
	b.StopTimer()
}

func Benchmark_LoadOnly_RocksDB(b *testing.B) {
	keys := generateBenchKeys()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		dir := b.TempDir()
		db := openBenchRocksDB(dir)
		writeOpts := newBenchRocksDBWriteOptions(false)
		b.StartTimer()

		for start := 0; start < len(keys); start += 10000 {
			end := start + 10000
			if end > len(keys) {
				end = len(keys)
			}
			rocksDBPutBatch(db, writeOpts, keys, start, end)
		}
		flushBenchRocksDB(db)
		b.StopTimer()
		closeBenchRocksDBWriteOptions(writeOpts)
		closeBenchRocksDB(db)
		b.StartTimer()
	}
}
