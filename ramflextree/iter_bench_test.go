package ramflextree

import (
	"bytes"
	"slices"
	"testing"
	"time"
)

func Benchmark_Iter_YogaDB_Ascend(b *testing.B) {
	cfg := &Config{}
	db, err := OpenFlexDB("", cfg)
	panicOn(err)
	defer db.Close()

	keys := generateBenchKeys()

	// Insert all keys, reporting insert time.
	t0 := time.Now()
	batch := db.NewBatch()
	for i, k := range keys {
		batch.Set(string(k), k, 0)
		if (i+1)%10000 == 0 {
			batch.Commit(false)
			batch = db.NewBatch()
		}
	}
	batch.Commit(false)
	db.Sync()

	slices.SortFunc(keys, bytes.Compare)

	insertElapsed := time.Since(t0)
	b.ReportMetric(float64(insertElapsed.Nanoseconds())/float64(len(keys)), "insert_ns/key")
	vv("yogadb insert %v", insertElapsed)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t0 := time.Now()
		db.View(func(roDB *ReadOnlyTx) error {
			it := roDB.NewIter()
			it.SeekFirst()
			count := 0
			for it.Valid() {
				count++
				it.Next()
			}
			it.Close()
			elapsed := time.Since(t0)
			if count > 0 {
				b.ReportMetric(float64(elapsed.Nanoseconds())/float64(count), "iter_ns/key")
			}
			return nil
		})
	}
	b.StopTimer()
}

func Benchmark_Iter_YogaDB_Descend(b *testing.B) {

	cfg := &Config{}
	db, err := OpenFlexDB("", cfg)
	panicOn(err)
	defer db.Close()

	keys := generateBenchKeys()

	// Insert all keys, reporting insert time.
	t0 := time.Now()
	batch := db.NewBatch()
	for i, k := range keys {
		batch.Set(string(k), k, 0)
		if (i+1)%10000 == 0 {
			batch.Commit(false)
			batch = db.NewBatch()
		}
	}
	batch.Commit(false)
	db.Sync()

	slices.SortFunc(keys, bytes.Compare)
	slices.Reverse(keys) // compare to Descending

	insertElapsed := time.Since(t0)
	b.ReportMetric(float64(insertElapsed.Nanoseconds())/float64(len(keys)), "insert_ns/key")
	vv("yogadb insert %v", insertElapsed)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t0 := time.Now()
		db.View(func(roDB *ReadOnlyTx) error {
			it := roDB.NewIter()
			it.SeekLast()
			count := 0
			for it.Valid() {
				count++
				it.Prev()
			}
			it.Close()
			elapsed := time.Since(t0)
			if count > 0 {
				b.ReportMetric(float64(elapsed.Nanoseconds())/float64(count), "iter_ns/key")
			}
			return nil
		})
	}
	b.StopTimer()
}
