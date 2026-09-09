package yogadb

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

func generateAscendingBenchKeys() [][]byte {
	keys := make([][]byte, iterBenchKeyCount)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
	}
	return keys
}

func Benchmark_LoadOnly_YogaDB_Ascending(b *testing.B) {
	keys := generateAscendingBenchKeys()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		dir := b.TempDir()
		cfg := &Config{}
		db, err := OpenFlexDB(dir, cfg)
		panicOn(err)
		b.StartTimer()

		batch := db.NewBatch()
		for i, k := range keys {
			panicOn(batch.SetBytes(k, k, 0))
			if (i+1)%10000 == 0 {
				_, err := batch.Commit(false)
				panicOn(err)
				batch = db.NewBatch()
			}
		}
		_, err = batch.Commit(false)
		panicOn(err)
		panicOn(db.Sync())
		b.StopTimer()
		db.Close()
		b.StartTimer()
	}
}

func Benchmark_LoadOnly_Pebble_Ascending(b *testing.B) {
	keys := generateAscendingBenchKeys()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		dir := b.TempDir()
		db, err := pebble.Open(dir, &pebble.Options{})
		panicOn(err)
		b.StartTimer()

		batch := db.NewBatch()
		for i, k := range keys {
			panicOn(batch.Set(k, k, pebble.NoSync))
			if (i+1)%10000 == 0 {
				panicOn(batch.Commit(pebble.NoSync))
				batch = db.NewBatch()
			}
		}
		panicOn(batch.Commit(pebble.NoSync))
		panicOn(db.Flush())
		b.StopTimer()
		panicOn(db.Close())
		b.StartTimer()
	}
}
