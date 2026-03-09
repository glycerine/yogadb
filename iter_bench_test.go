package yogadb

import (
	"bytes"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/bbolt"
)

const iterBenchKeyCount = 100_000

// generateBenchKeys creates iterBenchKeyCount unique keys using prng.NewCallID().
func generateBenchKeys() [][]byte {
	var seed [32]byte
	prng := newPRNG(seed)
	keys := make([][]byte, 0, iterBenchKeyCount)
	dup := make(map[string]bool, iterBenchKeyCount)
	for len(keys) < iterBenchKeyCount {
		cid := prng.NewCallID()
		if dup[cid] {
			continue
		}
		dup[cid] = true
		keys = append(keys, []byte(cid))
	}
	return keys
}

func Benchmark_Iter_YogaDB_Ascend(b *testing.B) {
	dir := b.TempDir()
	cfg := &Config{}
	db, err := OpenFlexDB(dir, cfg)
	panicOn(err)
	defer db.Close()

	keys := generateBenchKeys()

	// Insert all keys, reporting insert time.
	t0 := time.Now()
	batch := db.NewBatch()
	for i, k := range keys {
		batch.Set(string(k), k)
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

	var lastCounts string
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t0 := time.Now()
		db.View(func(roDB ReadOnlyDB) error {
			it := roDB.NewIter()
			it.SeekToFirst()
			count := 0
			for it.Valid() {
				count++
				it.Next()
			}
			it.Close()
			lastCounts = it.PathCounts()
			elapsed := time.Since(t0)
			if count > 0 {
				b.ReportMetric(float64(elapsed.Nanoseconds())/float64(count), "iter_ns/key")
			}
			return nil
		})
	}
	alwaysPrintf("path counts = '%v'", lastCounts)
	b.StopTimer()
}

func Benchmark_Iter_YogaDB_Descend(b *testing.B) {

	dir := b.TempDir()
	cfg := &Config{}
	db, err := OpenFlexDB(dir, cfg)
	panicOn(err)
	defer db.Close()

	keys := generateBenchKeys()

	// Insert all keys, reporting insert time.
	t0 := time.Now()
	batch := db.NewBatch()
	for i, k := range keys {
		batch.Set(string(k), k)
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
		db.View(func(roDB ReadOnlyDB) error {
			it := roDB.NewIter()
			it.SeekToLast()
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

func Benchmark_Iter_Pebble(b *testing.B) {
	dir := b.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	panicOn(err)
	defer db.Close()

	keys := generateBenchKeys()

	// Insert all keys, reporting insert time.
	t0 := time.Now()
	batch := db.NewBatch()
	for i, k := range keys {
		batch.Set(k, k, pebble.NoSync)
		if (i+1)%10000 == 0 {
			panicOn(batch.Commit(pebble.NoSync))
			batch = db.NewBatch()
		}
	}
	panicOn(batch.Commit(pebble.NoSync))
	panicOn(db.Flush())
	insertElapsed := time.Since(t0)
	b.ReportMetric(float64(insertElapsed.Nanoseconds())/float64(len(keys)), "insert_ns/key")
	vv("pebble insert %v", insertElapsed)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t0 := time.Now()
		iter, err := db.NewIter(nil)
		panicOn(err)
		count := 0
		for iter.First(); iter.Valid(); iter.Next() {
			count++
		}
		panicOn(iter.Close())
		elapsed := time.Since(t0)
		if count > 0 {
			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(count), "iter_ns/key")
		}
	}
	b.StopTimer()
}

func Benchmark_Iter_Bolt(b *testing.B) {
	dir := b.TempDir()
	dbPath := filepath.Join(dir, "bolt.db")

	// Configure options for maximum speed (UNSAFE for production)
	opts := &bbolt.Options{
		NoSync:         true, // Skips fsync() calls after each commit
		NoFreelistSync: true, // Skips syncing the freelist to disk
		NoGrowSync:     true, // Skips fsync() when the database file grows (platform-dependent)
	}
	db, err := bbolt.Open(dbPath, 0600, opts)
	panicOn(err)
	defer db.Close()

	keys := generateBenchKeys()

	// Insert all keys, reporting insert time.
	bucketName := []byte("bench")
	t0 := time.Now()
	err = db.Update(func(tx *bbolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		for _, k := range keys {
			if err := bkt.Put(k, k); err != nil {
				return err
			}
		}
		return nil
	})
	panicOn(err)
	insertElapsed := time.Since(t0)
	vv("bbolt insert %v", insertElapsed)
	b.ReportMetric(float64(insertElapsed.Nanoseconds())/float64(len(keys)), "insert_ns/key")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t0 := time.Now()
		var count int
		err := db.View(func(tx *bbolt.Tx) error {
			c := tx.Bucket(bucketName).Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				count++
			}
			return nil
		})
		panicOn(err)
		elapsed := time.Since(t0)
		if count > 0 {
			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(count), "iter_ns/key")
		}
	}
	b.StopTimer()
}

/*
go test -v -run=xxx -bench=Iter
goos: darwin
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
Benchmark_Iter_YogaDB_
Benchmark_Iter_YogaDB_-8   	      33	  31760600 ns/op	       319.6 iter_ns/key
Benchmark_Iter_Pebble
Benchmark_Iter_Pebble-8    	      80	  12817115 ns/op	       119.1 iter_ns/key
Benchmark_Iter_Bolt
Benchmark_Iter_Bolt-8      	     984	   1205083 ns/op	        12.47 iter_ns/key
PASS
ok  	github.com/glycerine/yogadb	39.845s


With insert timings:

-*- mode: compilation; default-directory: "~/yogadb/" -*-
Compilation started at Fri Mar  6 05:20:08

go test -v -run=xxx -bench=Iter
goos: darwin
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
Benchmark_Iter_YogaDB_

iter_bench_test.go:54 [goID 19] 2026-03-06 08:20:13.175374000 +0000 UTC yogadb insert 347.581556ms

iter_bench_test.go:54 [goID 43] 2026-03-06 08:20:14.032653000 +0000 UTC yogadb insert 293.990217ms

iter_bench_test.go:54 [goID 43] 2026-03-06 08:20:15.434466000 +0000 UTC yogadb insert 298.539252ms

iter_bench_test.go:54 [goID 43] 2026-03-06 08:20:17.019145000 +0000 UTC yogadb insert 292.681442ms
Benchmark_Iter_YogaDB_-8   	      40	  30325903 ns/op	       344.6 iter_ns/key
Benchmark_Iter_Pebble
2026/03/06 08:20:18 Found 0 WALs

iter_bench_test.go:97 [goID 10] 2026-03-06 08:20:19.813331000 +0000 UTC pebble insert 1.211243942s
2026/03/06 08:20:20 Found 0 WALs

iter_bench_test.go:97 [goID 133] 2026-03-06 08:20:21.370222000 +0000 UTC pebble insert 1.226966331s
2026/03/06 08:20:21 Found 0 WALs

iter_bench_test.go:97 [goID 133] 2026-03-06 08:20:23.396435000 +0000 UTC pebble insert 1.34724021s
2026/03/06 08:20:24 Found 0 WALs

iter_bench_test.go:97 [goID 133] 2026-03-06 08:20:25.847963000 +0000 UTC pebble insert 1.289226616s
Benchmark_Iter_Pebble-8    	     100	  11789405 ns/op	       123.9 iter_ns/key
Benchmark_Iter_Bolt

iter_bench_test.go:143 [goID 335] 2026-03-06 08:20:36.760933000 +0000 UTC bbolt insert 9.640491743s

iter_bench_test.go:143 [goID 370] 2026-03-06 08:20:45.992883000 +0000 UTC bbolt insert 9.160110824s

iter_bench_test.go:143 [goID 370] 2026-03-06 08:20:55.311172000 +0000 UTC bbolt insert 9.135040463s
Benchmark_Iter_Bolt-8      	     956	   1142467 ns/op	        11.14 iter_ns/key
PASS
ok  	github.com/glycerine/yogadb	43.884s

Compilation finished at Fri Mar  6 05:20:56

In addition to NoSync, try setting NoFreelistSync and NoGrowSync. Did not help
speed up insertion.

go test -v -run=xxx -bench=Iter_Bolt
goos: darwin
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
Benchmark_Iter_Bolt

iter_bench_test.go:150 [goID 26] 2026-03-06 08:24:03.827632000 +0000 UTC bbolt insert 9.337382789s

iter_bench_test.go:150 [goID 34] 2026-03-06 08:24:13.063413000 +0000 UTC bbolt insert 9.165198269s

iter_bench_test.go:150 [goID 34] 2026-03-06 08:24:22.576358000 +0000 UTC bbolt insert 9.327407471s
Benchmark_Iter_Bolt-8   	     974	   1182140 ns/op	        11.26 iter_ns/key
PASS
ok  	github.com/glycerine/yogadb	29.351s

Compilation finished at Fri Mar  6 05:24:23

=== Optimization round 1: stateful FlexSpace cursor + HLC version detection ===

Three changes applied:
  1. flexCursor: holds position in sparse index tree (node, anchorIdx, kvIdx
     within cached interval). flexCursorNext steps forward in O(1) amortized
     instead of re-seeking from the sparse index root each time.
  2. HLC version detection: Iter snapshots db.hlc on Seek. On Next(), if
     db.hlc.Aload() == snapshotHLC, the FlexSpace cursor is still valid
     (no mutations happened) and we skip re-seeking it.
  3. Empty-memtable fast path: when both memtables are empty (post-Sync),
     skip all btree seeks and 3-way merging; iterate FlexSpace directly.
  4. Buffer reuse: Iter reuses keyBuf/valBuf across Next() calls to avoid
     allocation per step.

go test -v -run=xxx -bench=Benchmark_Iter -benchtime=3x
goos: darwin
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
Benchmark_Iter_YogaDB_-8   	       3	  16575014 ns/op	       171.6 iter_ns/key
Benchmark_Iter_Pebble-8    	       3	  24596809 ns/op	       211.6 iter_ns/key
Benchmark_Iter_Bolt-8      	       3	   2998000 ns/op	        11.78 iter_ns/key
PASS

Summary: YogaDB 379.2 -> 171.6 ns/key (2.2x faster, now beats Pebble)

=== Optimization round 2: eliminate dupBytes from hot path ===

Changes:
  1. flexCursorSeekGE no longer returns a KV copy. It just positions the
     cursor; callers read fc.fce.kvs[fc.kvIdx] directly (zero-copy ref
     into the interval cache entry, which is refcounted).
  2. flexCursorNext renamed to flexCursorAdvance — void return, just
     advances fc.kvIdx++ (common case: one increment, no alloc).
     Cross-interval boundary loads next anchor's cache entry.
  3. flexSpaceOnlySeekGE reads KV directly from cache, copies only once
     into it.keyBuf/valBuf via reuseAppend (reuses capacity, no alloc
     after warmup).

go test -v -run=xxx -bench=Benchmark_Iter -benchtime=3x
goos: darwin
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
Benchmark_Iter_YogaDB_-8   	       3	  12272577 ns/op	       118.4 iter_ns/key
Benchmark_Iter_Pebble-8    	       3	  25679733 ns/op	       213.1 iter_ns/key
Benchmark_Iter_Bolt-8      	       3	   2771072 ns/op	        12.60 iter_ns/key
PASS

Summary: YogaDB 171.6 -> 118.4 ns/key (1.4x faster; 3.2x total from baseline)
         YogaDB is now 1.8x faster than Pebble on iteration.

=== Optimization round 3: lazy value resolution ===

Changes:
  1. flexSpaceOnlySeekGE no longer calls resolveVPtr or copies values.
     It stores a direct reference (kv.Value) into cache memory (GC-safe).
  2. Value() lazily copies into valBuf on first access via reuseAppend.
     If the caller never calls Value() (key-only scan), zero value work.
  3. valueResolved bool tracks whether the lazy copy has been done.

go test -v -run=xxx -bench=Benchmark_Iter -benchtime=5s
goos: darwin
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
Benchmark_Iter_YogaDB_-8   	     904	   6071297 ns/op	        57.27 iter_ns/key
Benchmark_Iter_Pebble-8    	     494	  12222120 ns/op	       116.2 iter_ns/key
Benchmark_Iter_Bolt-8      	    5139	   1273625 ns/op	        11.71 iter_ns/key
PASS

Summary: YogaDB 118.4 -> 57.27 ns/key (2.1x faster; 6.6x total from baseline)
         YogaDB is now 2.0x faster than Pebble on iteration.
         bbolt still 4.9x faster (mmap'd B+tree with zero-copy pointer arithmetic).

=== Optimization round 4: prefetch buffer (iterPreFetchKeyCount=100) ===

Changes:
  1. prefetchEntry struct + pfBuf/pfLen/pfPos fields on Iter.
  2. prefetchFillFlexSpaceOnly: under a single RLock, walks the FlexSpace
     cursor forward and fills up to iterPreFetchKeyCount entries. Keys are
     copied into reusable buffers; values are lazy cache references.
  3. Next() ultra-fast path: if pfPos < pfLen and HLC unchanged, serve
     from buffer with ZERO lock acquisition — just an atomic HLC check
     + array index. 99/100 Next() calls skip the RLock entirely.
  4. Seek() fills the prefetch buffer on the fast path (empty memtables).
  5. HLC change invalidates the buffer; Prev() and releaseIterState() clear it.

go test -v -run=xxx -bench=Benchmark_Iter -benchtime=5s
goos: darwin
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
Benchmark_Iter_YogaDB_-8   	    1100	   5188769 ns/op	        46.68 iter_ns/key
Benchmark_Iter_Pebble-8    	     462	  12752627 ns/op	       126.7 iter_ns/key
Benchmark_Iter_Bolt-8      	    4773	   1248873 ns/op	        12.00 iter_ns/key
PASS

Summary: YogaDB 57.27 -> 46.68 ns/key (1.2x faster; 6.8x total from baseline)
         YogaDB is now 2.7x faster than Pebble on iteration.
         bbolt still 3.9x faster.

=== Optimization round 4b: zero-copy key references in prefetch ===

Changes:
  1. In prefetchFillFlexSpaceOnly, replace reuseAppend(pe.key, kv.Key) with
     pe.key = kv.Key — a direct reference into cache memory (GC-safe).
     Same safety model as lazy values: the Go GC keeps the underlying byte
     array alive as long as pe.key references it, even after the cache
     entry is evicted from the LRU.

go test -v -run=xxx -bench=Benchmark_Iter -benchtime=5s
goos: darwin
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
Benchmark_Iter_YogaDB_-8   	    2880	   2061507 ns/op	        20.28 iter_ns/key
Benchmark_Iter_Pebble-8    	     456	  12352978 ns/op	       125.3 iter_ns/key
Benchmark_Iter_Bolt-8      	    4773	   1286959 ns/op	        11.75 iter_ns/key
PASS

Summary: YogaDB 46.68 -> 20.28 ns/key (2.3x faster; 15.8x total from baseline)
         YogaDB is now 6.2x faster than Pebble on iteration.
         bbolt still 1.7x faster (mmap zero-copy pointer arithmetic).

pre-fetch pointers and use a free list

Benchmark_Iter_Pebble-8    	      78	  12939428 ns/op	       116.7 iter_ns/key
Benchmark_Iter_YogaDB_-8   	     675	   1658875 ns/op	        15.61 iter_ns/key
Benchmark_Iter_Bolt-8      	     897	   1250012 ns/op	        12.08 iter_ns/key

        bbolt is 1.3x faster, but yoga is pretty close.

atg after span descriptors
Benchmark_Iter_YogaDB_Ascend-8   	    4050	   1468370 ns/op	        14.18 iter_ns/key


-*- mode: compilation; default-directory: "~/yogadb/" -*-
Compilation started at Fri Mar  6 12:35:33

go test -v -run=xxx -bench=Iter_YogaDB_Ascend -cpuprofile=fwd20.prof -benchtime=20s
goos: darwin
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
Benchmark_Iter_YogaDB_Ascend

iter_bench_test.go:59 [goID 26] 2026-03-06 15:35:34.435124000 +0000 UTC yogadb insert 393.30762ms

iter_bench_test.go:59 [goID 28] 2026-03-06 15:35:35.336902000 +0000 UTC yogadb insert 401.745494ms

iter_bench_test.go:59 [goID 28] 2026-03-06 15:35:36.214485000 +0000 UTC yogadb insert 343.322583ms

iter_bench_test.go:59 [goID 28] 2026-03-06 15:35:50.325031000 +0000 UTC yogadb insert 345.322257ms
Benchmark_Iter_YogaDB_Ascend-8   	   17880	   1321547 ns/op	        12.56 iter_ns/key
PASS
ok  	github.com/glycerine/yogadb	40.503s

Compilation finished at Fri Mar  6 12:36:14

latest after spans:

-*- mode: compilation; default-directory: "~/yogadb/" -*-
Compilation started at Fri Mar  6 18:12:21

go test -v -run=xxx -bench=Iter
goos: darwin
goarch: amd64
pkg: github.com/glycerine/yogadb
cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
Benchmark_Iter_YogaDB_Ascend

iter_bench_test.go:59 [goID 50] 2026-03-06 21:12:23.697876000 +0000 UTC yogadb insert 380.260327ms

iter_bench_test.go:59 [goID 19] 2026-03-06 21:12:24.479144000 +0000 UTC yogadb insert 333.441451ms

iter_bench_test.go:59 [goID 19] 2026-03-06 21:12:25.309323000 +0000 UTC yogadb insert 337.808459ms
Benchmark_Iter_YogaDB_Ascend-8    	     925	   1240290 ns/op	        11.54 iter_ns/key
Benchmark_Iter_YogaDB_Descend

iter_bench_test.go:112 [goID 20] 2026-03-06 21:12:27.152665000 +0000 UTC yogadb insert 339.015828ms

iter_bench_test.go:112 [goID 21] 2026-03-06 21:12:27.849866000 +0000 UTC yogadb insert 325.151767ms

iter_bench_test.go:112 [goID 21] 2026-03-06 21:12:28.714604000 +0000 UTC yogadb insert 349.504446ms
Benchmark_Iter_YogaDB_Descend-8   	     826	   1318733 ns/op	        12.64 iter_ns/key
Benchmark_Iter_Pebble
2026/03/06 21:12:30 Found 0 WALs

iter_bench_test.go:159 [goID 23] 2026-03-06 21:12:31.355585000 +0000 UTC pebble insert 1.210014073s
2026/03/06 21:12:31 Found 0 WALs

iter_bench_test.go:159 [goID 138] 2026-03-06 21:12:32.919384000 +0000 UTC pebble insert 1.23637421s
2026/03/06 21:12:33 Found 0 WALs

iter_bench_test.go:159 [goID 138] 2026-03-06 21:12:34.821018000 +0000 UTC pebble insert 1.246402877s
2026/03/06 21:12:35 Found 0 WALs

iter_bench_test.go:159 [goID 138] 2026-03-06 21:12:37.227791000 +0000 UTC pebble insert 1.216118615s
Benchmark_Iter_Pebble-8           	     100	  11819443 ns/op	       114.9 iter_ns/key

Benchmark_Iter_Bolt

iter_bench_test.go:212 [goID 357] 2026-03-06 21:12:47.899073000 +0000 UTC bbolt insert 9.404426539s

iter_bench_test.go:212 [goID 320] 2026-03-06 21:12:57.144547000 +0000 UTC bbolt insert 9.174200081s

iter_bench_test.go:212 [goID 320] 2026-03-06 21:13:06.439665000 +0000 UTC bbolt insert 9.115873801s
Benchmark_Iter_Bolt-8             	     910	   1180788 ns/op	        16.76 iter_ns/key
PASS
ok  	github.com/glycerine/yogadb	44.491s

Compilation finished at Fri Mar  6 18:13:07

*/
