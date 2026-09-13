package yogadb

import (
	"bytes"
	"os"
	"runtime"
	"slices"
	"testing"
	"time"

	"runtime/pprof"
)

func generateBenchKeysNseed(n int, seed0 byte) [][]byte {
	var seed [32]byte
	seed[0] = seed0
	prng := newPRNG(seed)
	keys := make([][]byte, 0, n)
	dup := make(map[string]bool, n)
	for len(keys) < n {
		cid := prng.NewCallID()
		if dup[cid] {
			continue
		}
		dup[cid] = true
		keys = append(keys, []byte(cid))
	}
	return keys
}

// TODO: worst case for keystable as memtable would be
// alternating some inserts and some searches. maybe
// add a test like that.

// rog linux: about 75K writes/sec.
func Test_Writes_Occuring_After_Bulk_Load_YogaDB(t *testing.T) {
	//if !testing.Short() {
	//t.Skip("long test; only run for -short because it is opposites day.")
	//}
	dir := t.TempDir()
	// if we are right on the border of 5 seconds, sometimes the
	// flush worker sneeks in and destroys our perf with a ton of flush work, and we
	// go from 600K writes/sec -> 220K writes/sec. Turn off the background
	// flushes while trying to measure performance.
	cfg := &Config{
		DisableBackgroundFlush: true,

		//MemtableKind: MemtableKeyStable,
		MemtableKind: MemtableWormhole, // wormhole is the default.
	}
	vv("using cfg.MemtableKind = %s", cfg.MemtableKind)

	db, err := OpenFlexDB(dir, cfg)
	panicOn(err)
	defer db.Close()

	N := 2_000_000
	keys := generateBenchKeysNseed(N, 0)
	vals := make([][]byte, len(keys))
	for i := range keys {
		vals[i] = make([]byte, 100)
		n := 0
		for range 5 {
			n += copy(vals[i][n:], keys[i])
		}
	}

	expectedVals := make(map[string][]byte, N*2)
	expectedVtyps := make(map[string]uint64, N*2)
	for i, k := range keys {
		expectedVals[string(k)] = vals[i]
		expectedVtyps[string(k)] = uint64(i)
	}

	// Insert first batch of keys, ignoring insert time.
	batch := db.NewBatch()
	for i, k := range keys {
		vtyp := uint64(i)

		batch.SetBytes(k, vals[i], vtyp)
		if (i+1)%10000 == 0 {
			batch.Commit(false)
			batch = db.NewBatch()
		}
	}
	batch.Commit(false)
	db.Sync()

	db.AllowReads()

	// what we care about is this second fresh batch.
	// this is extremely unlikely to have any collisions with the first batch.
	keys2 := generateBenchKeysNseed(N, 1)

	vals2 := make([][]byte, len(keys2))
	for i := range keys2 {
		vals2[i] = make([]byte, 100)
		n := 0
		for range 5 {
			n += copy(vals2[i][n:], keys2[i])
		}
		vtyp := uint64(N + i + 1)
		expectedVals[string(keys2[i])] = vals2[i]
		expectedVtyps[string(keys2[i])] = vtyp
	}

	runtime.GC() // get up-to-date statistics
	m0 := &runtime.MemStats{}
	runtime.ReadMemStats(m0)

	const profile = true
	if profile {
		// --- START PROFILING ---
		f, err := os.Create("cpu_afterbulk_new_writes.out")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			t.Fatal(err)
		}
	}

	t0 := time.Now()
	batch = db.NewBatch()
	for i, k := range keys2 {
		vtyp := uint64(N + i + 1)
		batch.SetBytes(k, vals2[i], vtyp)
		if (i+1)%10000 == 0 {
			batch.Commit(false)
			batch = db.NewBatch()
		}
	}
	batch.Commit(true)
	if profile {
		pprof.StopCPUProfile()
	}

	runtime.GC() // get up-to-date statistics
	m2 := &runtime.MemStats{}
	runtime.ReadMemStats(m2)
	vv("end new writes: HeapAlloc = %v (diff: %v);  HeapInuse = %v (diff: %v)", formatUint64Under(m2.HeapAlloc), formatUint64Under(m2.HeapAlloc-m0.HeapAlloc), formatUint64Under(m2.HeapInuse), formatUint64Under(m2.HeapInuse-m0.HeapInuse))

	// do GC, then output HeapAlloc and HeapInuse numbers
	WriteMemProfiles("profile.memory.afterbulk_new_writes.out")

	//_, metrics, err := batch.CommitGetMetrics(true)
	insertElapsed := time.Since(t0)
	panicOn(err)
	rate := float64(len(keys2)) / insertElapsed.Seconds()

	//vv("after bulkload terminated with AllowReads: yogadb insert %v writes/sec\n%s\n", rate, metrics)
	vv("after bulkload terminated with AllowReads: yogadb insert %v writes/sec\n", rate)

	allkeys := append([][]byte{}, keys...)
	allkeys = append(allkeys, keys2...)
	slices.SortFunc(allkeys, bytes.Compare)
	for i := range allkeys {
		if i == 0 {
			continue
		}
		if 0 == bytes.Compare(allkeys[i], allkeys[i-1]) {
			t.Fatalf("ugh. duplicated keys. we wanted batch 2 to be disjoint from batch 1.")
		}
	}
	vv("good: all %v keys were distinct. len(vals) = %v; len(vals2) = %v", len(allkeys), len(vals), len(vals2))

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		it.SeekFirst()
		j := 0
		for j < len(allkeys) {
			if !it.Valid() {
				t.Fatalf("invalid it at only j=%v", j)
			}
			got := it.Key()
			gotv, vtyp, _, err := it.FetchV()
			panicOn(err)

			if string(allkeys[j]) != string(got) {
				t.Fatalf("at j=%v, expected key '%v', got '%v'", j, string(allkeys[j]), string(got))
			}
			wantv, ok := expectedVals[string(got)]
			if !ok {
				t.Fatalf("at j=%v, unexpected key '%v'", j, string(got))
			}
			if 0 != bytes.Compare(gotv, wantv) {
				t.Fatalf("at j=%v, expected value '%v', got '%v'", j, string(wantv), gotv)
			}
			if wantVtyp := expectedVtyps[string(got)]; vtyp != wantVtyp {
				t.Fatalf("at j=%v, key '%v' got vtyp %v, want %v", j, string(got), vtyp, wantVtyp)
			}
			it.Next()
			j++
		}
		if it.Valid() {
			t.Fatalf("iterator should be invalid after %v", len(allkeys))
		}
		//it.Close()
		if j == len(allkeys) {
			vv("good: verified all %v keys", j)
		} else {
			vv("only verified %v out of %v", j, len(allkeys))
		}
		return nil
	})
}

// replace all of the first set: use same keys in 2nd set.
func Test_Replacement_After_Bulk_Load_YogaDB(t *testing.T) {
	//if !testing.Short() {
	//t.Skip("long test; only run for -short because it is opposites day.")
	//}
	dir := t.TempDir()
	cfg := &Config{
		DisableBackgroundFlush: true,

		//MemtableKind: MemtableKeyStable,
		MemtableKind: MemtableWormhole, // wormhole is the default.
	}
	vv("using cfg.MemtableKind = %s", cfg.MemtableKind)

	db, err := OpenFlexDB(dir, cfg)
	panicOn(err)
	defer db.Close()

	//N := 20_000
	N := 2_000_000
	//N := 10_000_000
	keys := generateBenchKeysNseed(N, 0)
	vals := make([][]byte, len(keys))
	for i := range keys {
		vals[i] = make([]byte, 100)
		n := 0
		for range 5 {
			n += copy(vals[i][n:], keys[i])
		}
	}

	expectedVals := make(map[string][]byte, N)
	expectedVtyps := make(map[string]uint64, N)

	// Insert first batch of keys, ignoring insert time.
	batch := db.NewBatch()
	for i, k := range keys {
		vtyp := uint64(i)

		batch.SetBytes(k, vals[i], vtyp)
		if (i+1)%10000 == 0 {
			batch.Commit(false)
			batch = db.NewBatch()
		}
	}
	batch.Commit(false)
	db.Sync()

	db.AllowReads()

	// what we care about is this second fresh batch.
	// this is extremely unlikely to have any collisions with the first batch.
	keys2 := generateBenchKeysNseed(N, 1)
	vals2 := make([][]byte, len(keys2))

	for i := range keys2 {
		vals2[i] = make([]byte, 100)
		n := 0
		for range 5 {
			n += copy(vals2[i][n:], keys2[i])
		}
		vtyp := uint64(N + i + 1)
		expectedVals[string(keys[i])] = vals2[i]
		expectedVtyps[string(keys[i])] = vtyp
	}

	runtime.GC() // get up-to-date statistics
	m0 := &runtime.MemStats{}
	runtime.ReadMemStats(m0)

	// --- START PROFILING ---
	f, err := os.Create("cpu_afterbulk_replacement.out")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		t.Fatal(err)
	}

	t0 := time.Now()
	batch = db.NewBatch()

	// note how we write keys with vals2 (ignoring keys2)
	for i, k := range keys {
		vtyp := uint64(N + i + 1)
		batch.SetBytes(k, vals2[i], vtyp)
		if (i+1)%10000 == 0 {
			batch.Commit(false)
			batch = db.NewBatch()
		}
	}
	batch.Commit(true)
	pprof.StopCPUProfile()

	runtime.GC() // get up-to-date statistics
	m2 := &runtime.MemStats{}
	runtime.ReadMemStats(m2)
	vv("end replacements: HeapAlloc = %v (diff: %v);  HeapInuse = %v (diff: %v)", formatUint64Under(m2.HeapAlloc), formatUint64Under(m2.HeapAlloc-m0.HeapAlloc), formatUint64Under(m2.HeapInuse), formatUint64Under(m2.HeapInuse-m0.HeapInuse))

	WriteMemProfiles("profile.memory.afterbulk_replacement.out")

	//_, metrics, err := batch.CommitGetMetrics(true)
	insertElapsed := time.Since(t0)
	panicOn(err)
	rate := float64(len(keys)) / insertElapsed.Seconds()

	//vv("after bulkload terminated with AllowReads: yogadb replacements: %v writes/sec\n%s\n", rate, metrics)
	vv("after bulkload terminated with AllowReads: yogadb replacements: %v writes/sec\n", rate)

	allkeys := append([][]byte{}, keys...)
	slices.SortFunc(allkeys, bytes.Compare)

	db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		it.SeekFirst()
		j := 0
		for j < len(allkeys) {
			if !it.Valid() {
				t.Fatalf("invalid it at only j=%v", j)
			}
			got := it.Key()
			gotv, vtyp, _, err := it.FetchV()
			panicOn(err)

			if string(allkeys[j]) != string(got) {
				t.Fatalf("at j=%v, expected key '%v', got '%v'", j, string(allkeys[j]), string(got))
			}
			wantv, ok := expectedVals[string(got)]
			if !ok {
				t.Fatalf("at j=%v, unexpected key '%v'", j, string(got))
			}
			if 0 != bytes.Compare(gotv, wantv) {
				t.Fatalf("at j=%v, expected value '%v', got '%v'", j, string(wantv), gotv)
			}
			if wantVtyp := expectedVtyps[string(got)]; vtyp != wantVtyp {
				t.Fatalf("at j=%v, key '%v' got vtyp %v, want %v", j, string(got), vtyp, wantVtyp)
			}
			it.Next()
			j++
		}
		if it.Valid() {
			t.Fatalf("iterator should be invalid after %v", len(allkeys))
		}
		//it.Close()
		if j == len(allkeys) {
			vv("good: verified all %v keys", j)
		} else {
			vv("only verified %v out of %v", j, len(allkeys))
		}
		return nil
	})
}

/* wormhole after prefix addition, and more optimization (no lazy hlc dedup yet),

with point map maintained always, is 20% slower.
but we are ready for mixed reads/writes since we are after AllowReads().

go test -v -run After_Bulk
=== RUN   Test_Writes_Occuring_After_Bulk_Load_YogaDB

db.go:1649 [pid 1173462] 2026-09-13 08:23:18.568425332 +0000 UTC using cfg.MemtableKind = wormhole

afterbulk_test.go:141 [pid 1173462] 2026-09-13 08:23:29.900245355 +0000 UTC end new writes: HeapAlloc = 2_550_158_848 (diff: 317_276_376);  HeapInuse = 2_691_047_424 (diff: 311_443_456)

afterbulk_test.go:152 [pid 1173462] 2026-09-13 08:23:30.056351702 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 464712.0048064921 writes/sec
--------
without point map maintained:

go test -v -run After_Bulk
=== RUN   Test_Writes_Occuring_After_Bulk_Load_YogaDB

db.go:1649 [pid 1143487] 2026-09-13 07:33:53.226589245 +0000 UTC using cfg.MemtableKind = wormhole

afterbulk_test.go:141 [pid 1143487] 2026-09-13 07:34:03.989695363 +0000 UTC end new writes: HeapAlloc = 2_438_294_680 (diff: 317_257_656);  HeapInuse = 2_580_242_432 (diff: 312_655_872)

afterbulk_test.go:152 [pid 1143487] 2026-09-13 07:34:04.126776690 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 536273.7237336345 writes/sec

----
keystable still a bit faster at lots of writes, but only about 5% faster (26/536 == 0.0485):

-*- mode: compilation; default-directory: "~/yogadb/" -*-
Compilation started at Sun Sep 13 02:40:08

go test -v -run After_Bulk
=== RUN   Test_Writes_Occuring_After_Bulk_Load_YogaDB

db.go:1649 [pid 1147678] 2026-09-13 07:40:12.655565731 +0000 UTC using cfg.MemtableKind = keystable

afterbulk_test.go:141 [pid 1147678] 2026-09-13 07:40:23.321866412 +0000 UTC end new writes: HeapAlloc = 2_544_389_496 (diff: 423_759_728);  HeapInuse = 2_686_009_344 (diff: 419_340_288)

afterbulk_test.go:152 [pid 1147678] 2026-09-13 07:40:23.471622575 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 562110.3014525231 writes/sec

*/
/*
fascinating:
=== RUN   Test_Replacement_After_Bulk_Load_YogaDB

wormhole:

afterbulk_test.go:302 [pid 1181984] 2026-09-13 08:39:07.318008092 +0000 UTC end replacements: HeapAlloc = 1_765_804_520 (diff: 310_263_664);  HeapInuse = 1_909_121_024 (diff: 322_813_952)

afterbulk_test.go:312 [pid 1181984] 2026-09-13 08:39:07.412607183 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 91455.29651831448 writes/sec

keystable: (and no change with kvs only).

=== RUN   Test_Replacement_After_Bulk_Load_YogaDB

afterbulk_test.go:222 [pid 1183556] 2026-09-13 08:40:45.111475138 +0000 UTC using cfg.MemtableKind = keystable

afterbulk_test.go:309 [pid 1183556] 2026-09-13 08:41:11.895160376 +0000 UTC end replacements: HeapAlloc = 1_760_013_120 (diff: 416_721_240);  HeapInuse = 1_913_716_736 (diff: 438_575_104)

afterbulk_test.go:319 [pid 1183556] 2026-09-13 08:41:11.992306558 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 95956.10942222615 writes/sec

wormhole with lazy point map: from d6fbf82, same speed as wormhole with top level point map on this workload.

=== RUN   Test_Replacement_After_Bulk_Load_YogaDB

db.go:1649 [pid 1185850] 2026-09-13 08:45:09.971175337 +0000 UTC using cfg.MemtableKind = wormhole

afterbulk_test.go:302 [pid 1185850] 2026-09-13 08:45:37.360911129 +0000 UTC end replacements: HeapAlloc = 1_653_860_304 (diff: 310_153_088);  HeapInuse = 1_801_084_928 (diff: 323_919_872)

afterbulk_test.go:312 [pid 1185850] 2026-09-13 08:45:37.457766132 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 92707.76059367288 writes/sec

--------

*/
/* nice: keystable kvs only, "stable []int" gone, is still fast, just less memory allocation.
go test -v -run After_Bulk
=== RUN   Test_Writes_Occuring_After_Bulk_Load_YogaDB

afterbulk_test.go:51 [pid 1224817] 2026-09-13 09:48:43.285634125 +0000 UTC using cfg.MemtableKind = keystable

afterbulk_test.go:143 [pid 1224817] 2026-09-13 09:48:53.909293637 +0000 UTC end new writes: HeapAlloc = 2_463_366_200 (diff: 342_755_072);  HeapInuse = 2_599_297_024 (diff: 331_792_384)

afterbulk_test.go:154 [pid 1224817] 2026-09-13 09:48:54.065214980 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 552999.9857843058 writes/sec


afterbulk_test.go:167 [pid 1224817] 2026-09-13 09:48:56.322440573 +0000 UTC good: all 4000000 keys were distinct. len(vals) = 2000000; len(vals2) = 2000000

afterbulk_test.go:202 [pid 1224817] 2026-09-13 09:49:08.071786377 +0000 UTC good: verified all 4000000 keys
--- PASS: Test_Writes_Occuring_After_Bulk_Load_YogaDB (29.08s)
=== RUN   Test_Replacement_After_Bulk_Load_YogaDB

afterbulk_test.go:222 [pid 1224817] 2026-09-13 09:49:12.362383697 +0000 UTC using cfg.MemtableKind = keystable

afterbulk_test.go:309 [pid 1224817] 2026-09-13 09:49:38.366866683 +0000 UTC end replacements: HeapAlloc = 1_679_215_128 (diff: 335_930_008);  HeapInuse = 1_839_546_368 (diff: 379_830_272)

afterbulk_test.go:319 [pid 1224817] 2026-09-13 09:49:38.461990914 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 95558.49254327564 writes/sec


versus wormhole:

go test -v -run After_Bulk
=== RUN   Test_Writes_Occuring_After_Bulk_Load_YogaDB

afterbulk_test.go:51 [pid 1261322] 2026-09-13 10:38:15.202567810 +0000 UTC using cfg.MemtableKind = wormhole

afterbulk_test.go:143 [pid 1261322] 2026-09-13 10:38:26.530647863 +0000 UTC end new writes: HeapAlloc = 2_550_034_088 (diff: 317_157_032);  HeapInuse = 2_691_399_680 (diff: 312_205_312)

afterbulk_test.go:154 [pid 1261322] 2026-09-13 10:38:26.677909468 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 467093.51339444495 writes/sec


afterbulk_test.go:167 [pid 1261322] 2026-09-13 10:38:28.955933986 +0000 UTC good: all 4000000 keys were distinct. len(vals) = 2000000; len(vals2) = 2000000

afterbulk_test.go:202 [pid 1261322] 2026-09-13 10:38:40.819528424 +0000 UTC good: verified all 4000000 keys
--- PASS: Test_Writes_Occuring_After_Bulk_Load_YogaDB (29.97s)
=== RUN   Test_Replacement_After_Bulk_Load_YogaDB

afterbulk_test.go:222 [pid 1261322] 2026-09-13 10:38:45.168653197 +0000 UTC using cfg.MemtableKind = wormhole

afterbulk_test.go:309 [pid 1261322] 2026-09-13 10:39:12.760623158 +0000 UTC end replacements: HeapAlloc = 1_765_649_280 (diff: 310_087_184);  HeapInuse = 1_895_661_568 (diff: 324_009_984)

afterbulk_test.go:319 [pid 1261322] 2026-09-13 10:39:12.860408058 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 90786.90473435256 writes/sec

*/
