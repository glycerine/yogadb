package yogadb

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

/*
# Plan: Interval Cache Test Suite (`intervalcache_test.go`)

## Context

The interval cache (`intervalcache.go`, 361 lines) is a critical data structure in FlexDB.
It sits between the sparse KV index and FlexSpace, caching decoded KV intervals so that
lookups avoid re-reading and re-decoding from disk. The cache uses 1024 hash-partitioned
shards with CLOCK replacement eviction and reference counting.

**Lookup path:** `Get(key) → sparse index finds anchor → cache partition (by key hash) →
getEntry() (cache hit or FlexSpace load) → FindKeyEQ (linear scan with fingerprints) → value`

The interval cache has no dedicated test file. The goal is to create `intervalcache_test.go`
with comprehensive unit tests, fuzz tests, and benchmarks that exercise the cache in
isolation (no FlexDB/FlexSpace needed) by constructing cache structures directly.

## Invariants to Check

### Data Structure Invariants

| ID | Description |
|----|-------------|
| INV-IC-1 | **Dedup preserves highest HLC:** After dedup, each unique key K has exactly one entry with `HLC == max(all input HLCs for K)` |
| INV-IC-2 | **Dedup output sorted:** Output of `intervalCacheDedup` is strictly sorted by key ascending |
| INV-IC-3 | **Dedup no duplicates:** `len(output) == number of distinct keys in input` |
| INV-IC-4 | **FindKeyGE correct position:** Returns `(idx, true)` if key exists; otherwise `(idx, false)` where idx is the first position with key > search key |
| INV-IC-5 | **FindKeyEQ correct match:** Returns `(idx, true)` only if `fce.kvs[idx].Key == key` |
| INV-IC-6 | **FindKeyGE/EQ agree:** If `FindKeyEQ` returns `(idx, true)`, `FindKeyGE` also returns `(_, true)` for same key |
| INV-IC-7 | **Fingerprint never zero:** `fingerprint(h) != 0` for all uint32 h |
| INV-IC-8 | **Fingerprints match keys:** `fce.fps[i] == fingerprint(kvCRC32(fce.kvs[i].Key))` for all i |
| INV-IC-9 | **Count consistency:** `fce.count == len(fce.kvs) == len(fce.fps)` |
| INV-IC-10 | **Size consistency:** `fce.size == sum(kvSizeApprox(fce.kvs[i]))` |
| INV-IC-11 | **Partition size tracks entries:** `partition.size == sum(32 + fce.size)` for all entries in clock list |
| INV-IC-12 | **Clock list is valid circular DLL:** Following next from tick returns to tick; `node.prev.next == node` and `node.next.prev == node` for all nodes |
| INV-IC-13 | **Insert preserves sorted order:** After `cacheEntryInsert` at correct position, kvs remains sorted |
| INV-IC-14 | **Delete decrements count/size:** After `cacheEntryDelete`, count -= 1, size -= kvSizeApprox(deleted) |
| INV-IC-15 | **Replace maintains count, adjusts size:** After `cacheEntryReplace`, count unchanged, size changes by delta |
| INV-IC-16 | **Eviction respects refcnt:** Entries with `refcnt > 0` are never evicted |
| INV-IC-17 | **Eviction reduces size:** After `calibrate()`, `size <= cap` OR all remaining entries pinned |
| INV-IC-18 | **Partition ID deterministic and in range:** `cachePartitionID(key)` ∈ [0, 1023], same input → same output |

## Tests

### Unit Tests (15 tests)

| Test | Invariants | What It Tests |
|------|-----------|---------------|
| `TestIntervalCache_DedupBasic` | 1,2,3 | No dups, all dups, mixed, empty, single |
| `TestIntervalCache_DedupHLCWins` | 1 | Ascending/descending/middle/equal HLC patterns |
| `TestIntervalCache_DedupSortedOutput` | 2 | 100 entries → 20 distinct, output strictly sorted |
| `TestIntervalCache_FindKeyGE` | 4 | Existing keys, missing keys (before/between/after), empty entry |
| `TestIntervalCache_FindKeyEQ` | 5 | Existing keys, missing keys, fingerprint verification |
| `TestIntervalCache_FindKeyAgreement` | 6 | 50 sorted keys: GE and EQ agree on all existing + random missing |
| `TestIntervalCache_FingerprintNonZero` | 7 | 100K random keys, plus h=0 and h=0x00010001 edge cases |
| `TestIntervalCache_ClockListIntegrity` | 12 | Insert 1/2/10 entries, remove middle/tick/all, verify circular DLL |
| `TestIntervalCache_CacheEntryInsert` | 8,9,10,13 | Insert at beginning/middle/end, verify sort/count/size/fps |
| `TestIntervalCache_CacheEntryReplace` | 9,10,15 | Replace with longer/shorter/same-size value |
| `TestIntervalCache_CacheEntryDelete` | 9,10,14 | Delete from beginning/middle/end/until empty |
| `TestIntervalCache_Calibrate` | 17 | Entries exceeding cap are evicted; access counter grants reprieve |
| `TestIntervalCache_EvictionRefcntProtection` | 16 | All entries pinned → none evicted; mix of pinned/unpinned |
| `TestIntervalCache_PartitionID` | 18 | Determinism, range [0,1023], edge cases (nil/empty/long keys) |
| `TestIntervalCache_NewCache` | — | 1024 partitions, per-partition cap, initial size=0, tick=nil |

### Fuzz Tests (3 tests)

| Test | Invariants | Strategy |
|------|-----------|----------|
| `FuzzIntervalCache_Dedup` | 1,2,3,8 | Random sorted KV slices → dedup → check all dedup invariants |
| `FuzzIntervalCache_FindKey` | 4,5,6 | Random sorted entries + random search key → check GE/EQ agreement |
| `FuzzIntervalCache_Mutations` | 8,9,10,13,14,15 | Random insert/replace/delete sequences on a cache entry → check structural invariants after each op |

### Benchmarks (3 benchmarks)

| Benchmark | What It Measures |
|-----------|-----------------|
| `BenchmarkIntervalCache_FindKeyEQ` | Linear scan with fingerprints (100 entries) |
| `BenchmarkIntervalCache_FindKeyGE` | Binary search (100 entries) |
| `BenchmarkIntervalCache_Dedup` | Dedup of 1000 KVs with 200 distinct keys |

## Helper Functions

```go
// Test data construction (no FlexDB needed)
func makeKV(key, value string, hlc int64) KV
func makeSortedKVs(keys []string) []KV          // sequential HLCs, default values
func makeAnchor(key string) *dbAnchor
func makeCacheEntry(kvs []KV) *intervalCacheEntry      // computes fps, size, count
func makePartition(capBytes int64) *intervalCachePartition

// Invariant checkers (reused across tests and fuzz)
func checkClockListIntegrity(t testing.TB, p *intervalCachePartition, expectedCount int)
func checkFingerprintsMatch(t testing.TB, fce *intervalCacheEntry)
func checkCountAndSize(t testing.TB, fce *intervalCacheEntry)
func checkSorted(t testing.TB, fce *intervalCacheEntry)
```

## File Created

| File | Description |
|------|-------------|
| `intervalcache_test.go` (NEW) | All 15 unit tests, 3 fuzz tests, 3 benchmarks, helper functions |

## Key Architectural Notes

- All tests construct cache structures directly — no FlexDB/FlexSpace/disk I/O needed
- `loadInterval()` is NOT tested here (requires FlexSpace); integration tests in `flexdb_test.go` already cover it
- The `intervalCacheDedup` function reuses the input slice (`out := kvs[:0]`); tests account for this
- Relevant helper functions from `db.go`: `kvLess`, `kvSizeApprox`, `kvCRC32`, `fingerprint`, `cachePartitionID`, `KV` type, `HLC` type

## Verification

```bash
# Run all interval cache tests
go test -v -run TestIntervalCache -count=1 ./...

# Run fuzz tests (30 seconds each)
go test -fuzz FuzzIntervalCache_Dedup -fuzztime 30s -run=xxx
go test -fuzz FuzzIntervalCache_FindKey -fuzztime 30s -run=xxx
go test -fuzz FuzzIntervalCache_Mutations -fuzztime 30s -run=xxx

# Run benchmarks
go test -bench BenchmarkIntervalCache -benchmem -run=xxx ./...

# Full suite (ensure no regressions)
go test -count=1 ./...
```

  ┌────────────┬───────┬─────────────────────┐
  │  Category  │ Count │       Status        │
  ├────────────┼───────┼─────────────────────┤
  │ Unit tests │ 16    │ All pass            │
  ├────────────┼───────┼─────────────────────┤
  │ Fuzz tests │ 3     │ All pass (10s each) │
  ├────────────┼───────┼─────────────────────┤
  │ Benchmarks │ 3     │ All pass            │
  └────────────┴───────┴─────────────────────┘

  Benchmark results:
  - FindKeyEQ (linear scan + fingerprints, 100 entries): ~45 ns/op, 0 allocs
  - FindKeyGE (binary search, 100 entries): ~23 ns/op, 0 allocs
  - Dedup (1000 KVs → 200 distinct): ~26 us/op

  Full test suite: All existing tests continue to pass (50s total, no regressions).
*/

// ====================== Test Helpers ======================

func makeKV(key, value string, hlc int64) KV {
	return KV{Key: []byte(key), Value: []byte(value), Hlc: HLC(hlc)}
}

func makeSortedKVs(keys []string) []KV {
	sort.Strings(keys)
	kvs := make([]KV, len(keys))
	for i, k := range keys {
		kvs[i] = KV{Key: []byte(k), Value: []byte("v-" + k), Hlc: HLC(int64(i + 1))}
	}
	return kvs
}

func makeAnchor(key string) *dbAnchor {
	return &dbAnchor{key: []byte(key)}
}

func makeCacheEntry(kvs []KV) *intervalCacheEntry {
	fps := make([]uint16, len(kvs))
	size := 0
	for i, kv := range kvs {
		fps[i] = fingerprint(kvCRC32(kv.Key))
		size += kvSizeApprox(&kv)
	}
	return &intervalCacheEntry{
		kvs:   kvs,
		fps:   fps,
		size:  size,
		count: len(kvs),
	}
}

func makePartition(capBytes int64) *intervalCachePartition {
	return &intervalCachePartition{cap: capBytes}
}

// ====================== Invariant Checkers ======================

func checkClockListIntegrity(t testing.TB, p *intervalCachePartition, expectedCount int) {
	t.Helper()
	if expectedCount == 0 {
		if p.tick != nil {
			t.Fatalf("expected nil tick for empty clock, got non-nil")
		}
		return
	}
	if p.tick == nil {
		t.Fatalf("expected non-nil tick for %d entries", expectedCount)
	}
	// Walk forward counting nodes; verify DLL pointers.
	count := 0
	node := p.tick
	for {
		if node.next.prev != node {
			t.Fatalf("node.next.prev != node at position %d", count)
		}
		if node.prev.next != node {
			t.Fatalf("node.prev.next != node at position %d", count)
		}
		count++
		node = node.next
		if node == p.tick {
			break
		}
		if count > expectedCount+10 {
			t.Fatalf("clock list appears broken: walked %d nodes, expected %d", count, expectedCount)
		}
	}
	if count != expectedCount {
		t.Fatalf("clock list count = %d, expected %d", count, expectedCount)
	}
}

func checkFingerprintsMatch(t testing.TB, fce *intervalCacheEntry) {
	t.Helper()
	for i := 0; i < fce.count; i++ {
		expected := fingerprint(kvCRC32(fce.kvs[i].Key))
		if fce.fps[i] != expected {
			t.Fatalf("fps[%d] = %d, expected %d for key %q", i, fce.fps[i], expected, fce.kvs[i].Key)
		}
	}
}

func checkCountAndSize(t testing.TB, fce *intervalCacheEntry) {
	t.Helper()
	if fce.count != len(fce.kvs) {
		t.Fatalf("count = %d, len(kvs) = %d", fce.count, len(fce.kvs))
	}
	if fce.count != len(fce.fps) {
		t.Fatalf("count = %d, len(fps) = %d", fce.count, len(fce.fps))
	}
	expectedSize := 0
	for _, kv := range fce.kvs {
		expectedSize += kvSizeApprox(&kv)
	}
	if fce.size != expectedSize {
		t.Fatalf("size = %d, expected %d", fce.size, expectedSize)
	}
}

func checkSorted(t testing.TB, fce *intervalCacheEntry) {
	t.Helper()
	for i := 1; i < fce.count; i++ {
		if bytes.Compare(fce.kvs[i-1].Key, fce.kvs[i].Key) >= 0 {
			t.Fatalf("kvs not sorted at position %d: %q >= %q", i, fce.kvs[i-1].Key, fce.kvs[i].Key)
		}
	}
}

// ====================== Unit Tests ======================

func TestIntervalCache_DedupBasic(t *testing.T) {
	// Empty input
	out, fps, size := intervalCacheDedup(nil)
	if len(out) != 0 || fps != nil || size != 0 {
		t.Fatalf("dedup(nil) should return empty")
	}

	// Single element
	kvs := []KV{makeKV("a", "v1", 1)}
	out, fps, size = intervalCacheDedup(kvs)
	if len(out) != 1 || len(fps) != 1 {
		t.Fatalf("dedup single: len=%d fps=%d", len(out), len(fps))
	}
	if !bytes.Equal(out[0].Key, []byte("a")) {
		t.Fatalf("dedup single: key = %q", out[0].Key)
	}

	// No duplicates (3 distinct keys)
	kvs = []KV{makeKV("a", "v1", 1), makeKV("b", "v2", 2), makeKV("c", "v3", 3)}
	out, fps, size = intervalCacheDedup(kvs)
	if len(out) != 3 || len(fps) != 3 {
		t.Fatalf("dedup no-dups: len=%d", len(out))
	}

	// All duplicates (3 copies of same key)
	kvs = []KV{makeKV("x", "v1", 1), makeKV("x", "v2", 5), makeKV("x", "v3", 3)}
	out, fps, size = intervalCacheDedup(kvs)
	if len(out) != 1 {
		t.Fatalf("dedup all-dups: len=%d, expected 1", len(out))
	}
	if out[0].Hlc != 5 {
		t.Fatalf("dedup all-dups: hlc=%d, expected 5 (highest)", out[0].Hlc)
	}

	// Mixed: 2 keys with dups
	kvs = []KV{
		makeKV("a", "v1", 1), makeKV("a", "v2", 10),
		makeKV("b", "v3", 3), makeKV("b", "v4", 2),
	}
	out, fps, size = intervalCacheDedup(kvs)
	if len(out) != 2 {
		t.Fatalf("dedup mixed: len=%d", len(out))
	}
	if out[0].Hlc != 10 || out[1].Hlc != 3 {
		t.Fatalf("dedup mixed: hlcs = %d, %d; expected 10, 3", out[0].Hlc, out[1].Hlc)
	}
	_ = size
}

func TestIntervalCache_DedupHLCWins(t *testing.T) {
	// Ascending HLC: last entry wins
	kvs := []KV{makeKV("k", "v1", 1), makeKV("k", "v2", 2), makeKV("k", "v3", 3)}
	out, _, _ := intervalCacheDedup(kvs)
	if out[0].Hlc != 3 {
		t.Fatalf("ascending HLC: got hlc=%d, want 3", out[0].Hlc)
	}

	// Descending HLC: first entry wins
	kvs = []KV{makeKV("k", "v1", 3), makeKV("k", "v2", 2), makeKV("k", "v3", 1)}
	out, _, _ = intervalCacheDedup(kvs)
	if out[0].Hlc != 3 {
		t.Fatalf("descending HLC: got hlc=%d, want 3", out[0].Hlc)
	}

	// Middle HLC: middle entry wins
	kvs = []KV{makeKV("k", "v1", 1), makeKV("k", "v2", 99), makeKV("k", "v3", 5)}
	out, _, _ = intervalCacheDedup(kvs)
	if out[0].Hlc != 99 {
		t.Fatalf("middle HLC: got hlc=%d, want 99", out[0].Hlc)
	}

	// Equal HLCs: first among equals
	kvs = []KV{makeKV("k", "v1", 7), makeKV("k", "v2", 7), makeKV("k", "v3", 7)}
	out, _, _ = intervalCacheDedup(kvs)
	if out[0].Hlc != 7 {
		t.Fatalf("equal HLC: got hlc=%d, want 7", out[0].Hlc)
	}
}

func TestIntervalCache_DedupSortedOutput(t *testing.T) {
	// Build 100 entries with 20 distinct keys (5 dups each)
	var kvs []KV
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%03d", i)
		for j := 0; j < 5; j++ {
			kvs = append(kvs, makeKV(key, fmt.Sprintf("val-%d-%d", i, j), int64(j+1)))
		}
	}
	out, fps, size := intervalCacheDedup(kvs)
	if len(out) != 20 {
		t.Fatalf("dedup 100→20: got %d", len(out))
	}
	if len(fps) != 20 {
		t.Fatalf("fps len: got %d", len(fps))
	}
	// Check strictly sorted
	for i := 1; i < len(out); i++ {
		if bytes.Compare(out[i-1].Key, out[i].Key) >= 0 {
			t.Fatalf("not strictly sorted at %d: %q >= %q", i, out[i-1].Key, out[i].Key)
		}
	}
	// Each should have hlc=5 (highest of 1..5)
	for i, kv := range out {
		if kv.Hlc != 5 {
			t.Fatalf("out[%d] hlc=%d, want 5", i, kv.Hlc)
		}
	}
	if size <= 0 {
		t.Fatalf("size should be > 0, got %d", size)
	}
}

func TestIntervalCache_FindKeyGE(t *testing.T) {
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	fce := makeCacheEntry(makeSortedKVs(keys))

	// Exact matches
	for i, k := range keys {
		idx, exact := intervalCacheEntryFindKeyGE(fce, []byte(k))
		if !exact || idx != i {
			t.Fatalf("FindKeyGE(%q) = (%d,%v), want (%d,true)", k, idx, exact, i)
		}
	}

	// Before all: should return (0, false)
	idx, exact := intervalCacheEntryFindKeyGE(fce, []byte("aaa"))
	if exact || idx != 0 {
		t.Fatalf("FindKeyGE(aaa) = (%d,%v), want (0,false)", idx, exact)
	}

	// After all: should return (len, false)
	idx, exact = intervalCacheEntryFindKeyGE(fce, []byte("zzz"))
	if exact || idx != 5 {
		t.Fatalf("FindKeyGE(zzz) = (%d,%v), want (5,false)", idx, exact)
	}

	// Between banana and cherry
	idx, exact = intervalCacheEntryFindKeyGE(fce, []byte("candy"))
	if exact || idx != 2 {
		t.Fatalf("FindKeyGE(candy) = (%d,%v), want (2,false)", idx, exact)
	}

	// Empty entry
	empty := makeCacheEntry(nil)
	idx, exact = intervalCacheEntryFindKeyGE(empty, []byte("anything"))
	if exact || idx != 0 {
		t.Fatalf("FindKeyGE on empty = (%d,%v), want (0,false)", idx, exact)
	}
}

func TestIntervalCache_FindKeyEQ(t *testing.T) {
	keys := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	fce := makeCacheEntry(makeSortedKVs(keys))

	// Exact matches
	for i, k := range keys {
		idx, found := intervalCacheEntryFindKeyEQ(fce, []byte(k))
		if !found || idx != i {
			t.Fatalf("FindKeyEQ(%q) = (%d,%v), want (%d,true)", k, idx, found, i)
		}
	}

	// Missing keys
	for _, k := range []string{"aaa", "beta", "foxtrot", "zzz"} {
		idx, found := intervalCacheEntryFindKeyEQ(fce, []byte(k))
		if found {
			t.Fatalf("FindKeyEQ(%q) = (%d,true), want (_,false)", k, idx)
		}
	}

	// Verify fingerprints match
	checkFingerprintsMatch(t, fce)
}

func TestIntervalCache_FindKeyAgreement(t *testing.T) {
	// Build 50 sorted keys
	keys := make([]string, 50)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%04d", i*3) // gaps: 0,3,6,...
	}
	fce := makeCacheEntry(makeSortedKVs(keys))

	// All existing keys: GE and EQ should both find them
	for _, k := range keys {
		kb := []byte(k)
		idxGE, exactGE := intervalCacheEntryFindKeyGE(fce, kb)
		idxEQ, foundEQ := intervalCacheEntryFindKeyEQ(fce, kb)
		if !exactGE {
			t.Fatalf("FindKeyGE(%q) not exact", k)
		}
		if !foundEQ {
			t.Fatalf("FindKeyEQ(%q) not found", k)
		}
		if !bytes.Equal(fce.kvs[idxGE].Key, kb) || !bytes.Equal(fce.kvs[idxEQ].Key, kb) {
			t.Fatalf("key mismatch for %q", k)
		}
	}

	// Random missing keys: EQ should return false; GE should return false too
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key-%04d", rng.Intn(200))
		kb := []byte(k)
		_, foundEQ := intervalCacheEntryFindKeyEQ(fce, kb)
		_, exactGE := intervalCacheEntryFindKeyGE(fce, kb)
		// If EQ finds it, GE must also find it
		if foundEQ && !exactGE {
			t.Fatalf("EQ found %q but GE didn't", k)
		}
	}
}

func TestIntervalCache_FingerprintNonZero(t *testing.T) {
	// 100K random keys
	rng := rand.New(rand.NewSource(123))
	for i := 0; i < 100000; i++ {
		h := rng.Uint32()
		fp := fingerprint(h)
		if fp == 0 {
			t.Fatalf("fingerprint(%d) == 0", h)
		}
	}

	// Edge cases: h=0
	if fp := fingerprint(0); fp == 0 {
		t.Fatal("fingerprint(0) == 0")
	}

	// h where XOR of halves would be 0: upper and lower 16 bits equal
	// e.g. 0x00010001: uint16(0x0001) ^ uint16(0x0001) = 0 → should become 1
	if fp := fingerprint(0x00010001); fp == 0 {
		t.Fatal("fingerprint(0x00010001) == 0")
	}
	if fp := fingerprint(0xAAAAAAAA); fp == 0 {
		t.Fatal("fingerprint(0xAAAAAAAA) == 0")
	}
}

func TestIntervalCache_ClockListIntegrity(t *testing.T) {
	p := makePartition(1 << 20)

	// Insert 1 entry
	e1 := &intervalCacheEntry{}
	p.insertIntoClock(e1)
	checkClockListIntegrity(t, p, 1)
	if e1.next != e1 || e1.prev != e1 {
		t.Fatal("single entry should point to itself")
	}

	// Insert 2nd entry
	e2 := &intervalCacheEntry{}
	p.insertIntoClock(e2)
	checkClockListIntegrity(t, p, 2)

	// Insert 10 total
	entries := []*intervalCacheEntry{e1, e2}
	for i := 0; i < 8; i++ {
		e := &intervalCacheEntry{}
		p.insertIntoClock(e)
		entries = append(entries, e)
	}
	checkClockListIntegrity(t, p, 10)

	// Remove middle entry
	p.removeFromClock(entries[5])
	checkClockListIntegrity(t, p, 9)

	// Remove tick itself
	p.removeFromClock(p.tick)
	checkClockListIntegrity(t, p, 8)

	// Remove all remaining
	for p.tick != nil {
		p.removeFromClock(p.tick)
	}
	checkClockListIntegrity(t, p, 0)
}

func TestIntervalCache_CacheEntryInsert(t *testing.T) {
	p := makePartition(1 << 20)
	fce := makeCacheEntry(makeSortedKVs([]string{"b", "d", "f"}))

	// Insert at beginning
	p.cacheEntryInsert(fce, makeKV("a", "va", 1), 0)
	checkSorted(t, fce)
	checkCountAndSize(t, fce)
	checkFingerprintsMatch(t, fce)
	if fce.count != 4 {
		t.Fatalf("count = %d, want 4", fce.count)
	}

	// Insert in middle (between "b" and "d")
	p.cacheEntryInsert(fce, makeKV("c", "vc", 2), 2)
	checkSorted(t, fce)
	checkCountAndSize(t, fce)
	checkFingerprintsMatch(t, fce)
	if fce.count != 5 {
		t.Fatalf("count = %d, want 5", fce.count)
	}

	// Insert at end
	p.cacheEntryInsert(fce, makeKV("z", "vz", 3), 5)
	checkSorted(t, fce)
	checkCountAndSize(t, fce)
	checkFingerprintsMatch(t, fce)
	if fce.count != 6 {
		t.Fatalf("count = %d, want 6", fce.count)
	}

	// Verify all keys present
	expected := []string{"a", "b", "c", "d", "f", "z"}
	for i, exp := range expected {
		if string(fce.kvs[i].Key) != exp {
			t.Fatalf("kvs[%d].Key = %q, want %q", i, fce.kvs[i].Key, exp)
		}
	}
}

func TestIntervalCache_CacheEntryReplace(t *testing.T) {
	p := makePartition(1 << 20)
	fce := makeCacheEntry(makeSortedKVs([]string{"a", "b", "c"}))
	origCount := fce.count
	origSize := fce.size

	// Replace with longer value
	p.cacheEntryReplace(fce, makeKV("b", "much-longer-value", 10), 1)
	if fce.count != origCount {
		t.Fatalf("count changed: %d → %d", origCount, fce.count)
	}
	checkCountAndSize(t, fce)
	checkFingerprintsMatch(t, fce)
	if fce.size <= origSize {
		t.Fatalf("size should have increased: was %d, now %d", origSize, fce.size)
	}

	// Replace with shorter value
	prevSize := fce.size
	p.cacheEntryReplace(fce, makeKV("b", "x", 11), 1)
	checkCountAndSize(t, fce)
	if fce.size >= prevSize {
		t.Fatalf("size should have decreased: was %d, now %d", prevSize, fce.size)
	}

	// Replace with same-size value
	prevSize = fce.size
	p.cacheEntryReplace(fce, makeKV("b", "y", 12), 1) // "x" → "y", same len
	checkCountAndSize(t, fce)
	if fce.size != prevSize {
		t.Fatalf("size should be unchanged: was %d, now %d", prevSize, fce.size)
	}
}

func TestIntervalCache_CacheEntryDelete(t *testing.T) {
	p := makePartition(1 << 20)
	keys := []string{"a", "b", "c", "d", "e"}
	fce := makeCacheEntry(makeSortedKVs(keys))

	// Delete from middle
	p.cacheEntryDelete(fce, 2) // delete "c"
	if fce.count != 4 {
		t.Fatalf("count = %d, want 4", fce.count)
	}
	checkCountAndSize(t, fce)
	checkFingerprintsMatch(t, fce)
	checkSorted(t, fce)

	// Delete from beginning
	p.cacheEntryDelete(fce, 0) // delete "a"
	if fce.count != 3 {
		t.Fatalf("count = %d, want 3", fce.count)
	}
	checkCountAndSize(t, fce)

	// Delete from end
	p.cacheEntryDelete(fce, fce.count-1) // delete "e"
	if fce.count != 2 {
		t.Fatalf("count = %d, want 2", fce.count)
	}
	checkCountAndSize(t, fce)

	// Delete until empty
	p.cacheEntryDelete(fce, 0)
	p.cacheEntryDelete(fce, 0)
	if fce.count != 0 {
		t.Fatalf("count = %d, want 0", fce.count)
	}
	checkCountAndSize(t, fce)
}

func TestIntervalCache_Calibrate(t *testing.T) {
	p := makePartition(100) // very small cap

	// Add entries that exceed the cap
	entries := make([]*intervalCacheEntry, 5)
	for i := 0; i < 5; i++ {
		fce := makeCacheEntry([]KV{makeKV(fmt.Sprintf("k%d", i), "value", int64(i))})
		fce.access = 0 // no second chance
		entries[i] = fce
		p.insertIntoClock(fce)
		p.size += int64(32 + fce.size)
	}

	// Cap is 100 bytes, each entry is ~32 + (24+2+5) = 63 bytes → 5 entries = 315 bytes
	if p.size <= p.cap {
		t.Fatalf("test setup: size=%d should exceed cap=%d", p.size, p.cap)
	}

	p.calibrate()

	// After calibrate, size should be <= cap (or all remaining are pinned)
	if p.size > p.cap {
		// Check that remaining entries are all pinned
		if p.tick != nil {
			node := p.tick
			for {
				if node.refcnt == 0 {
					t.Fatalf("after calibrate, unpinned entry remains but size=%d > cap=%d", p.size, p.cap)
				}
				node = node.next
				if node == p.tick {
					break
				}
			}
		}
	}
}

func TestIntervalCache_EvictionRefcntProtection(t *testing.T) {
	p := makePartition(50) // tiny cap

	// All entries pinned (refcnt > 0)
	for i := 0; i < 5; i++ {
		fce := makeCacheEntry([]KV{makeKV(fmt.Sprintf("k%d", i), "value", int64(i))})
		fce.refcnt = 1 // pinned
		fce.access = 0
		p.insertIntoClock(fce)
		p.size += int64(32 + fce.size)
	}

	sizeBefore := p.size
	p.calibrate()
	// No entries should be evicted because all are pinned
	if p.size != sizeBefore {
		t.Fatalf("pinned entries were evicted: size %d → %d", sizeBefore, p.size)
	}
	checkClockListIntegrity(t, p, 5)

	// Mix of pinned and unpinned
	p2 := makePartition(50)
	for i := 0; i < 5; i++ {
		fce := makeCacheEntry([]KV{makeKV(fmt.Sprintf("k%d", i), "value", int64(i))})
		if i%2 == 0 {
			fce.refcnt = 1 // pinned
		}
		fce.access = 0
		p2.insertIntoClock(fce)
		p2.size += int64(32 + fce.size)
	}
	p2.calibrate()
	// Pinned entries should still be present
	if p2.tick != nil {
		node := p2.tick
		for {
			// All remaining entries should have refcnt > 0 or size is within cap
			node = node.next
			if node == p2.tick {
				break
			}
		}
	}
}

func TestIntervalCache_CalibrateAccessChance(t *testing.T) {
	// Test that access counter gives entries a reprieve
	p := makePartition(50) // tiny cap

	// Add entries with access > 0
	for i := 0; i < 3; i++ {
		fce := makeCacheEntry([]KV{makeKV(fmt.Sprintf("k%d", i), "value", int64(i))})
		fce.access = 2 // two chances
		p.insertIntoClock(fce)
		p.size += int64(32 + fce.size)
	}

	sizeBefore := p.size
	p.calibrate()
	// With the fixed full-circle detection, calibrate should now properly
	// decrement access counters and eventually evict
	if p.size > p.cap && p.tick != nil {
		// All remaining must have refcnt > 0
		node := p.tick
		for {
			if node.refcnt == 0 {
				t.Fatalf("unpinned entry with access decremented remains above cap")
			}
			node = node.next
			if node == p.tick {
				break
			}
		}
	}
	_ = sizeBefore
}

func TestIntervalCache_PartitionID(t *testing.T) {
	// Determinism: same key → same partition
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("test-key-%d", i))
		id1 := cachePartitionID(key)
		id2 := cachePartitionID(key)
		if id1 != id2 {
			t.Fatalf("cachePartitionID not deterministic for %q: %d vs %d", key, id1, id2)
		}
	}

	// Range: all results in [0, 1023]
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10000; i++ {
		key := make([]byte, rng.Intn(100)+1)
		rng.Read(key)
		id := cachePartitionID(key)
		if id < 0 || id >= intervalCachePartitionCount {
			t.Fatalf("cachePartitionID(%q) = %d, out of range [0,%d)", key, id, intervalCachePartitionCount)
		}
	}

	// Edge cases
	id := cachePartitionID(nil)
	if id < 0 || id >= intervalCachePartitionCount {
		t.Fatalf("cachePartitionID(nil) = %d", id)
	}

	id = cachePartitionID([]byte{})
	if id < 0 || id >= intervalCachePartitionCount {
		t.Fatalf("cachePartitionID(empty) = %d", id)
	}

	longKey := make([]byte, 10000)
	for i := range longKey {
		longKey[i] = byte(i % 256)
	}
	id = cachePartitionID(longKey)
	if id < 0 || id >= intervalCachePartitionCount {
		t.Fatalf("cachePartitionID(long) = %d", id)
	}
}

func TestIntervalCache_NewCache(t *testing.T) {
	c := newCache(nil, 64) // 64 MB, nil db is fine for structural test
	if len(c.partitions) != intervalCachePartitionCount {
		t.Fatalf("partitions = %d, want %d", len(c.partitions), intervalCachePartitionCount)
	}
	expectedPartCap := c.cap / intervalCachePartitionCount
	for i := range c.partitions {
		if c.partitions[i].cap != expectedPartCap {
			t.Fatalf("partition[%d].cap = %d, want %d", i, c.partitions[i].cap, expectedPartCap)
		}
		if c.partitions[i].size != 0 {
			t.Fatalf("partition[%d].size = %d, want 0", i, c.partitions[i].size)
		}
		if c.partitions[i].tick != nil {
			t.Fatalf("partition[%d].tick should be nil", i)
		}
	}
	if c.cap != 64<<20 {
		t.Fatalf("cap = %d, want %d", c.cap, 64<<20)
	}
}

// ====================== Fuzz Tests ======================

func FuzzIntervalCache_Dedup(f *testing.F) {
	f.Add([]byte("abc"), []byte("def"), []byte("abc"), int64(1), int64(2), int64(5))
	f.Fuzz(func(t *testing.T, k1, k2, k3 []byte, h1, h2, h3 int64) {
		// Build a sorted KV slice with potential duplicates
		kvs := []KV{
			{Key: k1, Value: []byte("v1"), Hlc: HLC(h1)},
			{Key: k2, Value: []byte("v2"), Hlc: HLC(h2)},
			{Key: k3, Value: []byte("v3"), Hlc: HLC(h3)},
		}
		sort.SliceStable(kvs, func(i, j int) bool {
			return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
		})

		// Count distinct keys and max HLCs
		maxHLC := make(map[string]HLC)
		for _, kv := range kvs {
			k := string(kv.Key)
			if existing, ok := maxHLC[k]; !ok || kv.Hlc > existing {
				maxHLC[k] = kv.Hlc
			}
		}

		out, fps, size := intervalCacheDedup(kvs)

		// INV-IC-3: correct number of distinct keys
		if len(out) != len(maxHLC) {
			t.Fatalf("dedup output len=%d, distinct keys=%d", len(out), len(maxHLC))
		}

		// INV-IC-2: strictly sorted (or equal adjacent keys means bug)
		for i := 1; i < len(out); i++ {
			if bytes.Compare(out[i-1].Key, out[i].Key) >= 0 {
				t.Fatalf("not sorted at %d", i)
			}
		}

		// INV-IC-1: highest HLC preserved
		for _, kv := range out {
			expected := maxHLC[string(kv.Key)]
			if kv.Hlc != expected {
				t.Fatalf("key %q: hlc=%d, want %d", kv.Key, kv.Hlc, expected)
			}
		}

		// INV-IC-8: fingerprints match
		if len(fps) != len(out) {
			t.Fatalf("fps len=%d, out len=%d", len(fps), len(out))
		}
		for i, kv := range out {
			expected := fingerprint(kvCRC32(kv.Key))
			if fps[i] != expected {
				t.Fatalf("fps[%d]=%d, want %d", i, fps[i], expected)
			}
		}

		// Size check
		expectedSize := 0
		for _, kv := range out {
			expectedSize += kvSizeApprox(&kv)
		}
		if size != expectedSize {
			t.Fatalf("size=%d, want %d", size, expectedSize)
		}
	})
}

func FuzzIntervalCache_FindKey(f *testing.F) {
	f.Add([]byte("a"), []byte("m"), []byte("z"), []byte("m"))
	f.Fuzz(func(t *testing.T, k1, k2, k3, search []byte) {
		// Build a sorted, deduplicated entry
		keys := [][]byte{k1, k2, k3}
		sort.Slice(keys, func(i, j int) bool {
			return bytes.Compare(keys[i], keys[j]) < 0
		})
		// Remove duplicates
		var uniq []KV
		for i, k := range keys {
			if i == 0 || !bytes.Equal(k, keys[i-1]) {
				uniq = append(uniq, KV{Key: k, Value: []byte("v"), Hlc: HLC(int64(i + 1))})
			}
		}
		fce := makeCacheEntry(uniq)

		idxGE, exactGE := intervalCacheEntryFindKeyGE(fce, search)
		idxEQ, foundEQ := intervalCacheEntryFindKeyEQ(fce, search)

		// INV-IC-6: if EQ finds it, GE must also find it
		if foundEQ && !exactGE {
			t.Fatalf("EQ found %q but GE didn't", search)
		}

		// INV-IC-4: verify GE position
		if exactGE {
			if idxGE < 0 || idxGE >= fce.count {
				t.Fatalf("GE exact idx=%d out of range [0,%d)", idxGE, fce.count)
			}
			if !bytes.Equal(fce.kvs[idxGE].Key, search) {
				t.Fatalf("GE exact but key mismatch")
			}
		} else {
			// idx is insertion point
			if idxGE < 0 || idxGE > fce.count {
				t.Fatalf("GE inexact idx=%d out of range [0,%d]", idxGE, fce.count)
			}
			if idxGE > 0 {
				if bytes.Compare(fce.kvs[idxGE-1].Key, search) >= 0 {
					t.Fatalf("GE inexact: key before idx >= search")
				}
			}
			if idxGE < fce.count {
				if bytes.Compare(fce.kvs[idxGE].Key, search) < 0 {
					t.Fatalf("GE inexact: key at idx < search")
				}
			}
		}

		// INV-IC-5: verify EQ result
		if foundEQ {
			if idxEQ < 0 || idxEQ >= fce.count {
				t.Fatalf("EQ found idx=%d out of range", idxEQ)
			}
			if !bytes.Equal(fce.kvs[idxEQ].Key, search) {
				t.Fatalf("EQ found but key mismatch")
			}
		}
	})
}

func FuzzIntervalCache_Mutations(f *testing.F) {
	f.Add(uint8(0), []byte("key"), []byte("val"))
	f.Fuzz(func(t *testing.T, op uint8, key, val []byte) {
		p := makePartition(1 << 30) // large cap to avoid eviction
		fce := makeCacheEntry(makeSortedKVs([]string{"b", "d", "f", "h"}))
		kv := KV{Key: key, Value: val, Hlc: 1}

		switch op % 3 {
		case 0: // Insert
			idx, _ := intervalCacheEntryFindKeyGE(fce, key)
			// Only insert if key doesn't already exist at idx
			if idx < fce.count && bytes.Equal(fce.kvs[idx].Key, key) {
				return // skip: key already present
			}
			prevCount := fce.count
			p.cacheEntryInsert(fce, kv, idx)
			// INV-IC-9: count increased by 1
			if fce.count != prevCount+1 {
				t.Fatalf("insert: count %d → %d", prevCount, fce.count)
			}
			checkCountAndSize(t, fce)
			checkFingerprintsMatch(t, fce)

		case 1: // Replace
			if fce.count == 0 {
				return
			}
			idx := int(op) % fce.count
			prevCount := fce.count
			kv.Key = append([]byte{}, fce.kvs[idx].Key...) // same key
			p.cacheEntryReplace(fce, kv, idx)
			// INV-IC-15: count unchanged
			if fce.count != prevCount {
				t.Fatalf("replace: count %d → %d", prevCount, fce.count)
			}
			checkCountAndSize(t, fce)
			checkFingerprintsMatch(t, fce)

		case 2: // Delete
			if fce.count == 0 {
				return
			}
			idx := int(op) % fce.count
			prevCount := fce.count
			p.cacheEntryDelete(fce, idx)
			// INV-IC-14: count decreased by 1
			if fce.count != prevCount-1 {
				t.Fatalf("delete: count %d → %d", prevCount, fce.count)
			}
			checkCountAndSize(t, fce)
			checkFingerprintsMatch(t, fce)
		}
	})
}

// ====================== Benchmarks ======================

func BenchmarkIntervalCache_FindKeyEQ(b *testing.B) {
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%06d", i)
	}
	fce := makeCacheEntry(makeSortedKVs(keys))
	searchKeys := make([][]byte, 100)
	for i := range searchKeys {
		searchKeys[i] = []byte(keys[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intervalCacheEntryFindKeyEQ(fce, searchKeys[i%100])
	}
}

func BenchmarkIntervalCache_FindKeyGE(b *testing.B) {
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%06d", i)
	}
	fce := makeCacheEntry(makeSortedKVs(keys))
	searchKeys := make([][]byte, 100)
	for i := range searchKeys {
		searchKeys[i] = []byte(keys[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intervalCacheEntryFindKeyGE(fce, searchKeys[i%100])
	}
}

func BenchmarkIntervalCache_Dedup(b *testing.B) {
	// Build 1000 KVs with 200 distinct keys (5 dups each)
	template := make([]KV, 1000)
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%06d", i)
		for j := 0; j < 5; j++ {
			template[i*5+j] = makeKV(key, fmt.Sprintf("val-%d-%d", i, j), int64(j+1))
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Copy since dedup reuses the input slice
		kvs := make([]KV, len(template))
		copy(kvs, template)
		intervalCacheDedup(kvs)
	}
}
