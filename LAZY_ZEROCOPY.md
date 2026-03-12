# Plan: KVcloser API with LAZY_LARGE, LAZY_SMALL, and Error Returns

## Context

The current `Find` API returns `(*KV, bool, bool)` - no error propagation for VLOG I/O failures, redundant `found` bool (nil KV already means not found), and no mechanism for deferred large-value fetching or zero-copy inline values. We want to change the return to `(*KVcloser, bool, error)` where nil KVcloser = not found, and the caller calls `kvc.Close()` when done with the value.

Two orthogonal SearchModifier flags:
- **LAZY_LARGE (128):** Don't auto-fetch VLOG values. User calls `kvc.Fetch()` explicitly.
- **LAZY_SMALL (64):** Zero-copy for inline values via cache pinning. `kvc.Value` aliases cache memory. User MUST call `Close()` to release the pin.

The diff in `diffs.txt` has partial work: the signature is changed, KVcloser struct exists with cache-pinning fields and Close(), but auto-fetch, `Fetch()`, and LAZY_SMALL are not yet implemented.

## Key Design Decisions

1. **Nil KVcloser = not found.** Removes the need for `found` bool. The `exact` bool is retained for GTE/LTE/GT/LT queries where the user needs to distinguish exact vs. neighbor match.

2. **Error return** propagates VLOG I/O errors (auto-fetch when not LAZY_LARGE) and any future I/O errors.

3. **KVcloser.Close()** is a no-op on nil receiver (safe to defer unconditionally). After Close, `Value` is set to nil to catch use-after-close. Key and Vptr remain accessible (owned copies, not cache-borrowed).

4. **LAZY_LARGE and LAZY_SMALL are orthogonal.** They can be combined with bitwise OR:
   - Neither: inline values copied, large values auto-fetched (safest, most convenient)
   - `LAZY_LARGE` only: inline values copied, large values deferred
   - `LAZY_SMALL` only: inline values zero-copy (cache-pinned), large values auto-fetched
   - `LAZY_SMALL|LAZY_LARGE`: inline values zero-copy, large values deferred (fastest)

5. **LAZY_SMALL cache-pinning uses a re-lookup approach.** After the iterator finds the result, we release the iterator and do a direct sparse-index lookup on the result key to get a pinned cache entry. This avoids modifying the hot iteration path (no changes to prefetchSpan or servePrefetch). The re-lookup is a guaranteed cache hit since the entry was just accessed. If the result came from a memtable (not FlexSpace), we fall back to copying (can't pin memtable data).

6. **tx.Find (WriteTx/ReadOnlyTx):** Keep existing `(*KV, bool, bool)` signature for now. These run within transaction callbacks that hold the lock, so the returned KV is valid for the callback's lifetime. Changing these is a separate follow-up.

## User-Facing API Examples

```go
// Example 1: Normal Find (auto-fetches large values, copies inline values)
kvc, exact, err := db.Find(Exact, "needle")
_ = exact // on an Exact Find call, if kvc != nil, we know exact will be true.
panicOn(err)
if kvc != nil {
   // found exact match! because Exact was requested.
   // large values automatically pulled in, since LAZY_LARGE not specified.
   fmt.Println(string(kvc.Value))
   kvc.Close()
}

// Example 2: LAZY_LARGE (deferred large value fetch)
kvc, _, err := db.Find(Exact|LAZY_LARGE, "needle")
panicOn(err)
if kvc != nil {
   err := kvc.Fetch() // fetch large value if value is large. else no-op.
   panicOn(err)
   // here we are sure we have kvc.Value
   fmt.Println(string(kvc.Value))
   kvc.Close()
}

// Example 3: LAZY_SMALL (zero-copy inline values, auto-fetch large)
kvc, _, err := db.Find(Exact|LAZY_SMALL, "needle")
panicOn(err)
if kvc != nil {
   // kvc.Value aliases cache memory - zero-copy!
   // MUST Close() to release cache pin.
   processValue(kvc.Value) // use value before Close
   kvc.Close()             // releases cache entry pin
}

// Example 4: LAZY_SMALL|LAZY_LARGE (maximum performance)
kvc, _, err := db.Find(GTE|LAZY_SMALL|LAZY_LARGE, "prefix")
panicOn(err)
if kvc != nil {
   if kvc.Large() {
       err := kvc.Fetch() // only fetch if we actually need the large value
       panicOn(err)
   }
   processValue(kvc.Value)
   kvc.Close()
}
```

## Changes

### Step 1: Add LAZY_SMALL constant (db.go, SearchModifier consts)

```go
const (
    Exact      SearchModifier = 0
    GTE        SearchModifier = 1
    LTE        SearchModifier = 2
    GT         SearchModifier = 3
    LT         SearchModifier = 4

    // LAZY_SMALL requests zero-copy return of inline values.
    // The returned KVcloser.Value aliases interval cache memory.
    // The caller MUST call Close() to release the cache pin.
    // If the result came from a memtable (not yet flushed),
    // Value is copied as usual (best-effort zero-copy).
    LAZY_SMALL    SearchModifier = 64

    // LAZY_LARGE requests that large values (stored in VLOG)
    // not be fetched automatically. The caller must call
    // kvc.Fetch() explicitly when they want the value bytes.
    LAZY_LARGE SearchModifier = 128
)
```

### Step 2: Complete KVcloser type (db.go ~line 2470)

Already done in diff. Verify:
```go
type KVcloser struct {
    KV
    partition *intervalCachePartition // nil when no pin needed
    entry     *intervalCacheEntry     // nil when no pin needed
    db        *FlexDB
}

func (s *KVcloser) Close() {
    if s == nil {
        return
    }
    if s.entry != nil {
        s.partition.releaseEntry(s.entry)
        s.partition = nil
        s.entry = nil
    }
    s.Value = nil // prevent use-after-close
}
```

### Step 3: Add KVcloser.Fetch() method (db.go, after Close)

```go
// Fetch retrieves the large value from the VLOG if this KV has
// a VPtr (kvc.Large() == true). For inline values, Fetch is a
// no-op. After Fetch returns nil error, kvc.Value holds the bytes.
// Fetch is only needed when LAZY_LARGE was used in the Find call.
func (s *KVcloser) Fetch() error {
    if s == nil {
        return nil
    }
    if !s.HasVPtr() {
        return nil // inline value already present
    }
    val, err := s.db.FetchLarge(&s.KV)
    if err != nil {
        return err
    }
    s.Value = val
    return nil
}
```

Note: `db.FetchLarge` acquires `topMutRW.RLock` internally, so Fetch is goroutine-safe and works outside any transaction.

### Step 4: Add findBuildKVZeroCopy helper (db.go)

This is the key new function for LAZY_SMALL. It does a direct sparse-index lookup on a known-to-exist key and returns a KVcloser that pins the cache entry.

```go
// findBuildKVZeroCopy returns a KVcloser whose Value aliases
// interval cache memory (zero-copy). The cache entry is pinned
// via refcnt; the caller must call Close() to release.
// Caller must hold topMutRW.RLock() or topMutRW.Lock().
//
// Returns nil if the key is not found in FlexSpace (e.g., it
// was in a memtable, or was a tombstone). Caller should fall
// back to the copy path.
func (db *FlexDB) findBuildKVZeroCopy(key string) *KVcloser {
    if db.tree.root == nil {
        return nil
    }
    var nh memSparseIndexTreeHandler
    db.tree.findAnchorPos(key, &nh)
    anchor := nh.node.anchors[nh.idx]
    if anchor == nil || anchor.psize == 0 {
        return nil
    }
    anchorLoff := uint64(anchor.loff + nh.shift)
    partition := db.cache.getPartition(anchor)
    fce := partition.getEntry(anchor, anchorLoff, db)

    idx, ok := intervalCacheEntryFindKeyEQ(fce, key)
    if !ok || fce.kvs[idx].isTombstone() {
        partition.releaseEntry(fce)
        return nil
    }
    // Transfer cache entry ownership to KVcloser (don't release).
    // fce.kvs[idx] is a value copy of the KV struct (64B), but
    // the .Value []byte slice header still points into cache memory.
    return &KVcloser{
        KV:        fce.kvs[idx],
        partition: partition,
        entry:     fce,
        db:        db,
    }
}
```

**Why this is safe:**
- `getEntry` bumps `refcnt`, preventing eviction of this cache entry
- We transfer that ref to the KVcloser (don't call releaseEntry)
- `KVcloser.Close()` calls `releaseEntry` to decrement refcnt
- Go strings are immutable, so `kvc.Key` is safe even after cache eviction
- `kvc.Value` aliases cache memory, protected by the refcnt pin
- The re-lookup is a guaranteed cache hit (entry was just accessed by the iterator)

### Step 5: Update Find to handle both flags (db.go ~line 2444)

```go
func (db *FlexDB) Find(smod SearchModifier, key string) (kvc *KVcloser, exact bool, err error) {
    db.topMutRW.RLock()
    defer db.topMutRW.RUnlock()

    it := &Iter{db: db}
    lazyLarge := (smod & LAZY_LARGE != 0)
    lazyVal := (smod & LAZY_SMALL != 0)
    if lazyLarge {
        it.lazyLarge = true
        smod &^= LAZY_LARGE
    }
    smod &^= LAZY_SMALL // strip LAZY_SMALL before passing to findSeekIter

    var found bool
    found, exact = findSeekIter(it, smod, key)
    if found {
        zc := findBuildKV(it)
        resultKey := zc.Key

        // Release iterator state early - we have what we need.
        it.releaseIterState()

        // LAZY_SMALL path: try zero-copy via cache pinning.
        // Only works for inline values from FlexSpace (not memtable).
        if lazyVal && !zc.HasVPtr() && zc.Value != nil {
            kvc = db.findBuildKVZeroCopy(resultKey)
            if kvc != nil {
                // Zero-copy success. Handle auto-fetch of large values
                // (not applicable here since !HasVPtr, but for consistency).
                return
            }
            // Fallback: key was in memtable or edge case. Copy below.
        }

        // Standard path: copy inline value, auto-fetch large value.
        owned := KV{Vptr: zc.Vptr, Hlc: zc.Hlc}
        owned.Key = resultKey
        if !zc.HasVPtr() && zc.Value != nil {
            owned.Value = append([]byte{}, zc.Value...)
        }
        kvc = &KVcloser{KV: owned, db: db}

        // Auto-fetch large value unless LAZY_LARGE was requested
        if !lazyLarge && kvc.HasVPtr() {
            val, fetchErr := db.resolveVPtr(kvc.KV)
            if fetchErr != nil {
                kvc = nil
                err = fetchErr
            } else {
                kvc.Value = val
            }
        }
        return
    }
    it.releaseIterState()
    return
}
```

**Key change from the diff:** `it.releaseIterState()` is called inside the `if found` block (early), then we do the re-lookup for LAZY_SMALL. The `else` path (not found) also calls `releaseIterState`. This avoids using the iterator's state after release.

**Note on `smod &^= LAZY_LARGE` vs `smod -= LAZY_LARGE`:** Using bit-clear (`&^=`) is safer than subtraction - it's idempotent and can't underflow.

### Step 6: Update GetKV (db.go ~line 2493)

Already done in diff:
```go
func (db *FlexDB) GetKV(key string) (kv *KVcloser, err error) {
    kv, _, err = db.Find(Exact, key)
    return
}
```

### Step 7: Update find_test.go

All tests currently use the old `(kv *KV, found, exact bool)` signature. Update each test:

**Pattern replacement:**
```go
// Old:
kv, found, exact := db.Find(Exact, "key003")
if !found || !exact { ... }
if kv.Large() { ... }

// New:
kvc, exact, err := db.Find(Exact, "key003")
panicOn(err)
if kvc == nil || !exact { ... }
if kvc.Large() { ... }
defer kvc.Close() // or explicit Close at end
```

For not-found cases:
```go
// Old:
kv2, found2, exact2 := db.Find(Exact, "key999")
if found2 || exact2 { ... }
if kv2 != nil { ... }

// New:
kvc2, exact2, err2 := db.Find(Exact, "key999")
if err2 != nil { t.Fatal(err2) }
if kvc2 != nil || exact2 { ... }
```

Tests to update (all in find_test.go):
- TestFind_Exact
- TestFind_GTE
- TestFind_GT
- TestFind_LTE
- TestFind_LT
- TestFindIt_IteratorContinuation (uses tx.Find - no change needed)
- TestFind_EmptyDB
- TestFind_AfterSync
- TestFind_KVOwnership
- TestFind_HLCPopulated
- TestFetchLarge_InlineValue (uses db.Find + db.FetchLarge)

### Step 8: Add LAZY_LARGE and LAZY_SMALL tests (find_test.go)

```go
func TestFind_LazyLarge(t *testing.T) {
    db, _ := openTestDB(t, nil)

    bigVal := bytes.Repeat([]byte("x"), 200)
    panicOn(db.Put("bigkey", bigVal))
    db.Sync()

    // Without LAZY_LARGE: value auto-fetched
    kvc, _, err := db.Find(Exact, "bigkey")
    panicOn(err)
    if kvc == nil { t.Fatal("not found") }
    if !bytes.Equal(kvc.Value, bigVal) {
        t.Fatal("value mismatch on auto-fetch")
    }
    kvc.Close()

    // With LAZY_LARGE: value NOT fetched until Fetch()
    kvc2, _, err := db.Find(Exact|LAZY_LARGE, "bigkey")
    panicOn(err)
    if kvc2 == nil { t.Fatal("not found") }
    if kvc2.Value != nil {
        t.Fatal("expected nil Value with LAZY_LARGE before Fetch")
    }
    if !kvc2.Large() {
        t.Fatal("expected Large() == true")
    }
    err = kvc2.Fetch()
    panicOn(err)
    if !bytes.Equal(kvc2.Value, bigVal) {
        t.Fatal("value mismatch after Fetch")
    }
    kvc2.Close()
}

func TestFind_LazyVal(t *testing.T) {
    db, _ := openTestDB(t, nil)

    // Populate and flush to FlexSpace so values are in cache.
    for i := 1; i <= 10; i++ {
        k := fmt.Sprintf("key%03d", i)
        v := fmt.Sprintf("val%03d", i)
        panicOn(db.Put(k, []byte(v)))
    }
    db.Sync()

    // LAZY_SMALL: zero-copy inline value
    kvc, exact, err := db.Find(Exact|LAZY_SMALL, "key005")
    panicOn(err)
    if kvc == nil || !exact { t.Fatal("not found") }
    if string(kvc.Value) != "val005" {
        t.Fatalf("got %q, want val005", kvc.Value)
    }
    // Value aliases cache memory - verify it's valid before Close
    valRef := kvc.Value
    if len(valRef) != 6 { t.Fatal("unexpected length") }
    kvc.Close()
    // After Close, kvc.Value is nil
    if kvc.Value != nil { t.Fatal("Value should be nil after Close") }

    // LAZY_SMALL with GTE: non-exact match
    kvc2, exact2, err := db.Find(GTE|LAZY_SMALL, "key004a")
    panicOn(err)
    if kvc2 == nil { t.Fatal("not found") }
    if exact2 { t.Fatal("should not be exact") }
    if kvc2.Key != "key005" { t.Fatalf("got %q, want key005", kvc2.Key) }
    if string(kvc2.Value) != "val005" {
        t.Fatalf("got %q, want val005", kvc2.Value)
    }
    kvc2.Close()

    // LAZY_SMALL|LAZY_LARGE combined with a large value
    bigVal := bytes.Repeat([]byte("B"), 200)
    panicOn(db.Put("large001", bigVal))
    db.Sync()

    kvc3, _, err := db.Find(Exact|LAZY_SMALL|LAZY_LARGE, "large001")
    panicOn(err)
    if kvc3 == nil { t.Fatal("not found") }
    if !kvc3.Large() { t.Fatal("expected Large") }
    if kvc3.Value != nil { t.Fatal("expected nil Value before Fetch") }
    err = kvc3.Fetch()
    panicOn(err)
    if !bytes.Equal(kvc3.Value, bigVal) {
        t.Fatal("value mismatch after Fetch")
    }
    kvc3.Close()
}
```

### Step 9: Update all callers of db.Find and db.GetKV

Files with callers:
- **`find_test.go`** - ~25 Find() call sites (see Step 7)
- **`vlog_test.go`** - 4 GetKV() call sites (lines 374, 402, 466, 493)
  - Old: `kv, ok := db.GetKV(key)` / `if !ok { ... }`
  - New: `kvc, err := db.GetKV(key)` / `panicOn(err)` / `if kvc == nil { ... }` / `kvc.Close()`

### Step 10: Nil-receiver safety for KVcloser methods

Ensure all KVcloser methods handle nil receiver:
- `Close()` - already handles nil (in diff)
- `Fetch()` - handle nil (return nil)

## Zero-Copy Analysis

### Where copies happen today

1. **Inline values (<=64 bytes):** `append([]byte{}, zc.Value...)` copies from cache memory.
2. **VLOG values (>64 bytes):** `vlog.read()` allocates a fresh `[]byte`. Already owned.
3. **Key (string):** Go strings are immutable. Assignment shares the string data. Already zero-copy.
4. **Vptr/Hlc:** Value types (16 + 8 bytes). Trivial.

### LAZY_SMALL cache-pinning design

When LAZY_SMALL is set and the value is inline (not VLOG):

1. Find uses the iterator to locate the key (via findSeekIter, which calls Seek internally)
2. Record the result key from pKV.Key
3. Release the iterator state (drops all prefetch span refs)
4. Call `findBuildKVZeroCopy(resultKey)` which does:
   a. `findAnchorPos(key)` - walk sparse index tree to find the anchor (O(log N))
   b. `getEntry(anchor, loff, db)` - guaranteed cache hit, bumps refcnt
   c. `findKeyEQ(fce, key)` - fingerprint scan to find the KV in the interval
   d. Return KVcloser with `KV: fce.kvs[idx]` (Value aliases cache) and pinned entry
5. The KVcloser holds the cache entry pin until Close()

**Why re-lookup instead of per-span tracking:**
- No changes to prefetchSpan struct (hot iteration path unchanged)
- No refcnt management during span building/release
- Guaranteed cache hit (entry was just loaded by the iterator)
- Cost: one sparse index tree walk + one partition lock acquisition (both fast)
- Works for all search modifiers (Exact, GTE, GT, LTE, LT)

**When LAZY_SMALL falls back to copying:**
- Result came from memtable (data not in FlexSpace yet): `valueResolved` would be true in the iterator, but since we release iterator state before the re-lookup, we detect this when `findBuildKVZeroCopy` returns nil (key not found in FlexSpace). Actually, more precisely: the key might be in both memtable and FlexSpace, but the memtable version wins in the merge. The re-lookup would find the FlexSpace version which may be stale. To handle this correctly, we check `it.valueResolved` BEFORE the re-lookup: if the iterator used the merged path (valueResolved=true, meaning memtables were non-empty), we skip the LAZY_SMALL optimization and copy.

Wait - this is an important correction. Let me refine:

**Refined LAZY_SMALL condition in Find (Step 5):**
```go
// LAZY_SMALL zero-copy only when the fast path was used (empty memtables,
// pKV came from cache). When memtables are non-empty, the merged path
// may return a memtable result that differs from FlexSpace. Re-lookup
// would return stale FlexSpace data. Fall back to copy.
if lazyVal && !it.valueResolved && !zc.HasVPtr() && zc.Value != nil {
    kvc = db.findBuildKVZeroCopy(resultKey)
    ...
}
```

The `!it.valueResolved` check ensures we only attempt zero-copy when the fast path was used (both memtables empty, pKV points directly into cache). This is the common steady-state case (memtables are empty most of the time between flushes).

## Files to Modify

1. **`db.go`** - LAZY_SMALL const, findBuildKVZeroCopy(), KVcloser.Fetch(), Find logic
2. **`find_test.go`** - Update ~25 Find() call sites + add LAZY_LARGE and LAZY_SMALL tests
3. **`vlog_test.go`** - Update 4 GetKV() call sites (lines 374, 402, 466, 493)

## Verification

1. `go build ./...` - compile check
2. `go test -run TestFind ./...` - all Find tests pass with new signature
3. `go test -run TestFetchLarge ./...` - large value tests
4. `go test -run TestFind_LazyLarge ./...` - new LAZY_LARGE test
5. `go test -run TestFind_LazyVal ./...` - new LAZY_SMALL zero-copy test
6. `go test ./...` - full test suite passes

## IO erros now propagated up

  intervalcache.go:
  - intervalCacheEntry gained loadErr error field
  - loadInterval returns error (wraps ReadFragmentation failures with context)
  - getEntry returns (*intervalCacheEntry, error) - stores/checks loadErr including for spin-wait path
  - getEntryUnsorted returns (*intervalCacheEntry, error)

  iter.go:
  - Iter struct gained err error field + Err() error method
  - 6 flex* methods now return error: flexSpaceSeekGE, flexSpaceSeekLE, flexCursorSeekGE, flexCursorSeekLE, flexCursorNextInterval, flexCursorPrevInterval
  - All callers within iter.go set it.err on error and bail out

  db.go:
  - findBuildKVZeroCopy returns (*KVcloser, error)
  - getPassthrough returns ([]byte, bool, error)
  - getPassthroughKV returns (KV, bool, error)
  - putPassthrough returns error
  - someLockHeldGet returns ([]byte, bool, error) - propagates resolveVPtr errors
  - FlexDB.Get returns ([]byte, bool, error)
  - FlexDB.Find propagates findBuildKVZeroCopy error

  tx.go:
  - ReadOnlyDB.Get interface returns ([]byte, bool, error)
  - WriteTx.Get / ReadOnlyTx.Get return ([]byte, bool, error)
  - txFind/txFindIt propagate findBuildKVZeroCopy error

  Test/cmd files: ~60+ db.Get/rwDB.Get/roDB.Get call sites updated across db_test.go, gc_test.go, iter_test.go, recovery_test.go, tx_test.go, batch_test.go, find_test.go, porc_test.go,
  anchor_drift_fuzz_test.go, and cmd/yogabench/*.go.

## iterator span nil-ing to prevent temp memleaks when ref counts hit 0.

 Now addressing your follow-up question about cleanup safety: we need to verify that at each iterIOPanic site, the
  panic unwind doesn't leave the database in a corrupted state. Key concerns:

  1. Held cache entry refcounts - if getEntry returned an fce with bumped refcnt, and we panic before releaseEntry, the
   refcnt leaks (entry never evictable)
  2. flexCursor state - the cursor may hold a pinned fce
  3. Partition locks - are any partition.mu locks held at panic time?

  Let me trace each panic site. (done)


follow up on potential for cache leaks, even if temporary.

 So each interval has up to ~1000 KVs. With inline values <= 64 bytes and keys of variable size, each KV struct is 64 bytes (as mentioned in
  the memory notes). So one interval is roughly 64 KB of KV structs plus the backing data for keys and small values.

  Now the worst case. Let me check how the kvs backing array is allocated:

 Searched for 2 patterns, read 1 file (ctrl+o to expand)

 This is the key data. Line 310: kvs := make([]KV, count) - a fresh allocation per decode. Line 313: kvs[i].Key = string(src[...]) - copies
  key bytes into a new string. Line 330-331: kvs[i].Value = make([]byte, vl) then copy - copies value bytes into a fresh allocation. Line 328:
  VPtr entries just decode the pointer (no large value data).

  So each cache entry's kvs backing array contains:
  - The []KV array itself: count * 64 bytes (64-byte KV struct)
  - Each Key string: independent allocation (Go string header + backing bytes)
  - Each inline Value: independent make([]byte, vl) allocation, max 64 bytes
  - VPtr entries: no Value allocation (just the 24-byte VPtr struct stored inline in KV)

  Now I can do the worst-case analysis. Let me check maxPrefetchSpans:

 Now I have the full picture. Here's the worst-case analysis:

  Worst case: 24 spans, each from a different evicted interval.

  Each interval has up to ~1000 KVs. The span's []KV slice header pins the entire backing array (that's how Go slices work - a slice of
  kvs[:count] still pins the full cap of the underlying array).

  Per evicted interval pinned by a span:
  - []KV backing array: 1000 * 64 = 64 KB (KV structs)
  - Key strings: each is an independent allocation, pinned by the string header inside the KV struct. ~1000 keys * avg key size. Say avg 32
  bytes = 32 KB
  - Inline Value []byte: ~1000 values * avg up to 64 bytes = 64 KB (worst case all inline)
  - Total per interval: ~160 KB

  With 24 spans: 24 * 160 KB = ~3.8 MB worst case per iterator.

-----------
All pass. Here's the analysis and what was done:

  Worst-case space retention analysis

  What the spans pin: Each prefetchSpan.kvs is a []KV slice header that keeps the entire backing array alive via Go GC roots, even after the
  cache evicts the entry.

  Per interval pinned:
  - []KV backing array: ~1000 entries * 64 bytes = 64 KB
  - Key strings (separate allocations, held by string headers in KV structs): ~32 KB avg
  - Inline values (separate []byte allocations, max 64 bytes each): up to 64 KB
  - Large values are NOT pinned - they live in VLOG, only a 24-byte VPtr is in the KV struct
  - Total per interval: ~160 KB

  Worst case per iterator: 24 spans * 160 KB = ~3.8 MB

  Realistic case: The prefetch loop stops at 512 keys, and intervals hold ~1000 keys, so typically 1-2 spans. Retention: 160-320 KB, and only
  until the next Next() refill.

  Real risk scenario: Multiple idle iterators in a long View() transaction. 10 iterators * 3.8 MB = 38 MB of pinned evicted cache entries.

  Fix applied

  Nil out span.kvs at every point where spans are discarded:

  1. servePrefetch/servePrefetchReverse - when advancing past an exhausted span (the most common path)
  2. releaseIterState - clears all spans when iterator is closed/reset
  3. HLC-stale invalidation in Next() and Prev() - clears unserved spans when a mutation invalidates the prefetch

  This ensures cache entry backing arrays become GC-eligible as soon as we're done reading from each span, rather than lingering until the next
   prefetchFill overwrites the span struct.

