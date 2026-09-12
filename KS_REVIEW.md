# keyStable, memtable, and interval cache locking review

## Scope

This reviews the new `x bool` locking convention added around `keyStable` and the surrounding memtable/iterator/cache call paths.

The contract I used for the audit:

- `x == true`: the caller already holds `db.topMutRW.Lock()` exclusively, so `keyStable` may skip its internal `mu`.
- `x == false`: the caller does not have exclusive DB access, so `keyStable` must take its internal `mu`.

The important failure mode is passing `x=true` from a path that only holds `topMutRW.RLock()`, or from a path with no DB top lock at all.

## Summary

I did not find a production path where `x=true` is passed to `keyStable`/memtable lookup or mutation without the DB write lock actually being held.

The places that looked suspicious in `iter.go` are conservative: they mostly hard-code `x=false`. That is safe for correctness, including inside a `WriteTx`, but it means write-transaction iterators sometimes take `keyStable.mu` unnecessarily. The current implementation favors safety over maximum concurrency/performance there.

The main risk area is not an obvious `x=true` misuse. It is that `bulkIngestBuilder.get` ignores `x` and lazily mutates `bulk.index`. That is safe only if the pre-`AllowReads` bulk memtable is never read concurrently by normal reader APIs. The phase invariant appears intended, but it is important enough to document or assert.

The interval cache should not mechanically receive this same `x` flag. It has its own locking model: partition mutexes plus atomic refcounts for cache structure/lifetime, and `topMutRW` for protecting cache entry contents from concurrent writer mutation. I found no need for `x` there, but several cache methods rely on the caller holding the DB write lock when mutating entry contents.

## Audited `x=true` Paths

### Correct production uses

These paths pass `x=true` only while `topMutRW.Lock()` is held:

- `Batch.commitMaybeMetrics` acquires `db.topMutRW.Lock()` before using `mt.put`, `flushMemtable`, `mt.ks.clear`, and old-value lookup helpers.
- `Put`, `Delete`, `Merge`, `Clear`, and `DeleteRange` enter write-locked helper bodies before using `x=true`.
- `Sync`, `Close`, background `doFlush`, recovery, integrity checks, vacuum/merge helpers, and delete-all helpers perform their `x=true` work under the write lock or before the DB is exposed to concurrent users.
- `WriteTx` methods use `x=true`, and `Update`/`BeginUpdate` hold `topMutRW.Lock()` for the lifetime of the transaction.
- `rollbackOpen` clears the memtable with `x=true`; this is correct because a write transaction owns the write lock and begins after `writeLockHeldSync`.

### Test/direct-structure exceptions

Some tests and `btree_demo.go` pass `x=true` directly to `keyStable` without any `FlexDB.topMutRW`. These are direct single-threaded structure tests, not API call paths. They do not violate runtime concurrency, but they are a sign that raw boolean use is easy to copy into the wrong context.

## `iter.go` Findings

The iterator code does not appear to pass `x=true` unsafely. The suspicious sites are `x=false`:

- `mergedSeekGE` and `mergedSeekGEFastFlexSpace` hard-code `x=false`.
- `Iter.SeekLast`, `Iter.Prev`, `Iter.FetchV`, and `iterResolvedValue` hard-code `x=false`.
- `findSeekIter` receives an `x`, but only the `LTE`/`LT` paths pass it into `seekLE`. The `Exact`/`GTE`/`GT` paths use `Iter.Seek`, which currently routes through conservative `x=false` paths.
- `WriteTx.Descend` and `WriteTx.DescendRange` pass `x=true` to the initial non-empty-pivot `seekLE`, which is correct. If the pivot is empty, they call `SeekLast`, which uses `x=false`; this is safe but less optimized.

The current iterator state does not remember whether it was created by `WriteTx` or `ReadOnlyTx`. That explains the "not sure" comments: later calls such as `Prev` cannot know whether they may safely pass `x=true`, so they choose `false`.

Recommendation: if this becomes performance-sensitive, give `Iter` an explicit lock mode set by `WriteTx.NewIter`/`ReadOnlyTx.NewIter`/public `Find`, instead of scattering local `const x = false` decisions. Until then, the current choices are safe.

## Memtable and keyStable Concurrency

`keyStable.mu` protects lazy sorting and the mutable keyStable arrays when callers do not have exclusive DB access. That covers the original concern about readers concurrently entering `keyStable.get` and triggering lazy sorting.

However, `keyStable.mu` is not a complete memtable lock:

- `memtable.size`, `vtypArena`, WAL state, and the bulk ingest builder are still protected by the higher-level DB write lock, not by `keyStable.mu`.
- `memtable.reset` and `memtable.materializeBulk` use internal `x=true` and therefore rely on being called only under the DB write lock in production.
- `keyStable.Len()` does not lock. Production use in `bulkInitialFastPathEligibleLocked` is under the write lock. Test use is single-threaded.

One thing to be careful with: `keyStable.Ascend`/`Descend`/`Scan`/`Reverse` hold `keyStable.mu` across the callback when `x=false`. I did not see production `x=false` callbacks that call back into DB/keyStable, but future call sites should avoid doing meaningful DB work from such callbacks or they may create a self-deadlock.

## Bulk Ingest Builder Risk

`bulkIngestBuilder.get(key, x bool)` ignores `x` and calls `ensureIndex`, which lazily writes `b.index`.

That is acceptable only if all calls that can see `m.bulk.count > 0` are serialized by `topMutRW.Lock()` or happen before `AllowReads` exposes normal readers. The surrounding code strongly suggests this is the intended invariant:

- Pre-`AllowReads` only batch loading is supported.
- `AllowReads` grabs the write lock and calls `writeLockHeldSyncCheckpoint`.
- Normal read APIs require `AllowReads`.
- `flushMemtable` rejects unmaterialized initial bulk data reaching the normal memtable flush path.

Still, this invariant is not enforced at the `bulkIngestBuilder` boundary. If a future path lets readers call `mt.get(..., x=false)` while `m.bulk.count > 0`, concurrent readers can race while building `bulk.index`.

Recommendation: either document/assert that `mt.bulk.count == 0` whenever `allowReads` is true and normal read APIs are active, or give `bulkIngestBuilder` its own synchronization for `ensureIndex`.

## Interval Cache Review

I do not think the interval cache should grow the same `x bool` parameter. It is not doing lazy keyStable sorting; it already has a partition-level mutex and atomics for its own structure.

The interval cache uses two separate protection layers:

- `intervalCachePartition.mu` protects the clock list, partition size accounting, `loading`, and cache entry publication/removal.
- Atomic fields protect `refcnt`, `access`, and `anchor.fce` publication.
- `topMutRW` protects entry contents (`fce.kvs`, `fce.count`, `dirty`, `dirtyNode`, `anchor.psize`, sparse-index shifts) from concurrent writer mutation while readers are active.

Reader paths such as `getPassthrough`, iterator FlexSpace seeks, and zero-copy `findBuildKVZeroCopy` call `partition.getEntry`, pin the entry, read `fce.kvs`, and then release or transfer the pin. Multiple readers can do this concurrently because they only read entry contents.

Writer paths such as `putPassthroughR`, `treeInsertAnchor`, `flushDirtyPages`, `delete-all`, merge install, vacuum, and bulk flush mutate cache entries and/or anchors under `topMutRW.Lock()`. The entry mutation helpers (`cacheEntryInsert`, `cacheEntryReplace`, `cacheEntryDelete`, `replaceEntryContents`) do not take `partition.mu` around `fce.kvs`/`fce.count` mutation, so they are not independently thread-safe. They rely on the DB write lock.

That division appears correct as currently used.

### Interval cache cautions

- `flushDirtyPages` walks anchors and reads/writes `fce.dirty` and `fce.kvs` without partition locks. Current call sites are write-locked or recovery-time, so this is fine. It should not be called from an `RLock` path.
- `intervalCache.hasDirtyPages` reads `fce.dirty` without partition locks. Current use is inside `writeLockHeldSyncR`, under the write lock.
- `destroyAll` drops partition clock state without checking `refcnt`. A `KVcloser` from `LAZY_SMALL` may outlive the DB read lock and still hold a pin. In Go this looks memory-safe because the returned `KV`/`Value` slice keeps the value backing storage alive and `releaseEntry` is just an atomic decrement, but `destroyAll` is intentionally stronger than eviction and ignores the usual refcount eviction rule.
- The cache's zero-copy API makes lifetime separate from `topMutRW`: `Find(LAZY_SMALL, ...)` can return after releasing the DB read lock while the cache entry remains pinned. That is okay, but it is a distinct contract and should stay documented.

Recommendation: do not add `x` to interval cache APIs. Instead, document which interval cache methods require `topMutRW.Lock()` for entry-content mutation and which are safe under `RLock`. If practical, add debug assertions around mutating helpers to catch accidental use outside the write lock.

## Naming and Guardrail Suggestions

The raw name `x` is too small for the amount of correctness it carries. It also gets reused for `valueLog.read(..., topWriteLocked bool)`, which is a different lock domain from `keyStable`.

Suggested follow-ups:

- Replace raw call-site booleans with named constants, for example `ksExclusive = true` and `ksConcurrent = false`, or a small typed enum.
- Consider a debug-only assertion helper for `x=true` paths so misuse fails early.
- Give iterators an explicit lock mode if avoiding extra `keyStable.mu` acquisition inside write transactions matters.
- Assert the bulk-load phase invariant: normal read paths should not observe `mt.bulk.count > 0`.

## Bottom Line

The new `keyStable.mu` strategy looks concurrency-correct in the audited production API paths. I did not find the specific bad pattern of `x=true` being passed without the DB write lock.

The two places I would harden are the pre-`AllowReads` bulk builder invariant and the readability/maintainability of the boolean itself. The interval cache does not need `x`, but its write-only mutation contract should be documented because several helpers are safe only because `topMutRW.Lock()` excludes readers.
