// YogaDB is an embedded persistent ordered key-value store built on
// the FlexSpace log-structured storage engine architecture. A three-layer
// architecture is deployed: at the lowest level is the FlexTree
// (a B-tree extent index with shift-propagation); at the middle level is
// the FlexSpace (log-structured file layer with GC); and at the
// top sits the FlexDB (the KV store with memtables, WAL,
// sparse index, and interval cache).
//
// # The Iterator Optimization History:
//
// Getting 42x faster at iteration: from 340 ns/key to 8 ns/key.
//
// The Iter type went through a series of CPU-profile-driven optimizations
// to bring forward iteration throughput from ~340 ns/key down to ~8 ns/key,
// beating bbolt (~12 ns/key) and roughly 25x faster than Pebble
// (~210 ns/key). Each optimization was benchmarked individually using
// Benchmark_Iter_YogaDB_Ascend with 100K keys. The progression:
//
// 1. Stateful FlexSpace Cursor (flexCursor)
//
// Replaced stateless re-seeking (O(log n) binary search per Next()) with a
// persistent flexCursor that maintains a pointer to the current sparse-index
// leaf node, anchor index, and position within the cached interval. Sequential
// Next() calls advance the cursor in O(1) amortized time by incrementing
// kvIdx within the current interval, only doing tree traversal at interval
// boundaries. The cursor holds a refcounted interval cache entry so KV data
// is accessed zero-copy.
//
// 2. HLC-Based Version Detection
//
// Instead of re-acquiring topMutRW.RLock on every Next() call, the iterator
// snapshots the database's Hybrid Logical Clock (HLC) at seek time. On Next(),
// a single atomic load compares the current HLC against the snapshot. If they
// match, no mutations have occurred and the cursor remains valid - the entire
// lock acquisition and merged re-seek are skipped. When mutations are detected,
// the iterator falls back to a full re-seek. This turns the common case
// (iteration without concurrent writes) into an atomic load instead of a
// mutex round-trip.
//
// 3. Prefetch Buffer
//
// Added a prefetch buffer that fills multiple KV entries per lock acquisition.
// A single RLock fills up to iterPreFetchKeyCount entries; subsequent Next()
// calls serve from the buffer with only the atomic HLC check. This amortized
// the per-key lock overhead to near zero, since one lock acquisition covers
// hundreds of keys.
//
// 4. Pointer-Based KV Access (*KV instead of embedded KV)
//
// Changed the Iter struct from embedding a full KV (with Key/Value byte slices
// copied per entry) to holding a *KV pointer directly into the interval cache's
// kvs[] slice. On the fast path, Next() just updates a pointer - no struct copy,
// no byte slice copy. Key() and Value() read through the pointer. This eliminated
// ~80 bytes of copying per key. (~18 ns/key to ~16 ns/key.)
//
// 5. Batched Interval Recording in prefetchFill
//
// Restructured prefetchFillFlexSpaceOnly to process entries in batches per
// interval rather than one at a time. Each interval contributes a contiguous
// block of entries, avoiding repeated per-entry function calls and pointer
// chasing across interval boundaries. (~16 ns/key to ~14 ns/key.)
//
// 6. Split Advance/Retreat into Fast Path + Boundary Crossing
//
// Split flexCursorAdvance into a two-instruction fast path (kvIdx++ and bounds
// check) plus a separate flexCursorNextInterval function for the rare interval
// boundary crossing. The fast path is fully inlineable by the Go compiler.
// flexCursorRetreat was split similarly into flexCursorPrevInterval. This
// eliminated function call overhead from the hot loop. (~14 ns/key to ~13.5 ns/key.)
//
// 7. Span-Based Prefetch (O(intervals) not O(keys))
//
// Replaced the per-key []*KV prefetch buffer with an array of prefetchSpan
// descriptors. Each span records a (kvs slice, pos, end) triple referencing
// a contiguous range within one interval's kvs[]. The fill function
// (prefetchFillFlexSpaceOnly) runs under the lock and is now O(intervals):
// it just records ~3 slice boundaries, touching no per-KV data. The per-KV
// work (tombstone checking, pointer dereferencing) is deferred to
// servePrefetch which runs outside the lock. The kvs slice is pre-sliced
// to [:count] as a bounds-check elimination (BCE) hint so the Go compiler
// can prove index safety within the inner loop. This reduced lock hold time
// proportional to the number of intervals (~3) rather than keys (~512).
// (~13.5 ns/key to ~13 ns/key, with the main benefit being reduced lock
// contention under concurrent access.)
//
// 8. Increased Prefetch Count + Eliminated Redundant Atomic Loads
//
// Raised iterPreFetchKeyCount from 90 to 512. With span-based fill being
// O(intervals), larger prefetch counts add negligible fill cost while
// reducing the frequency of lock acquisitions for refills. Also eliminated
// a redundant db.hlc.Aload() in the Next() refill path by caching the HLC
// value read during the initial check. (~13-14 ns/key, reduced variance.)
//
// 9. Atomic Reference Counting in Interval Cache
//
// Converted the interval cache entry's refcnt and access fields from
// mutex-protected int to atomic int32 operations. releaseEntry (called on
// every interval boundary crossing) went from mutex lock/unlock to a single
// atomic.AddInt32. The getEntry cache-hit path was reduced from two
// mutex lock/unlock pairs to one (for the anchor.fce read) plus an atomic
// store for the access field. calibrate and allocEntryForNewAnchor use
// atomic.LoadInt32/AddInt32/StoreInt32. This eliminated ~660ms of mutex
// overhead from releaseEntry and reduced getEntry's lock contention.
//
// 10. Tuning the overwrite in place slotted page sizes SLOTTED_PAGE_KB=4
// and the flexdbUnsortedWriteQuota=6 brought iteration down to
// about 8 nsec/key. Using SLOTTED_PAGE_KB=128 we could cut that in half
// again, but we are already beating Bolt by 2x (why bother with 3x; it
// is available if you need it though) and we want to balance against
// read and insertion efficiency and on-disk space consumption.
package yogadb
