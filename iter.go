package yogadb

import (
	//"fmt"
	"sort"
	"sync/atomic"

	"github.com/tidwall/btree"
)

// iterPreFetchKeyCount is the number of keys to prefetch into the iterator's
// span buffer during a single lock acquisition. Subsequent Next() calls
// serve from spans with only an atomic HLC check (no lock).
// With spans, larger values reduce refill frequency with minimal overhead
// since fill is O(intervals) not O(keys).
const iterPreFetchKeyCount = 512 // 512 // ; 2048=>11.25; 1024=>10.24; 512=>9.961 nsec; 400=>9.742 nsec; 350=>11.90; 450=>12.69; 600=>10.58; 550=>10.38; 500=>10.08; 400=>12.82; 512=>9.816; 1=>17.5;
// 1=>21.49 'InlineFast=0 ServePrefetch=0 InlineRefill1=93789 InlineRefill2=0 FullRefill=6211 SlowPath=0 HLCStale=0 SnapshotZero=0'
// 2=>13.89, 21.24 'InlineFast=49693 ServePrefetch=0 InlineRefill1=44096 InlineRefill2=0 FullRefill=6211 SlowPath=0 HLCStale=0 SnapshotZero=0'
// 4=>13.28 'InlineFast=74540 ServePrefetch=0 InlineRefill1=19249 InlineRefill2=0 FullRefill=6211 SlowPath=0 HLCStale=0 SnapshotZero=0'
// 8=>10.45 (with:'InlineFast=86963 ServePrefetch=0 InlineRefill1=6826 InlineRefill2=0 FullRefill=6211 SlowPath=0 HLCStale=0 SnapshotZero=0'
// 16=>10.77 InlineFast=93175 ServePrefetch=0 InlineRefill1=3512 InlineRefill2=0 FullRefill=3313 SlowPath=0 HLCStale=0 SnapshotZero=0'
// 32=>9.903,12.69  'InlineFast=93388 ServePrefetch=2204 InlineRefill1=2204 InlineRefill2=0 FullRefill=2204 SlowPath=0 HLCStale=0 SnapshotZero=0'
// 64=>10.65 'InlineFast=93403 ServePrefetch=3958 InlineRefill1=1319 InlineRefill2=0 FullRefill=1320 SlowPath=0 HLCStale=0 SnapshotZero=0'
// 128=>10.83,11.83 'InlineFast=93432 ServePrefetch=5109 InlineRefill1=729 InlineRefill2=0 FullRefill=730 SlowPath=0 HLCStale=0 SnapshotZero=0'
// 256=>10.99 'InlineFast=93513 ServePrefetch=5724 InlineRefill1=381 InlineRefill2=0 FullRefill=382 SlowPath=0 HLCStale=0 SnapshotZero=0'
// 320=>15.44, 'InlineFast=93537 ServePrefetch=5848 InlineRefill1=307 InlineRefill2=0 FullRefill=308 SlowPath=0 HLCStale=0 SnapshotZero=0'
// 350=>12.69 'InlineFast=93507 ServePrefetch=5928 InlineRefill1=282 InlineRefill2=0 FullRefill=283 SlowPath=0 HLCStale=0 SnapshotZero=0'
// 400=>10.82 'InlineFast=93789 ServePrefetch=5714 InlineRefill1=248 InlineRefill2=0 FullRefill=249 SlowPath=0 HLCStale=0 SnapshotZero=0'
// 450=>14.22 'InlineFast=93789 ServePrefetch=5714 InlineRefill1=248 InlineRefill2=0 FullRefill=249 SlowPath=0 HLCStale=0 SnapshotZero=0'
// 512=>9.998 'InlineFast=93789 ServePrefetch=5714 InlineRefill1=248 InlineRefill2=0 FullRefill=249 SlowPath=0 HLCStale=0 SnapshotZero=0'
// 1024=>12.95 'InlineFast=93789 ServePrefetch=5714 InlineRefill1=248 InlineRefill2=0 FullRefill=249 SlowPath=0 HLCStale=0 SnapshotZero=0'
// 5120=>9.898 'InlineFast=93789 ServePrefetch=5714 InlineRefill1=248 InlineRefill2=0 FullRefill=249 SlowPath=0 HLCStale=0 SnapshotZero=0'

// flexCursor holds the stateful position within FlexSpace's sparse index tree.
// Instead of re-seeking from the root on every Next(), it maintains a pointer
// to the current leaf node, anchor index, and position within the cached interval.
type flexCursor struct {
	node       *memSparseIndexTreeNode // current leaf in sparse index tree
	anchorIdx  int                     // index within node.anchors[]
	shift      int64                   // accumulated shift for this node
	fce        *intervalCacheEntry     // currently loaded interval cache entry
	partition  *intervalCachePartition // partition owning fce (for release)
	kvIdx      int                     // position within fce.kvs[]
	positioned bool                    // true if cursor has a valid position
}

// release drops the reference on the currently held cache entry.
func (fc *flexCursor) release() {
	if fc.fce != nil && fc.partition != nil {
		fc.partition.releaseEntry(fc.fce)
		fc.fce = nil
		fc.partition = nil
	}
}

// reset clears all cursor state.
func (fc *flexCursor) reset() {
	fc.release()
	fc.node = nil
	fc.anchorIdx = 0
	fc.shift = 0
	fc.kvIdx = 0
	fc.positioned = false
}

// maxPrefetchSpans is the max number of interval spans per prefetch fill.
// With ~32 keys per interval and 512 keys to prefetch: ceil(512/32)+1 = 17.
// We use 24 for safety (empty intervals, tombstone-only intervals, etc.).
const maxPrefetchSpans = 24

// prefetchSpan records a contiguous range within one interval's kvs slice.
// Used to defer per-KV access from prefetchFill (under lock) to servePrefetch (no lock).
type prefetchSpan struct {
	kvs []KV // slice into cache interval, pre-sliced to [:count] for BCE
	pos int  // next index to serve
	end int  // forward: pos < end; reverse: pos >= end (end is lower bound)
}

// ====================== Iterator ======================
//
// Iter is a stateful, prefetching iterator over the merged view of:
//   - Active memtable (highest priority)
//   - Inactive memtable
//   - FlexSpace via sparse index (lowest priority)
//
// Iterators are created within a transaction via rwDB.NewIter() or
// roDB.NewIter(). The transaction holds the lock; the iterator does
// not manage locks itself. Multiple iterators can coexist within
// one transaction. All iterators are auto-closed when the transaction
// callback returns, but may be closed earlier via it.Close().
//
// Zero-copy access to cache memory is safe because the transaction
// holds a lock (write lock for Update, read lock for View).
//
// Large values (stored in VLOG) are not fetched by default. Use
// Large() to check and FetchV() to fetch on demand.
// iterIOErr is a sentinel panic value for FlexSpace I/O errors during
// iteration. Update/View/Find/Get recover it and return as error.
type iterIOErr struct{ err error }

func iterIOPanic(err error) { panic(iterIOErr{err}) }

type Iter struct {
	db *FlexDB

	// pKV points to the current key-value pair. Points directly into
	// cache memory (zero-copy), safe because the write lock is held.
	pKV *KV

	valid     bool
	dir       int // 1=forward, -1=backward (informational)
	closed    bool
	lazyLarge bool

	// Stateful FlexSpace cursor for O(1) amortized forward iteration.
	fc flexCursor

	// HLC snapshot for fast-path version detection.
	// If db.hlc hasn't changed since snapshotHLC, no mutations occurred
	// and the FlexSpace cursor remains valid - use the fast path.
	snapshotHLC HLC

	// Reusable buffers for key/value copies to avoid allocation per Next().
	keyBuf []byte
	valBuf []byte

	// Lazy value resolution: on the fast path, curValue holds a direct
	// reference into cache memory (Go GC keeps it alive). The value is
	// only copied into valBuf when Vin() is actually called by the user.
	valueResolved bool

	// Diagnostic counters for profiling Next() path distribution.
	// Check with it.PathCounts() after iteration.
	// cntInlineFast    int64 // served from inline fast path (common case)
	// cntServePrefetch int64 // served from servePrefetch (tombstone/span boundary)
	// cntInlineRefill1 int64 // inline refill within fast path (same interval)
	// cntInlineRefill2 int64 // inline refill at refill entry (same interval)
	// cntFullRefill    int64 // called prefetchFillFlexSpaceOnly
	// cntSlowPath      int64 // hit releaseIterState slow path
	// cntHLCStale      int64 // HLC mismatch invalidated prefetch
	// cntSnapshotZero  int64 // snapshotHLC was 0 at refill check

	// Prefetch spans: recorded during fill (under lock), served lazily.
	// Each span references a contiguous range within a cache interval's kvs[].
	// Fill is O(intervals), not O(keys) - just records slice boundaries.
	// Per-KV access (tombstone check, pointer deref) deferred to servePrefetch.
	pfSpans     [maxPrefetchSpans]prefetchSpan
	pfSpanCount int // number of recorded spans
	pfSpanIdx   int // current span being served
}

// PathCounts returns diagnostic counters showing which Next() code paths were taken.
// func (it *Iter) PathCounts() string {
// 	return fmt.Sprintf(
// 		"InlineFast=%d ServePrefetch=%d InlineRefill1=%d InlineRefill2=%d FullRefill=%d SlowPath=%d HLCStale=%d SnapshotZero=%d",
// 		it.cntInlineFast, it.cntServePrefetch, it.cntInlineRefill1, it.cntInlineRefill2,
// 		it.cntFullRefill, it.cntSlowPath, it.cntHLCStale, it.cntSnapshotZero)
// }

// releaseIterState releases all stateful cursor resources.
func (it *Iter) releaseIterState() {
	it.fc.reset()
	it.snapshotHLC = 0
	it.pfSpanCount = 0
	it.pfSpanIdx = 0
}

// prefetchFillFlexSpaceOnly records span descriptors for upcoming KV entries
// by walking the FlexSpace cursor forward. O(intervals) not O(keys) - just
// saves slice boundaries (~3 spans), no per-KV data access under the lock.
// Per-KV work (tombstone check, pointer deref) is deferred to servePrefetch.
// Caller must hold topMutRW.RLock().
func (it *Iter) prefetchFillFlexSpaceOnly() {
	it.pfSpanCount = 0
	it.pfSpanIdx = 0
	nSpans := 0
	total := 0

	for total < iterPreFetchKeyCount && nSpans < maxPrefetchSpans {
		if !it.fc.positioned || it.fc.fce == nil {
			break
		}
		count := it.fc.fce.count
		idx := it.fc.kvIdx
		if idx >= count {
			if err := it.db.flexCursorNextInterval(&it.fc); err != nil {
				iterIOPanic(err)
				break
			}
			continue
		}

		// How many entries to claim from this interval.
		n := count - idx
		if n > iterPreFetchKeyCount-total {
			n = iterPreFetchKeyCount - total
		}

		it.pfSpans[nSpans] = prefetchSpan{
			kvs: it.fc.fce.kvs[:count], // BCE hint: pre-sliced
			pos: idx,
			end: idx + n,
		}
		nSpans++
		total += n

		it.fc.kvIdx = idx + n
		if it.fc.kvIdx >= count {
			if err := it.db.flexCursorNextInterval(&it.fc); err != nil {
				iterIOPanic(err)
				break
			}
		}
	}
	it.pfSpanCount = nSpans
}

// servePrefetch walks forward through prefetch spans, skipping tombstones,
// and sets pKV to the next valid entry. Returns true if an entry was served.
// Runs outside the lock.
//
// The common case (next entry in current span, not a tombstone) is inlined
// into Next() via servePrefetchInline to avoid function call overhead.
// This full version handles span exhaustion and tombstone runs.
func (it *Iter) servePrefetch() bool {
	for it.pfSpanIdx < it.pfSpanCount {
		span := &it.pfSpans[it.pfSpanIdx]
		pos := span.pos
		// Reslice to span.end so the compiler can eliminate
		// bounds checks: pos < len(kvs) is proven by the loop condition.
		kvs := span.kvs[:span.end]
		for pos < len(kvs) {
			pkv := &kvs[pos] // BCE: pos < len(kvs)
			pos++
			if pkv.isTombstone() {
				continue
			}
			span.pos = pos
			it.pKV = pkv
			it.valueResolved = false
			it.valid = true
			it.dir = 1
			return true
		}
		span.pos = pos
		it.pfSpanIdx++
	}
	// All spans exhausted (possibly all tombstones). Don't set valid=false;
	// let the caller fall through to refill.
	return false
}

// reuseAppend appends src to buf, reusing buf's capacity when possible.
func reuseAppend(buf, src []byte) []byte {
	if cap(buf) >= len(src) {
		buf = buf[:len(src)]
		copy(buf, src)
		return buf
	}
	return append(buf[:0], src...)
}

// ====================== btree one-shot seek helpers ======================

// btreeSeekGE does a one-shot seek in a btree: finds the first item >= target.
// If strict is true, skips exact matches (finds first item > target).
func btreeSeekGE(bt *btree.BTreeG[KV], target string, strict bool) (KV, bool) {
	var result KV
	var found bool
	bt.Ascend(KV{Key: target}, func(item KV) bool {
		if strict && item.Key == target {
			return true // skip exact match
		}
		result = item
		found = true
		return false
	})
	return result, found
}

// btreeSeekLE does a one-shot seek in a btree: finds the last item <= target.
// If strict is true, skips exact matches (finds last item < target).
func btreeSeekLE(bt *btree.BTreeG[KV], target string, strict bool) (KV, bool) {
	var result KV
	var found bool
	if target == "" {
		bt.Reverse(func(item KV) bool {
			result = item
			found = true
			return false
		})
	} else {
		bt.Descend(KV{Key: target}, func(item KV) bool {
			if strict && item.Key == target {
				return true // skip exact match
			}
			result = item
			found = true
			return false
		})
	}
	return result, found
}

// ====================== FlexSpace one-shot seek helpers ======================

// flexSpaceSeekGE finds the first KV >= target in FlexSpace via sparse index + cache.
// If strict is true, skips exact matches. Caller must hold topMutRW.RLock().
func (db *FlexDB) flexSpaceSeekGE(target string, strict bool) (KV, bool, error) {
	t := db.tree
	if t == nil || t.root == nil {
		return KV{}, false, nil
	}

	var nh memSparseIndexTreeHandler
	t.findAnchorPos(target, &nh)
	node := nh.node
	anchorIdx := nh.idx
	shift := nh.shift

	if node == nil || node.count == 0 {
		return KV{}, false, nil
	}

	for {
		if anchorIdx >= node.count {
			next := node.next
			if next == nil {
				return KV{}, false, nil
			}
			node = next
			anchorIdx = 0
			nh2 := memSparseIndexTreeHandler{node: node}
			memSparseIndexTreeHandlerInfoUpdate(&nh2)
			shift = nh2.shift
		}

		anchor := node.anchors[anchorIdx]
		if anchor == nil || anchor.psize == 0 {
			anchorIdx++
			continue
		}

		anchorLoff := uint64(anchor.loff + shift)
		partition := db.cache.getPartition(anchor)
		fce, err := partition.getEntry(anchor, anchorLoff, db)
		if err != nil {
			partition.releaseEntry(fce)
			return KV{}, false, err
		}

		if fce.count == 0 {
			partition.releaseEntry(fce)
			anchorIdx++
			continue
		}

		idx := sort.Search(fce.count, func(i int) bool {
			return fce.kvs[i].Key >= target
		})
		if strict && idx < fce.count && fce.kvs[idx].Key == target {
			idx++
		}
		if idx < fce.count {
			kv := fce.kvs[idx]
			result := KV{Key: kv.Key, Value: dupBytes(kv.Value), Vptr: kv.Vptr, Hlc: kv.Hlc}
			partition.releaseEntry(fce)
			return result, true, nil
		}

		partition.releaseEntry(fce)
		anchorIdx++
	}
}

// flexSpaceSeekLE finds the last KV <= target in FlexSpace via sparse index + cache.
// If strict is true, skips exact matches. Caller must hold topMutRW.RLock().
func (db *FlexDB) flexSpaceSeekLE(target string, strict bool) (KV, bool, error) {
	t := db.tree
	if t == nil || t.root == nil {
		return KV{}, false, nil
	}

	var node *memSparseIndexTreeNode
	var anchorIdx int
	var shift int64

	if target == "" {
		// Navigate to last leaf, last anchor
		node = t.root
		for !node.isLeaf {
			node = node.children[node.count].node
		}
		anchorIdx = node.count - 1
		if anchorIdx < 0 {
			return KV{}, false, nil
		}
		nh2 := memSparseIndexTreeHandler{node: node}
		memSparseIndexTreeHandlerInfoUpdate(&nh2)
		shift = nh2.shift
	} else {
		var nh memSparseIndexTreeHandler
		t.findAnchorPos(target, &nh)
		node = nh.node
		anchorIdx = nh.idx
		shift = nh.shift
	}

	if node == nil || node.count == 0 {
		return KV{}, false, nil
	}

	for {
		if anchorIdx < 0 {
			prev := node.prev
			if prev == nil {
				return KV{}, false, nil
			}
			node = prev
			anchorIdx = node.count - 1
			nh2 := memSparseIndexTreeHandler{node: node}
			memSparseIndexTreeHandlerInfoUpdate(&nh2)
			shift = nh2.shift
			if anchorIdx < 0 {
				return KV{}, false, nil
			}
		}

		anchor := node.anchors[anchorIdx]
		if anchor == nil || anchor.psize == 0 {
			anchorIdx--
			continue
		}

		anchorLoff := uint64(anchor.loff + shift)
		partition := db.cache.getPartition(anchor)
		fce, err := partition.getEntry(anchor, anchorLoff, db)
		if err != nil {
			partition.releaseEntry(fce)
			return KV{}, false, err
		}

		if fce.count == 0 {
			partition.releaseEntry(fce)
			anchorIdx--
			continue
		}

		var idx int
		if target == "" {
			idx = fce.count - 1
		} else {
			idx = sort.Search(fce.count, func(i int) bool {
				return fce.kvs[i].Key > target
			}) - 1
		}
		if strict && idx >= 0 && fce.kvs[idx].Key == target {
			idx--
		}
		if idx >= 0 {
			kv := fce.kvs[idx]
			result := KV{Key: kv.Key, Value: dupBytes(kv.Value), Vptr: kv.Vptr, Hlc: kv.Hlc}
			partition.releaseEntry(fce)
			return result, true, nil
		}

		partition.releaseEntry(fce)
		anchorIdx--
	}
}

// ====================== stateful FlexSpace cursor ======================

// flexCursorSeekGE positions the flexCursor at the first KV >= target in FlexSpace.
// If strict is true, skips exact matches. The cursor holds a refcounted cache entry;
// callers read the KV directly from fc.fce.kvs[fc.kvIdx] (zero-copy).
// Caller must hold topMutRW.RLock().
func (db *FlexDB) flexCursorSeekGE(fc *flexCursor, target string, strict bool) error {
	fc.release()
	fc.positioned = false

	t := db.tree
	if t == nil || t.root == nil {
		return nil
	}

	var nh memSparseIndexTreeHandler
	var node *memSparseIndexTreeNode
	var anchorIdx int
	var shift int64

	t.findAnchorPos(target, &nh)
	node = nh.node
	anchorIdx = nh.idx
	shift = nh.shift

	if node == nil || node.count == 0 {
		return nil
	}

	for {
		if anchorIdx >= node.count {
			next := node.next
			if next == nil {
				return nil
			}
			node = next
			anchorIdx = 0
			nh2 := memSparseIndexTreeHandler{node: node}
			memSparseIndexTreeHandlerInfoUpdate(&nh2)
			shift = nh2.shift
		}

		anchor := node.anchors[anchorIdx]
		if anchor == nil || anchor.psize == 0 {
			anchorIdx++
			continue
		}

		anchorLoff := uint64(anchor.loff + shift)
		partition := db.cache.getPartition(anchor)
		fce, err := partition.getEntry(anchor, anchorLoff, db)
		if err != nil {
			partition.releaseEntry(fce)
			return err
		}

		if fce.count == 0 {
			partition.releaseEntry(fce)
			anchorIdx++
			continue
		}

		idx := sort.Search(fce.count, func(i int) bool {
			return fce.kvs[i].Key >= target
		})
		if strict && idx < fce.count && fce.kvs[idx].Key == target {
			idx++
		}
		if idx < fce.count {
			fc.node = node
			fc.anchorIdx = anchorIdx
			fc.shift = shift
			fc.fce = fce
			fc.partition = partition
			fc.kvIdx = idx
			fc.positioned = true
			return nil
		}

		partition.releaseEntry(fce)
		anchorIdx++
	}
}

// flexCursorAdvance moves the flexCursor to the next position.
// After calling, check fc.positioned and read fc.fce.kvs[fc.kvIdx] directly.
// Caller must hold topMutRW.RLock().
func (db *FlexDB) flexCursorAdvance(fc *flexCursor) error {
	if !fc.positioned {
		return nil
	}

	// Try next entry in current interval - the common fast path (no alloc, no function call overhead)
	fc.kvIdx++
	if fc.fce != nil && fc.kvIdx < fc.fce.count {
		return nil // still in same interval, zero-copy
	}

	return db.flexCursorNextInterval(fc)
}

// flexCursorNextInterval crosses an interval boundary: releases the current
// cache entry and advances to the next anchor. Called when we know kvIdx >= count.
// Caller must hold topMutRW.RLock().
func (db *FlexDB) flexCursorNextInterval(fc *flexCursor) error {
	// Inline release - skip nil checks since callers ensure fce is set.
	if fc.fce != nil {
		fc.partition.releaseEntry(fc.fce)
		fc.fce = nil
	}
	fc.anchorIdx++

	node := fc.node
	anchorIdx := fc.anchorIdx
	shift := fc.shift

	for {
		if node == nil {
			fc.positioned = false
			return nil
		}
		if anchorIdx >= node.count {
			node = node.next
			if node == nil {
				fc.positioned = false
				return nil
			}
			anchorIdx = 0
			nh2 := memSparseIndexTreeHandler{node: node}
			memSparseIndexTreeHandlerInfoUpdate(&nh2)
			shift = nh2.shift
		}

		anchor := node.anchors[anchorIdx]
		if anchor == nil || anchor.psize == 0 {
			anchorIdx++
			continue
		}

		partition := &db.cache.partitions[anchor.partitionID]

		// Lock-free fast path: if anchor already has a cached entry,
		// acquire a reference without taking the partition lock.
		fce := anchor.loadFce()
		if fce != nil {
			atomic.AddInt32(&fce.refcnt, 1)
			// Verify entry wasn't evicted between load and refcnt bump.
			// If evicted, anchor.fce is nil (or a new entry). Our refcnt
			// bump on the old entry is harmless - just undo and fall back.
			if anchor.loadFce() == fce && !fce.loading {
				if fce.count == 0 {
					partition.releaseEntry(fce)
					anchorIdx++
					continue
				}
				atomic.StoreInt32(&fce.access, intervalCacheEntryChance)
				fc.node = node
				fc.anchorIdx = anchorIdx
				fc.shift = shift
				fc.fce = fce
				fc.partition = partition
				fc.kvIdx = 0
				return nil
			}
			// Race lost or still loading - release and use locked path.
			atomic.AddInt32(&fce.refcnt, -1)
		}

		// Slow path: cache miss or loading - go through getEntry.
		anchorLoff := uint64(anchor.loff + shift)
		fce, err := partition.getEntry(anchor, anchorLoff, db)
		if err != nil {
			partition.releaseEntry(fce)
			return err
		}

		if fce.count == 0 {
			partition.releaseEntry(fce)
			anchorIdx++
			continue
		}

		fc.node = node
		fc.anchorIdx = anchorIdx
		fc.shift = shift
		fc.fce = fce
		fc.partition = partition
		fc.kvIdx = 0
		return nil
	}
}

// ====================== stateful FlexSpace cursor (backward) ======================

// flexCursorSeekLE positions the flexCursor at the last KV <= target in FlexSpace.
// If strict is true, skips exact matches. The cursor holds a refcounted cache entry;
// callers read the KV directly from fc.fce.kvs[fc.kvIdx] (zero-copy).
// Caller must hold topMutRW.RLock().
func (db *FlexDB) flexCursorSeekLE(fc *flexCursor, target string, strict bool) error {
	fc.release()
	fc.positioned = false

	t := db.tree
	if t == nil || t.root == nil {
		return nil
	}

	var node *memSparseIndexTreeNode
	var anchorIdx int
	var shift int64

	if target == "" {
		// Navigate to last leaf, last anchor
		node = t.root
		for !node.isLeaf {
			node = node.children[node.count].node
		}
		anchorIdx = node.count - 1
		if anchorIdx < 0 {
			return nil
		}
		nh2 := memSparseIndexTreeHandler{node: node}
		memSparseIndexTreeHandlerInfoUpdate(&nh2)
		shift = nh2.shift
	} else {
		var nh memSparseIndexTreeHandler
		t.findAnchorPos(target, &nh)
		node = nh.node
		anchorIdx = nh.idx
		shift = nh.shift
	}

	if node == nil || node.count == 0 {
		return nil
	}

	for {
		if anchorIdx < 0 {
			prev := node.prev
			if prev == nil {
				return nil
			}
			node = prev
			anchorIdx = node.count - 1
			nh2 := memSparseIndexTreeHandler{node: node}
			memSparseIndexTreeHandlerInfoUpdate(&nh2)
			shift = nh2.shift
			if anchorIdx < 0 {
				return nil
			}
		}

		anchor := node.anchors[anchorIdx]
		if anchor == nil || anchor.psize == 0 {
			anchorIdx--
			continue
		}

		anchorLoff := uint64(anchor.loff + shift)
		partition := db.cache.getPartition(anchor)
		fce, err := partition.getEntry(anchor, anchorLoff, db)
		if err != nil {
			partition.releaseEntry(fce)
			return err
		}

		if fce.count == 0 {
			partition.releaseEntry(fce)
			anchorIdx--
			continue
		}

		var idx int
		if target == "" {
			idx = fce.count - 1
		} else {
			idx = sort.Search(fce.count, func(i int) bool {
				return fce.kvs[i].Key > target
			}) - 1
		}
		if strict && idx >= 0 && fce.kvs[idx].Key == target {
			idx--
		}
		if idx >= 0 {
			fc.node = node
			fc.anchorIdx = anchorIdx
			fc.shift = shift
			fc.fce = fce
			fc.partition = partition
			fc.kvIdx = idx
			fc.positioned = true
			return nil
		}

		partition.releaseEntry(fce)
		anchorIdx--
	}
}

// flexCursorRetreat moves the flexCursor to the previous position.
// After calling, check fc.positioned and read fc.fce.kvs[fc.kvIdx] directly.
// Caller must hold topMutRW.RLock().
func (db *FlexDB) flexCursorRetreat(fc *flexCursor) error {
	if !fc.positioned {
		return nil
	}

	// Try previous entry in current interval - the common fast path
	fc.kvIdx--
	if fc.kvIdx >= 0 {
		return nil // still in same interval, zero-copy
	}

	return db.flexCursorPrevInterval(fc)
}

// flexCursorPrevInterval crosses an interval boundary backward: releases the
// current cache entry and retreats to the previous anchor. Called when kvIdx < 0.
// Caller must hold topMutRW.RLock().
func (db *FlexDB) flexCursorPrevInterval(fc *flexCursor) error {
	// Inline release - skip nil checks since callers ensure fce is set.
	if fc.fce != nil {
		fc.partition.releaseEntry(fc.fce)
		fc.fce = nil
	}
	fc.anchorIdx--

	node := fc.node
	anchorIdx := fc.anchorIdx
	shift := fc.shift

	for {
		if node == nil {
			fc.positioned = false
			return nil
		}
		if anchorIdx < 0 {
			node = node.prev
			if node == nil {
				fc.positioned = false
				return nil
			}
			anchorIdx = node.count - 1
			nh2 := memSparseIndexTreeHandler{node: node}
			memSparseIndexTreeHandlerInfoUpdate(&nh2)
			shift = nh2.shift
			if anchorIdx < 0 {
				fc.positioned = false
				return nil
			}
		}

		anchor := node.anchors[anchorIdx]
		if anchor == nil || anchor.psize == 0 {
			anchorIdx--
			continue
		}

		anchorLoff := uint64(anchor.loff + shift)
		partition := db.cache.getPartition(anchor)
		fce, err := partition.getEntry(anchor, anchorLoff, db)
		if err != nil {
			partition.releaseEntry(fce)
			return err
		}

		if fce.count == 0 {
			partition.releaseEntry(fce)
			anchorIdx--
			continue
		}

		fc.node = node
		fc.anchorIdx = anchorIdx
		fc.shift = shift
		fc.fce = fce
		fc.partition = partition
		fc.kvIdx = fce.count - 1
		return nil
	}
}

// ====================== merged seek ======================

// mergedSeekGE finds the smallest key >= target across all sources, resolving
// duplicates by priority (active mt > inactive mt > FlexSpace) and skipping tombstones.
// If strict is true, finds smallest key > target. Caller must hold topMutRW.RLock().
func (db *FlexDB) mergedSeekGE(target string, strict bool) (key, value []byte, hlc HLC, hasVPtr bool, vptr VPtr, found bool) {
	for {
		active := db.activeMT
		inactive := 1 - active

		var candidates [3]KV
		var have [3]bool

		candidates[0], have[0] = btreeSeekGE(db.memtables[active].bt, target, strict)
		if !db.memtables[inactive].empty {
			candidates[1], have[1] = btreeSeekGE(db.memtables[inactive].bt, target, strict)
		}
		var seekErr error
		candidates[2], have[2], seekErr = db.flexSpaceSeekGE(target, strict)
		if seekErr != nil {
			return nil, nil, 0, false, VPtr{}, false
		}

		// Find minimum key
		var minKey string
		haveMin := false
		for i := 0; i < 3; i++ {
			if have[i] {
				if !haveMin || candidates[i].Key < minKey {
					minKey = candidates[i].Key
					haveMin = true
				}
			}
		}
		if !haveMin {
			return nil, nil, 0, false, VPtr{}, false
		}

		// Pick highest-priority source at minKey
		var bestKV KV
		haveBest := false
		for i := 0; i < 3; i++ {
			if have[i] && candidates[i].Key == minKey {
				if !haveBest {
					bestKV = candidates[i]
					haveBest = true
				}
			}
		}

		if !haveBest || bestKV.isTombstone() {
			target = minKey
			strict = true
			continue // skip tombstone, seek past it
		}

		if bestKV.HasVPtr() {
			return []byte(minKey), nil, bestKV.Hlc, true, bestKV.Vptr, true
		}
		val, err := db.resolveVPtr(bestKV)
		if err != nil {
			target = minKey
			strict = true
			continue
		}
		return []byte(minKey), dupBytes(val), bestKV.Hlc, false, VPtr{}, true
	}
}

// ====================== fast-path stateful iteration ======================

// initFlexCursorSeekGE positions the FlexSpace cursor at the first KV >= target
// and snapshots the HLC. Caller must hold topMutRW.RLock().
func (it *Iter) initFlexCursorSeekGE(target string) {
	it.fc.reset()
	if err := it.db.flexCursorSeekGE(&it.fc, target, false); err != nil {
		it.fc.positioned = false
		iterIOPanic(err)
		return
	}
	it.snapshotHLC = it.db.hlc.Aload()
}

// initFlexCursorSeekLE positions the FlexSpace cursor at the last KV <= target
// and snapshots the HLC. Caller must hold topMutRW.RLock().
func (it *Iter) initFlexCursorSeekLE(target string) {
	it.fc.reset()
	if err := it.db.flexCursorSeekLE(&it.fc, target, false); err != nil {
		it.fc.positioned = false
		iterIOPanic(err)
		return
	}
	it.snapshotHLC = it.db.hlc.Aload()
}

// prefetchFillFlexSpaceReverse records span descriptors for upcoming KV entries
// by walking the FlexSpace cursor backward. O(intervals) not O(keys).
// For reverse spans, pos starts high and decrements toward end (lower bound).
// Caller must hold topMutRW.RLock().
func (it *Iter) prefetchFillFlexSpaceReverse() {
	it.pfSpanCount = 0
	it.pfSpanIdx = 0
	nSpans := 0
	total := 0

	for total < iterPreFetchKeyCount && nSpans < maxPrefetchSpans {
		if !it.fc.positioned || it.fc.fce == nil {
			break
		}
		count := it.fc.fce.count
		idx := it.fc.kvIdx
		if idx < 0 {
			if err := it.db.flexCursorPrevInterval(&it.fc); err != nil {
				iterIOPanic(err)
				break
			}
			continue
		}

		// How many entries to claim walking backward from idx.
		n := idx + 1
		if n > iterPreFetchKeyCount-total {
			n = iterPreFetchKeyCount - total
		}
		endIdx := idx - n // one below the last entry we'll serve

		if count > len(it.fc.fce.kvs) {
			break // safety
		}
		it.pfSpans[nSpans] = prefetchSpan{
			kvs: it.fc.fce.kvs[:count], // BCE hint
			pos: idx,                   // start high (first to serve = highest key)
			end: endIdx,                // lower sentinel (pos > end means more entries)
		}
		nSpans++
		total += n

		it.fc.kvIdx = endIdx // one below what we claimed
		if it.fc.kvIdx < 0 {
			if err := it.db.flexCursorPrevInterval(&it.fc); err != nil {
				iterIOPanic(err)
				break
			}
		}
	}
	it.pfSpanCount = nSpans
}

// servePrefetchReverse walks backward through prefetch spans, skipping tombstones,
// and sets pKV to the next valid entry. Returns true if an entry was served.
// Runs outside the lock.
func (it *Iter) servePrefetchReverse() bool {
	for it.pfSpanIdx < it.pfSpanCount {
		span := &it.pfSpans[it.pfSpanIdx]
		for span.pos > span.end {
			pkv := &span.kvs[span.pos]
			span.pos--
			if pkv.isTombstone() {
				continue
			}
			it.pKV = pkv
			it.valueResolved = false
			it.valid = true
			it.dir = -1
			return true
		}
		it.pfSpanIdx++
	}
	return false
}

// mergedSeekGEFastFlexSpace performs a merged seek using one-shot btree seeks for
// memtables and the stateful FlexSpace cursor. The cursor should already be
// positioned at or past the target. Caller must hold topMutRW.RLock().
func (it *Iter) mergedSeekGEFastFlexSpace(target string, strict bool) (kv *KV, found bool) {
	db := it.db

	// Fast path: both memtables empty -> pure FlexSpace iteration.
	// Skip all btree seeks and 3-way merging.
	if db.memtables[0].empty && db.memtables[1].empty {
		kv, found = it.flexSpaceOnlySeekGE(target, strict)
		it.valueResolved = false // value is lazy cache reference
		return
	}

	it.valueResolved = true // slow path always returns owned copies

	for {
		active := db.activeMT
		inactive := 1 - active

		var candidates [3]KV
		var have [3]bool

		// Memtables: one-shot seek (O(log n) but no lock held between calls)
		candidates[0], have[0] = btreeSeekGE(db.memtables[active].bt, target, strict)
		if !db.memtables[inactive].empty {
			candidates[1], have[1] = btreeSeekGE(db.memtables[inactive].bt, target, strict)
		}

		// FlexSpace: use stateful cursor
		it.positionFlexCursorForSeek(target, strict)
		if it.fc.positioned && it.fc.fce != nil && it.fc.kvIdx < it.fc.fce.count {
			fkv := it.fc.fce.kvs[it.fc.kvIdx]
			candidates[2] = KV{Key: fkv.Key, Value: fkv.Value, Vptr: fkv.Vptr, Hlc: fkv.Hlc}
			have[2] = true
		}

		// Find minimum key
		var minKey string
		haveMin := false
		for i := 0; i < 3; i++ {
			if have[i] {
				if !haveMin || candidates[i].Key < minKey {
					minKey = candidates[i].Key
					haveMin = true
				}
			}
		}
		if !haveMin {
			return
		}

		// Pick highest-priority source at minKey
		var bestKV KV
		haveBest := false
		for i := 0; i < 3; i++ {
			if have[i] && candidates[i].Key == minKey {
				if !haveBest {
					bestKV = candidates[i]
					haveBest = true
				}
			}
		}

		// Advance FlexSpace cursor if it was consumed
		if have[2] && candidates[2].Key == minKey {
			if err := db.flexCursorAdvance(&it.fc); err != nil {
				iterIOPanic(err)
				return
			}
		}

		if !haveBest || bestKV.isTombstone() {
			target = minKey
			strict = true
			continue
		}

		if bestKV.HasVPtr() {
			return &KV{Key: minKey, Hlc: bestKV.Hlc, Vptr: bestKV.Vptr}, true
		}
		val, err := db.resolveVPtr(bestKV)
		if err != nil {
			target = minKey
			strict = true
			continue
		}
		return &KV{Key: minKey, Value: dupBytes(val), Hlc: bestKV.Hlc}, true
	}
}

// flexSpaceOnlySeekGE is the ultra-fast path when both memtables are empty.
// Steps through the FlexSpace cursor with zero btree overhead and returns a
// pointer directly into the cache entry's KV slice (zero-copy). Skips tombstones.
// Caller must hold topMutRW.RLock().
func (it *Iter) flexSpaceOnlySeekGE(target string, strict bool) (kv *KV, found bool) {
	it.positionFlexCursorForSeek(target, strict)

	for it.fc.positioned && it.fc.fce != nil && it.fc.kvIdx < it.fc.fce.count {
		// Read directly from cache entry - zero-copy reference
		kv = &it.fc.fce.kvs[it.fc.kvIdx]

		// Advance cursor position for next call (inline, no dupBytes)
		if err := it.db.flexCursorAdvance(&it.fc); err != nil {
			iterIOPanic(err)
			return
		}

		if kv.isTombstone() {
			continue
		}
		found = true
		if kv.HasVPtr() {
			return
		}
		// Lazy value: store direct reference into cache memory (GC-safe).
		// The actual copy into valBuf is deferred to Value().
		return
	}
	return
}

// positionFlexCursorForSeek ensures the FlexSpace cursor is at or past the target.
func (it *Iter) positionFlexCursorForSeek(target string, strict bool) {
	if !it.fc.positioned || it.fc.fce == nil || it.fc.kvIdx >= it.fc.fce.count {
		return
	}
	fkv := it.fc.fce.kvs[it.fc.kvIdx]
	if fkv.Key > target || (fkv.Key == target && !strict) {
		return // cursor is already past target
	}
	// Cursor is behind; advance it
	it.advanceFlexCursorPast(target, strict)
}

// advanceFlexCursorPast advances the FlexSpace cursor past target.
// If strict, past keys > target; otherwise past keys >= target.
func (it *Iter) advanceFlexCursorPast(target string, strict bool) {
	for it.fc.positioned {
		if it.fc.fce != nil && it.fc.kvIdx < it.fc.fce.count {
			fkv := it.fc.fce.kvs[it.fc.kvIdx]
			if fkv.Key > target || (fkv.Key == target && !strict) {
				return
			}
		}
		if err := it.db.flexCursorAdvance(&it.fc); err != nil {
			it.fc.positioned = false
			iterIOPanic(err)
			return
		}
	}
}

// retreatFlexCursorBefore retreats the FlexSpace cursor before target.
// If strict, to keys < target; otherwise to keys <= target.
func (it *Iter) retreatFlexCursorBefore(target string, strict bool) {
	for it.fc.positioned {
		if it.fc.fce != nil && it.fc.kvIdx >= 0 {
			fkv := it.fc.fce.kvs[it.fc.kvIdx]
			if fkv.Key < target || (fkv.Key == target && !strict) {
				return
			}
		}
		if err := it.db.flexCursorRetreat(&it.fc); err != nil {
			it.fc.positioned = false
			iterIOPanic(err)
			return
		}
	}
}

// mergedSeekLE finds the largest key <= target across all sources, resolving
// duplicates by priority and skipping tombstones.
// If strict is true, finds largest key < target. Caller must hold topMutRW.RLock().
func (db *FlexDB) mergedSeekLE(target string, strict bool) (kv *KV, found bool) {
	for {
		active := db.activeMT
		inactive := 1 - active

		var candidates [3]KV
		var have [3]bool

		candidates[0], have[0] = btreeSeekLE(db.memtables[active].bt, target, strict)
		if !db.memtables[inactive].empty {
			candidates[1], have[1] = btreeSeekLE(db.memtables[inactive].bt, target, strict)
		}
		var seekErr error
		candidates[2], have[2], seekErr = db.flexSpaceSeekLE(target, strict)
		if seekErr != nil {
			return
		}

		// Find maximum key
		var maxKey string
		haveMax := false
		for i := 0; i < 3; i++ {
			if have[i] {
				if !haveMax || candidates[i].Key > maxKey {
					maxKey = candidates[i].Key
					haveMax = true
				}
			}
		}
		if !haveMax {
			return
		}

		// Pick highest-priority source at maxKey
		var bestKV KV
		haveBest := false
		for i := 0; i < 3; i++ {
			if have[i] && candidates[i].Key == maxKey {
				if !haveBest {
					bestKV = candidates[i]
					haveBest = true
				}
			}
		}

		if !haveBest || bestKV.isTombstone() {
			target = maxKey
			strict = true
			continue
		}
		found = true
		if bestKV.HasVPtr() {
			kv = &KV{}
			*kv = bestKV
			kv.Key = maxKey
			kv.Value = nil
			return
		}
		val, err := db.resolveVPtr(bestKV)
		if err != nil {
			target = maxKey
			strict = true
			continue
		}
		kv = &KV{Key: maxKey, Value: dupBytes(val), Hlc: bestKV.Hlc}
		return
	}
}

// ====================== Public Iterator Methods ======================

// Seek positions the iterator at the first key >= target.
func (it *Iter) Seek(target string) {
	if it.closed {
		return
	}
	it.releaseIterState()
	db := it.db
	it.initFlexCursorSeekGE(target)

	// Prefetch fast path: both memtables empty -> fill buffer from FlexSpace.
	if db.memtables[0].empty && db.memtables[1].empty {
		it.prefetchFillFlexSpaceOnly()
		if it.pfSpanCount == 0 || !it.servePrefetch() {
			it.valid = false
		}
		return
	}

	it.pKV, it.valid = it.mergedSeekGEFastFlexSpace(target, false)
	it.dir = 1
}

// seekLE positions the iterator at the last key <= target, with backward prefetch.
func (it *Iter) seekLE(target string, strict bool) {
	if it.closed {
		return
	}
	it.releaseIterState()
	db := it.db
	it.initFlexCursorSeekLE(target)

	// Prefetch fast path: both memtables empty -> fill buffer backward from FlexSpace.
	if db.memtables[0].empty && db.memtables[1].empty {
		if strict {
			it.retreatFlexCursorBefore(target, true)
		}
		it.prefetchFillFlexSpaceReverse()
		if it.pfSpanCount == 0 || !it.servePrefetchReverse() {
			it.valid = false
		}
		return
	}

	it.pKV, it.valid = db.mergedSeekLE(target, strict)
	it.dir = -1
	it.valueResolved = true
}

// SeekToFirst positions the iterator at the first key.
func (it *Iter) SeekToFirst() {
	it.Seek("")
}

// SeekToLast positions the iterator at the last key.
func (it *Iter) SeekToLast() {
	if it.closed {
		return
	}
	it.releaseIterState()
	db := it.db
	it.initFlexCursorSeekLE("")

	// Prefetch fast path: both memtables empty -> fill buffer backward from FlexSpace.
	if db.memtables[0].empty && db.memtables[1].empty {
		it.prefetchFillFlexSpaceReverse()
		if it.pfSpanCount == 0 || !it.servePrefetchReverse() {
			it.valid = false
		}
		return
	}

	it.pKV, it.valid = db.mergedSeekLE("", false)
	it.dir = -1
	it.valueResolved = true
}

// Next advances to the next key in ascending order.
func (it *Iter) Next() {
	if !it.valid || it.closed {
		return
	}

	currentHLC := it.db.hlc.Aload()

	// Fast path: serve from prefetch spans.
	// HLC check detects mutations made via it.Put/it.Delete since last fill.
	if it.pfSpanIdx < it.pfSpanCount {
		if it.snapshotHLC == currentHLC {
			// Inline the common case: next entry in current span, not a tombstone.
			// This avoids a function call to servePrefetch (which Go can't inline
			// because it contains loops) on every Next().
			span := &it.pfSpans[it.pfSpanIdx]
			if pos := span.pos; pos < span.end {
				pkv := &span.kvs[pos]                       // one cache-line access (64B KV)
				if pkv.Vptr.Length != tombstoneVPtrLength { // not tombstone
					span.pos = pos + 1
					it.pKV = pkv
					it.valueResolved = false
					it.valid = true
					it.dir = 1
					//it.cntInlineFast++
					return
				}
				span.pos = pos + 1 // skip tombstone so servePrefetch doesn't re-check
			}
			// Tombstone or span boundary - fall through to full loop.
			if it.servePrefetch() {
				//it.cntServePrefetch++
				return
			}
			// Spans exhausted (all remaining were tombstones).
			// Try quick inline refill: if the flex cursor still has entries
			// in the current interval, create a new span without calling
			// prefetchFillFlexSpaceOnly.
			if it.fc.fce != nil && it.fc.kvIdx < it.fc.fce.count {
				count := it.fc.fce.count
				idx := it.fc.kvIdx
				n := count - idx
				if n > iterPreFetchKeyCount {
					n = iterPreFetchKeyCount
				}
				it.pfSpans[0] = prefetchSpan{
					kvs: it.fc.fce.kvs[:count],
					pos: idx,
					end: idx + n,
				}
				it.pfSpanCount = 1
				it.pfSpanIdx = 0
				it.fc.kvIdx = idx + n
				// Serve the first entry from the new span.
				pkv := &it.pfSpans[0].kvs[idx]
				if pkv.Vptr.Length != tombstoneVPtrLength {
					it.pfSpans[0].pos = idx + 1
					it.pKV = pkv
					it.valueResolved = false
					it.valid = true
					it.dir = 1
					//it.cntInlineRefill1++
					return
				}
				it.pfSpans[0].pos = idx + 1
				if it.servePrefetch() {
					//it.cntInlineRefill1++
					return
				}
			}
			// Fall through to full refill.
		} else {
			// HLC changed: invalidate prefetch, fall through to slow path.
			it.pfSpanCount = 0
			it.pfSpanIdx = 0
			//it.cntHLCStale++
		}
	}

	db := it.db
	if it.dir == 1 && it.snapshotHLC == currentHLC && it.snapshotHLC != 0 {
		// Cursor still valid. Try prefetch refill on the FlexSpace-only fast path.
		if db.memtables[0].empty && db.memtables[1].empty {
			// Inline single-interval refill: if the cursor still has entries
			// in its current interval, create a span directly without calling
			// prefetchFillFlexSpaceOnly (avoids function call + loop overhead).
			if it.fc.fce != nil && it.fc.kvIdx < it.fc.fce.count {
				count := it.fc.fce.count
				idx := it.fc.kvIdx
				n := count - idx
				if n > iterPreFetchKeyCount {
					n = iterPreFetchKeyCount
				}
				it.pfSpans[0] = prefetchSpan{
					kvs: it.fc.fce.kvs[:count],
					pos: idx,
					end: idx + n,
				}
				it.pfSpanCount = 1
				it.pfSpanIdx = 0
				it.fc.kvIdx = idx + n
				it.snapshotHLC = currentHLC
				// Serve first entry inline.
				pkv := &it.pfSpans[0].kvs[idx]
				if pkv.Vptr.Length != tombstoneVPtrLength {
					it.pfSpans[0].pos = idx + 1
					it.pKV = pkv
					it.valueResolved = false
					it.valid = true
					it.dir = 1
					//it.cntInlineRefill2++
					return
				}
				it.pfSpans[0].pos = idx + 1
				if it.servePrefetch() {
					//it.cntInlineRefill2++
					return
				}
				// All remaining were tombstones; fall through to full refill.
			}
			//it.cntFullRefill++
			it.prefetchFillFlexSpaceOnly()
			it.snapshotHLC = currentHLC
			if it.pfSpanCount == 0 || !it.servePrefetch() {
				it.valid = false
			}
			return
		}

		// Non-empty memtables: single merged seek with cursor reuse.
		curKey := it.pKV.Key // save before re-seek overwrites pKV
		it.pKV, it.valid = it.mergedSeekGEFastFlexSpace(curKey, true)
		return
	}

	// Slow path: HLC changed (mutation happened) or direction changed.
	curKey := it.pKV.Key // save before re-seek overwrites pKV

	// Re-seek FlexSpace cursor from scratch.
	//if it.snapshotHLC == 0 {
	//it.cntSnapshotZero++
	//}
	//it.cntSlowPath++
	it.releaseIterState()
	it.initFlexCursorSeekGE(curKey)

	// Try prefetch on fresh cursor if memtables are empty.
	if db.memtables[0].empty && db.memtables[1].empty {
		// Position cursor past curKey (strict > curKey).
		it.positionFlexCursorForSeek(curKey, true)
		it.prefetchFillFlexSpaceOnly()
		if it.pfSpanCount == 0 || !it.servePrefetch() {
			it.valid = false
		}
		it.dir = 1
		return
	}

	it.pKV, it.valid = it.mergedSeekGEFastFlexSpace(curKey, true)
	it.dir = 1
}

// Prev moves to the previous key in descending order.
func (it *Iter) Prev() {
	if !it.valid || it.closed {
		return
	}

	currentHLC := it.db.hlc.Aload()

	// Fast path: serve from reverse prefetch spans.
	// HLC check detects mutations made via it.Put/it.Delete since last fill.
	if it.pfSpanIdx < it.pfSpanCount && it.dir == -1 {
		if it.snapshotHLC == currentHLC {
			if it.servePrefetchReverse() {
				return
			}
			// Spans exhausted; fall through to refill.
		} else {
			// HLC changed: invalidate prefetch, fall through to slow path.
			it.pfSpanCount = 0
			it.pfSpanIdx = 0
		}
	}

	db := it.db
	curKey := it.pKV.Key // save before any re-seek overwrites pKV

	if it.dir == -1 && it.snapshotHLC == currentHLC && it.snapshotHLC != 0 {
		// Cursor still valid. Try prefetch refill on the FlexSpace-only fast path.
		if db.memtables[0].empty && db.memtables[1].empty {
			it.prefetchFillFlexSpaceReverse()
			it.snapshotHLC = currentHLC
			if it.pfSpanCount == 0 || !it.servePrefetchReverse() {
				it.valid = false
			}
			return
		}

		// Non-empty memtables: single merged seek.
		it.pKV, it.valid = db.mergedSeekLE(curKey, true)
		it.valueResolved = true
		return
	}

	// Slow path: HLC changed (mutation happened) or direction changed.
	// Re-seek FlexSpace cursor backward from scratch.
	it.releaseIterState()
	it.initFlexCursorSeekLE(curKey)

	// Position cursor before curKey (strict < curKey).
	if it.fc.positioned && it.fc.fce != nil && it.fc.kvIdx >= 0 {
		fkv := it.fc.fce.kvs[it.fc.kvIdx]
		if fkv.Key >= curKey {
			// Cursor is at or past curKey; retreat past it.
			it.retreatFlexCursorBefore(curKey, true)
		}
	}

	// Try prefetch on fresh cursor if memtables are empty.
	if db.memtables[0].empty && db.memtables[1].empty {
		it.prefetchFillFlexSpaceReverse()
		if it.pfSpanCount == 0 || !it.servePrefetchReverse() {
			it.valid = false
		}
		it.dir = -1
		return
	}

	it.pKV, it.valid = db.mergedSeekLE(curKey, true)
	it.dir = -1
	it.valueResolved = true
}

// Close marks the iterator as closed and releases cursor state.
// The transaction manages lock lifetime, not the iterator.
func (it *Iter) Close() {
	if it.closed {
		return
	}
	it.releaseIterState()
	it.valid = false
	it.closed = true
}

// Valid returns true if the iterator is positioned at a valid key.
func (it *Iter) Valid() bool { return it.valid }

// KV returns the current *KV with any inline value resolved (lazy-copied
// from cache memory). It does not fetch large values from the VLOG.
// The returned pointer is owned by the iterator
// and must not be retained past the next Next()/Prev()/Seek() call.
// Copy Key and Value if you need them to outlive the iterator step.
func (it *Iter) KV() *KV {
	if it.pKV == nil || !it.valid {
		return nil
	}

	// valueResolved does lazy value copying. We don't want
	// to return cache memory to the user, but if they
	// don't need a copy (just iterating keys), we avoid
	// the expense of copying until Vin (or Vel) is called.
	if !it.valueResolved && it.pKV.Value != nil {
		it.valBuf = reuseAppend(it.valBuf, it.pKV.Value)
		it.pKV.Value = it.valBuf
		it.valueResolved = true
	}

	return it.pKV
}

// GetAnySize returns values large or small, if available.
func (it *Iter) GetAnySize() (key string, val []byte, found bool, err error) {
	if !it.valid || it.pKV == nil {
		return
	}
	found = true
	key = it.pKV.Key
	if !it.valueResolved && it.pKV.Value != nil {
		it.valBuf = reuseAppend(it.valBuf, it.pKV.Value)
		it.pKV.Value = it.valBuf
		it.valueResolved = true
	}
	val = it.pKV.Value
	if it.pKV.HasVPtr() {
		val, err = it.FetchV()
	}
	return
}

// Key returns the current key. On the fast path this is a direct pointer
// into cache memory (zero-copy). Call dupBytes(it.Key) if you need to
// keep a copy beyond the next Next()/Prev() call.
func (it *Iter) Key() string {
	if it.pKV == nil {
		return ""
	}
	return it.pKV.Key
}

// Vin returns the current value for _inline_ values. For large values
// stored in VLOG, Vin returns nil. If you get back nil,
// you must use Large() to check if the value is actually large, and
// then FetchV() to fetch it on demand if so. Or just use Vel() below to find
// out what you've got in one call.
//
// On the fast path (empty memtables), the first call copies from the
// cache reference into valBuf. Subsequent calls return valBuf directly.
func (it *Iter) Vin() (val []byte) {
	if it.pKV == nil {
		return nil
	}

	// valueResolved does lazy value copying. We don't want
	// to return cache memory to the user, but if they
	// don't need a copy (just iterating keys), we avoid
	// the expense of copying until Vin (or Vel) is called.
	if !it.valueResolved && it.pKV.Value != nil {
		it.valBuf = reuseAppend(it.valBuf, it.pKV.Value)
		it.pKV.Value = it.valBuf
		it.valueResolved = true
	}
	return it.pKV.Value
}

// Vel can be more ergonomic than Vin. Vel returns the
// current inline Value as well as the two important
// flags to tell you how to interpret the zero len(val)
// situation: whether the value is truly empty, or
// just large and sitting in the VLOG waiting for
// a separate FetchV call.
//
// Vel is a mnemonic for the returns: value, empty, large.
//
// Plus it's fun to say with your best Young Frankenstein
// style faux German accent (Gene Wilder, Mel Brooks 1974)
//
// "Vel, vel, vel... vhat have ve here? A large value??"
func (it *Iter) Vel() (val []byte, empty, large bool) {
	if it.pKV == nil {
		empty = true
		return
	}
	large = it.pKV.HasVPtr()
	if !it.valueResolved && it.pKV.Value != nil {
		it.valBuf = reuseAppend(it.valBuf, it.pKV.Value)
		it.pKV.Value = it.valBuf
		it.valueResolved = true
	}
	val = it.pKV.Value
	return
}

// Hlc returns the HLC timestamp of the current key-value pair.
func (it *Iter) Hlc() HLC {
	if it.pKV == nil {
		return 0
	}
	return it.pKV.Hlc
}

// Large returns false if the current value is stored inline
// (small value). When true, the value is stored in the VLOG and
// must be fetched via FetchV(). Iteration remains
// cheap until you really need to see the large value.
func (it *Iter) Large() bool {
	if it.pKV == nil {
		return false
	}
	return it.pKV.HasVPtr()
}

// FetchV fetches the current iterator value
// from the VLOG if the iterator points to a large value.
// For inline values, this is equivalent to Vin(). Returns the value
// bytes and any error from the VLOG read. If the value is
// inline and not large, it will still be returned
// (and the error will be nil).
func (it *Iter) FetchV() ([]byte, error) {
	if !it.valid || it.pKV == nil {
		return nil, nil
	}
	if !it.pKV.HasVPtr() {
		return it.pKV.Value, nil
	}
	return it.db.resolveVPtr(KV{Vptr: it.pKV.Vptr})
}

// ====================== Callback-based iteration ======================

// iterResolvedValue returns the value for the current iterator position,
// resolving VLOG pointers if needed.
func (it *Iter) iterResolvedValue() []byte {
	if it.pKV == nil {
		return nil
	}
	if !it.pKV.HasVPtr() {
		return it.pKV.Value
	}
	val, err := it.db.resolveVPtr(KV{Vptr: it.pKV.Vptr})
	if err != nil {
		return nil
	}
	return val
}

// Ascend/Descend/AscendRange/DescendRange are available as methods
// on ReadOnlyDB and WritableDB inside View/Update transactions.
// See tx.go for the transaction-based iteration API.
