package yogadb

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"
)

// ====================== Interval cache ======================
//
// The interval cache is a critical data structure in FlexDB.
// It sits between the sparse KV index and FlexSpace, caching
// decoded KV intervals so that lookups avoid re-reading and
// re-decoding from disk. The cache uses 1024 hash-partitioned
// shards with CLOCK replacement eviction and reference counting.
//
// Lookup path:
//      Get(key) ->
//      sparse index finds anchor ->
//      cache partition (by key hash) ->
//      getEntry() (cache hit or FlexSpace load) ->
//      FindKeyEQ (linear scan with fingerprints) -> value
//

type dbAnchor struct {
	key string // smallest key in this interval ("" = sentinel null key)

	// loff is the relative loff; actual loff = loff + accumulated shift from parents
	// loff was promoted to int64 since we saw silent
	// overflow bugs with the original uint32. See the note
	// at the top of sparseindextree.go.
	loff int64

	psize       uint32 // packed size of this interval in FlexSpace
	unsorted    uint8  // count of unsorted appended KVs
	partitionID int    // cached cachePartitionID(key), set once at creation
	fce         *intervalCacheEntry
}

// loadFce atomically loads anchor.fce. Safe for lock-free reads.
func (a *dbAnchor) loadFce() *intervalCacheEntry {
	return (*intervalCacheEntry)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&a.fce))))
}

// storeFce atomically stores anchor.fce. Use under partition lock.
func (a *dbAnchor) storeFce(fce *intervalCacheEntry) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&a.fce)), unsafe.Pointer(fce))
}

type intervalCacheEntry struct {
	anchor    *dbAnchor
	kvs       []KV                    // decoded KV slice (sorted when unsorted==0)
	fps       []uint16                // fingerprints per KV
	size      int                     // sum of kvSizeApprox for all kvs
	count     int                     // same as len(kvs)
	frag      bool                    // needs defrag
	dirty     bool                    // modified in cache, not yet written to FlexSpace
	dirtyNode *memSparseIndexTreeNode // leaf node containing anchor (set when dirty=true)
	access    int32                   // clock chance counter (atomic)
	refcnt    int32                   // active users (atomic)
	loading   bool                    // being loaded
	loadErr   error                   // error from loadInterval (nil on success)
	prev      *intervalCacheEntry
	next      *intervalCacheEntry
}

type intervalCachePartition struct {
	db   *FlexDB
	mu   sync.Mutex
	cap  int64
	size int64
	tick *intervalCacheEntry // clock pointer
}

type intervalCache struct {
	db         *FlexDB
	cap        int64
	partitions [intervalCachePartitionCount]intervalCachePartition
}

func newCache(db *FlexDB, capMB uint64) *intervalCache {
	c := &intervalCache{db: db, cap: int64(capMB << 20)}
	for i := range c.partitions {
		c.partitions[i].db = db
		c.partitions[i].cap = c.cap / intervalCachePartitionCount
	}
	return c
}

func (c *intervalCache) getPartition(anchor *dbAnchor) *intervalCachePartition {
	return &c.partitions[anchor.partitionID]
}

// flushDirtyPages writes all dirty cache entries to FlexSpace via Overwrite.
// It walks the sparse index tree leaf-by-leaf to compute correct absolute loffs,
// avoiding stale dirtyLoff values that can occur after splits shift offsets.
func (c *intervalCache) flushDirtyPages() {
	if c.db == nil {
		return
	}
	tree := c.db.tree
	if tree == nil {
		return
	}
	var flushOverwrites, flushUpdates int
	var flushUpdateGarbage int64
	// Walk the sparse index tree leaf linked list.
	leaf := tree.leafHead
	for leaf != nil {
		// Compute shift for this leaf by walking up to root.
		shift := int64(0)
		n := leaf
		for n.parent != nil {
			shift += n.parent.children[n.parentID].shift
			n = n.parent
		}
		// Check each anchor in this leaf.
		for i := 0; i < leaf.count; i++ {
			anchor := leaf.anchors[i]
			if anchor == nil {
				continue
			}
			fce := anchor.loadFce()
			if fce == nil || !fce.dirty {
				continue
			}
			if anchor.psize == 0 {
				fce.dirty = false
				continue
			}
			absLoff := uint64(anchor.loff + shift)
			buf := slottedPageEncodePadded(fce.kvs[:fce.count], int(anchor.psize))
			if len(buf) == int(anchor.psize) {
				// Normal case: padded page fits exactly.
				err := c.db.ff.Overwrite(buf, absLoff, uint64(anchor.psize))
				if err != nil {
					panicf("flushDirtyPages: anchor.loff=%d shift=%d absLoff=%d psize=%d maxLoff=%d count=%d key=%q err=%v",
						anchor.loff, shift, absLoff, anchor.psize, c.db.ff.tree.MaxLoff, fce.count, anchor.key, err)
				}
				flushOverwrites++
			} else {
				// Content exceeds current psize. This happens when:
				// (a) HLC delta overflow inflates encoding, or
				// (b) page was tight (from initial flush or post-vacuum) and grew.
				// Resize to slottedPageMaxSize to provide in-place overwrite
				// capacity for future updates. If content exceeds that, use tight.
				growTarget := slottedPageMaxSize
				if len(buf) > slottedPageMaxSize {
					growTarget = len(buf)
				}
				padBuf := slottedPageEncodePadded(fce.kvs[:fce.count], growTarget)
				oldPsize := anchor.psize
				_, err := c.db.ff.Update(padBuf, absLoff, uint64(len(padBuf)), uint64(oldPsize))
				if err != nil {
					panicf("flushDirtyPages Update: anchor.loff=%d absLoff=%d psize=%d bufLen=%d err=%v",
						anchor.loff, absLoff, oldPsize, len(padBuf), err)
				}
				newPsize := uint32(len(padBuf))
				anchor.psize = newPsize

				// The FlexTree shifted subsequent loffs by (newPsize - oldPsize).
				// Propagate the same shift in the anchor tree so that subsequent
				// anchors' absLoff values remain correct.
				if newPsize != oldPsize {
					shiftDelta := int64(newPsize) - int64(oldPsize)
					nh := memSparseIndexTreeHandler{node: leaf, idx: i, shift: shift}
					nh.shiftUpPropagate(shiftDelta)
				}
				flushUpdates++
				flushUpdateGarbage += int64(oldPsize)
			}
			fce.dirty = false
		}
		leaf = leaf.next
	}
	//if flushUpdates > 0 || flushOverwrites > 0 {
	//	alwaysPrintf("flushDirtyPages: overwrites=%d updates=%d updateGarbage=%d",
	//		flushOverwrites, flushUpdates, flushUpdateGarbage)
	//}
}

// allocEntryForNewAnchor creates a new (empty) cache entry for a freshly created anchor.
func (p *intervalCachePartition) allocEntryForNewAnchor(anchor *dbAnchor) *intervalCacheEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	fce := &intervalCacheEntry{anchor: anchor}
	anchor.storeFce(fce)
	p.insertIntoClock(fce)
	atomic.AddInt32(&fce.refcnt, 1)
	atomic.StoreInt32(&fce.access, intervalCacheEntryChance)
	p.size += int64(32) // base overhead
	return fce
}

func (p *intervalCachePartition) insertIntoClock(fce *intervalCacheEntry) {
	if p.tick == nil {
		p.tick = fce
		fce.prev = fce
		fce.next = fce
	} else {
		fce.prev = p.tick.prev
		fce.next = p.tick
		fce.prev.next = fce
		fce.next.prev = fce
	}
}

func (p *intervalCachePartition) removeFromClock(fce *intervalCacheEntry) {
	if fce.next == fce {
		p.tick = nil
	} else {
		if p.tick == fce {
			p.tick = fce.next
		}
		fce.prev.next = fce.next
		fce.next.prev = fce.prev
	}
}

func (p *intervalCachePartition) releaseEntry(fce *intervalCacheEntry) {
	atomic.AddInt32(&fce.refcnt, -1)
}

// freeEntry removes and frees a cache entry. Returns the freed size. Caller holds p.mu.
func (p *intervalCachePartition) freeEntry(fce *intervalCacheEntry) int64 {
	if fce.dirty {
		p.flushDirtyEntry(fce)
	}
	freed := int64(32 + fce.size)
	if fce.anchor != nil {
		fce.anchor.storeFce(nil)
	}
	p.removeFromClock(fce)
	return freed
}

// flushDirtyEntry writes a dirty cache entry to FlexSpace via Overwrite.
// Computes the absolute loff by walking from the leaf node to the root.
// Caller holds p.mu.
func (p *intervalCachePartition) flushDirtyEntry(fce *intervalCacheEntry) {
	if !fce.dirty || fce.anchor == nil {
		return
	}
	anchor := fce.anchor
	if anchor.psize == 0 {
		fce.dirty = false
		return
	}
	// Compute absolute loff by walking from leaf to root.
	shift := int64(0)
	n := fce.dirtyNode
	for n != nil && n.parent != nil {
		shift += n.parent.children[n.parentID].shift
		n = n.parent
	}
	absLoff := uint64(anchor.loff + shift)
	buf := slottedPageEncodePadded(fce.kvs[:fce.count], int(anchor.psize))
	if len(buf) == int(anchor.psize) {
		err := p.db.ff.Overwrite(buf, absLoff, uint64(anchor.psize))
		if err != nil {
			panicf("flushDirtyEntry: %v", err)
		}
	} else {
		// Content exceeds current psize: see flushDirtyPages for explanation.
		growTarget := slottedPageMaxSize
		if len(buf) > slottedPageMaxSize {
			growTarget = len(buf)
		}
		padBuf := slottedPageEncodePadded(fce.kvs[:fce.count], growTarget)
		oldPsize := anchor.psize
		//alwaysPrintf("flushDirtyEntry Update: oldPsize=%d newSize=%d key=%q",
		//	oldPsize, len(padBuf), anchor.key)
		_, err := p.db.ff.Update(padBuf, absLoff, uint64(len(padBuf)), uint64(oldPsize))
		if err != nil {
			panicf("flushDirtyEntry Update: %v", err)
		}
		newPsize := uint32(len(padBuf))
		anchor.psize = newPsize

		// Propagate size change to anchor tree (same as flushDirtyPages).
		if newPsize != oldPsize && fce.dirtyNode != nil {
			shiftDelta := int64(newPsize) - int64(oldPsize)
			// Find anchor's index in the leaf node.
			idx := -1
			for j := 0; j < fce.dirtyNode.count; j++ {
				if fce.dirtyNode.anchors[j] == anchor {
					idx = j
					break
				}
			}
			if idx >= 0 {
				nh := memSparseIndexTreeHandler{node: fce.dirtyNode, idx: idx, shift: shift}
				nh.shiftUpPropagate(shiftDelta)
			}
		}
	}
	fce.dirty = false
	fce.dirtyNode = nil
}

// calibrate evicts entries until size <= cap. Caller holds p.mu.
func (p *intervalCachePartition) calibrate() {
	if p.size <= p.cap {
		return
	}
	for p.size > p.cap && p.tick != nil {
		victim := p.tick
		start := victim // save starting point for full-circle detection
		for atomic.LoadInt32(&victim.refcnt) > 0 || atomic.LoadInt32(&victim.access) > 0 {
			if atomic.LoadInt32(&victim.refcnt) == 0 {
				atomic.AddInt32(&victim.access, -1)
			}
			victim = victim.next
			p.tick = victim
			if victim == start {
				break // full circle, give up
			}
		}
		if atomic.LoadInt32(&victim.refcnt) > 0 {
			break
		}
		freed := p.freeEntry(victim)
		p.size -= freed
	}
}

// getEntry returns the cache entry for anchor, loading it from FlexSpace if necessary.
func (p *intervalCachePartition) getEntry(anchor *dbAnchor, anchorLoff uint64, db *FlexDB) (*intervalCacheEntry, error) {
	p.mu.Lock()
	fce := anchor.loadFce()
	if fce != nil {
		atomic.AddInt32(&fce.refcnt, 1)
		isLoading := fce.loading
		p.mu.Unlock()
		if isLoading {
			// spin until loaded
			for {
				p.mu.Lock()
				loading := fce.loading
				p.mu.Unlock()
				if !loading {
					break
				}
			}
		}
		atomic.StoreInt32(&fce.access, intervalCacheEntryChance)
		return fce, fce.loadErr
	}
	// miss: allocate and load
	fce = &intervalCacheEntry{anchor: anchor}
	anchor.storeFce(fce)
	p.insertIntoClock(fce)
	atomic.AddInt32(&fce.refcnt, 1)
	fce.loading = true
	p.size += int64(32)
	p.mu.Unlock()

	// load interval from FlexSpace (without lock)
	fce.loadErr = p.loadInterval(fce, anchor, anchorLoff, db)

	p.mu.Lock()
	p.size += int64(fce.size)
	fce.loading = false
	atomic.StoreInt32(&fce.access, intervalCacheEntryChance)
	p.calibrate()
	p.mu.Unlock()
	return fce, fce.loadErr
}

// getEntryUnsorted returns the cache entry only if already loaded,
// OR forces a load if the unsorted quota is exceeded.
func (p *intervalCachePartition) getEntryUnsorted(anchor *dbAnchor, anchorLoff uint64, db *FlexDB) (*intervalCacheEntry, error) {
	// Atomic load - no lock needed just to check if fce is populated.
	fce := anchor.loadFce()
	if fce != nil || anchor.unsorted >= flexdbUnsortedWriteQuota || anchor.psize >= flexdbSparseIntervalSize {
		return p.getEntry(anchor, anchorLoff, db)
	}
	return nil, nil
}

// loadInterval reads the FlexSpace interval and populates fce.
func (p *intervalCachePartition) loadInterval(fce *intervalCacheEntry, anchor *dbAnchor, anchorLoff uint64, db *FlexDB) error {
	fce.kvs = fce.kvs[:0]
	fce.fps = fce.fps[:0]
	fce.size = 0
	fce.count = 0
	if anchor.psize == 0 {
		return nil
	}
	itvbuf := make([]byte, anchor.psize)
	var frag uint64
	n, fragOut, err := db.ff.ReadFragmentation(itvbuf, anchorLoff, uint64(anchor.psize))
	if err != nil {
		return fmt.Errorf("flexdb: loadInterval ReadFragmentation at loff %d psize %d: %w", anchorLoff, anchor.psize, err)
	}
	if n != int(anchor.psize) {
		return fmt.Errorf("flexdb: loadInterval short read at loff %d: got %d, want %d", anchorLoff, n, anchor.psize)
	}
	frag = fragOut

	src := itvbuf
	if slottedPageIsSlotted(src) {
		// Decode slotted page.
		kvs, consumed, err := slottedPageDecode(src)
		if err == nil {
			for _, kv := range kvs {
				fce.kvs = append(fce.kvs, kv)
				fce.fps = append(fce.fps, fingerprint(kvCRC32(kv.Key)))
				fce.size += kvSizeApprox(&kv)
				fce.count++
			}
			src = src[consumed:]
		} else {
			src = nil // corrupt page, skip
		}
	}
	// All KV.SLOT_BLOCKS data should be slotted page format.
	// kv128 format is no longer written to KV.SLOT_BLOCKS.
	if len(src) > 0 {
		panicf("loadInterval: unexpected non-slotted data at loff=%d, %d trailing bytes, first 16 bytes: %x",
			anchorLoff, len(src), src[:min(len(src), 16)])
	}

	// If unsorted, sort and dedup.
	// SliceStable preserves the relative order of equal keys, which is critical:
	// unsorted appends (overwrites) come after sorted entries in FlexSpace,
	// and intervalCacheDedup keeps the last occurrence, so stability ensures
	// the latest value is kept.
	if anchor.unsorted > 0 {
		sort.SliceStable(fce.kvs, func(i, j int) bool {
			return kvLess(fce.kvs[i], fce.kvs[j])
		})
		fce.kvs, fce.fps, fce.size = intervalCacheDedup(fce.kvs)
		fce.count = len(fce.kvs)
	}

	// fragmentation hint
	if frag > uint64(fce.count>>1) {
		fce.frag = true
	}
	return nil
}

// intervalCacheDedup deduplicates a sorted KV slice (keeps highest HLC per key).
// PRE: kvs must be sorted by ascending Key.
func intervalCacheDedup(kvs []KV) ([]KV, []uint16, int) {
	if len(kvs) == 0 {
		return kvs, nil, 0
	}
	out := kvs[:0]
	i := 0
	var prevKey string
	hasPrev := false
	for i < len(kvs) {
		if hasPrev {
			if prevKey > kvs[i].Key {
				panicf("internal logic error: PRE-condition violated, kvs was not sorted by key! prevKey=%q > key=%q; kvs='%#v'", prevKey, kvs[i].Key, kvs)
			}
		}

		best := i
		j := i + 1
		for j < len(kvs) && kvs[i].Key == kvs[j].Key {
			if kvs[j].Hlc > kvs[best].Hlc {
				best = j
			}
			j++
		}
		out = append(out, kvs[best])
		if j < len(kvs) {
			prevKey = kvs[i].Key
			hasPrev = true
		}
		i = j
	}
	fps := make([]uint16, len(out))
	size := 0
	for i, kv := range out {
		fps[i] = fingerprint(kvCRC32(kv.Key))
		size += kvSizeApprox(&kv)
	}
	return out, fps, size
}

// intervalCacheEntryFindKeyGE: binary search for first position >= key. Returns (idx, exact).
func intervalCacheEntryFindKeyGE(fce *intervalCacheEntry, key string) (int, bool) {
	lo, hi := 0, fce.count
	for lo < hi {
		mid := (lo + hi) >> 1
		if key > fce.kvs[mid].Key {
			lo = mid + 1
		} else if key < fce.kvs[mid].Key {
			hi = mid
		} else {
			return mid, true
		}
	}
	return lo, false
}

// intervalCacheEntryFindKeyEQ: linear scan using fingerprints for exact match.
func intervalCacheEntryFindKeyEQ(fce *intervalCacheEntry, key string) (int, bool) {
	fp := fingerprint(kvCRC32(key))
	for i := 0; i < fce.count; i++ {
		if fce.fps[i] == fp && key == fce.kvs[i].Key {
			return i, true
		}
	}
	return -1, false
}

func (p *intervalCachePartition) cacheEntryInsert(fce *intervalCacheEntry, kv KV, idx int) {
	fce.kvs = append(fce.kvs, KV{})
	fce.fps = append(fce.fps, 0)
	copy(fce.kvs[idx+1:], fce.kvs[idx:])
	copy(fce.fps[idx+1:], fce.fps[idx:])
	fce.kvs[idx] = kv
	fce.fps[idx] = fingerprint(kvCRC32(kv.Key))
	approx := kvSizeApprox(&kv)
	fce.size += approx
	fce.count++
	p.mu.Lock()
	p.size += int64(approx)
	p.mu.Unlock()
}

func (p *intervalCachePartition) cacheEntryReplace(fce *intervalCacheEntry, kv KV, idx int) {
	old := fce.kvs[idx]
	oldSz := kvSizeApprox(&old)
	newSz := kvSizeApprox(&kv)
	fce.kvs[idx] = kv
	fce.fps[idx] = fingerprint(kvCRC32(kv.Key))
	diff := newSz - oldSz
	fce.size += diff
	p.mu.Lock()
	p.size += int64(diff)
	p.mu.Unlock()
}

func (p *intervalCachePartition) cacheEntryDelete(fce *intervalCacheEntry, idx int) {
	old := fce.kvs[idx]
	sz := kvSizeApprox(&old)
	copy(fce.kvs[idx:], fce.kvs[idx+1:fce.count])
	copy(fce.fps[idx:], fce.fps[idx+1:fce.count])
	fce.kvs = fce.kvs[:fce.count-1]
	fce.fps = fce.fps[:fce.count-1]
	fce.size -= sz
	fce.count--
	p.mu.Lock()
	p.size -= int64(sz)
	p.mu.Unlock()
}

func (c *intervalCache) destroyAll() {
	for i := range c.partitions {
		p := &c.partitions[i]
		p.mu.Lock()
		p.tick = nil // let GC clean up
		p.mu.Unlock()
	}
}
