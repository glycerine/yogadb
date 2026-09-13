package yogadb

import (
	"sync"
	"sync/atomic"
)

const (
	wormPageBits    = 11
	wormPageSize    = 1 << wormPageBits
	wormPageMask    = wormPageSize - 1
	wormBlockBits   = 10
	wormBlockSize   = 1 << wormBlockBits
	wormBlockMask   = wormBlockSize - 1
	wormDirSize     = 1024
	wormLeafDirSize = 1 << 16
)

type wormRef uint32

type wormConfig struct {
	leafCapacity int
}

/* why is this version of the wormhole the fastest we found (thus far)?

The restored fast wormhole is fast mainly because the
point index is optional and dormant until explicitly built.

Key differences from the previous plain-map / dirty-tail wormhole:

 - wormhole.point is now atomic.Pointer[pointIndex], not a permanent top-level map[string]wormRef.
 - If point == nil, Put does no point-map maintenance at all. updatePointIndexPut becomes a no-op.
 - The mixed benchmark does not call BuildPointIndex, so it runs in this no-point-index mode:
     - writes update only the ordered leaf structure;
     - reads use cached leaf lookup plus in-leaf hint search;
     - no Go map assign, no dirty refs, no point-index flushing.

 - When BuildPointIndex is called, it builds an immutable base map[string]wormRef.
 - After that:
     - exclusive x=true writes mutate base directly;
     - shared x=false writes use a sync.Map delta overlay;
     - reads check delta first, then base.

 - The old experimental versions paid point-index costs during
   mixed writes even when the workload mostly did not need them.
   That was the big tax.

So the 63 ns/op mixed result is not from a cleverer point map.
It is from avoiding the point map entirely in that workload
and leaning on the sorted leaf/read-cache path. That is a
pretty important insight.
*/

// wormhole is our memtable. The name comes from the wormhole data structure
// used in the original C FlexDB implementation for the memtable. We
// have optimized our Go version of wormhole to the point where,
// besides the name, the internal algorithms are probably
// un-recognizable and completely different. But still, its a good name.
//
// wormhole now benchmarks better than the alternative memtables we tried:
// tidwall's Btree, pebble's skiplist. It is still 20% slower than our
// own keyStable arena at after-AllowReads() writes, but much better
// at mixed reads and writes and that post-bulk-load environment is
// where mixed reads and writes will happen.
type wormhole struct {
	mu        sync.RWMutex
	leaves    []*wormLeaf
	tail      *wormLeaf
	store     wormKVstore
	point     atomic.Pointer[pointIndex]
	leafDir   []*wormLeaf
	leafCap   int
	liveKeys  atomic.Int64
	readCache atomic.Pointer[wormLeaf]
}

type wormLeaf struct {
	mu           sync.RWMutex
	anchor       string
	high         string
	anchorPrefix uint64
	highPrefix   uint64
	prev         *wormLeaf
	next         *wormLeaf
	items        []wormRef
	metas        []wormKeyMeta
	readHint     atomic.Int64
}

type wormKeyMeta struct {
	key    string
	prefix uint64
}

type pointIndex struct {
	base  map[string]wormRef
	delta *sync.Map
}

type pointMutation struct {
	idx     wormRef
	deleted bool
}

type wormKVstore struct {
	next   atomic.Int64
	blocks [wormDirSize]atomic.Pointer[wormPageBlock]
}

type wormPageBlock struct {
	pages [wormBlockSize]atomic.Pointer[wormKvPage]
}

type wormKvPage struct {
	kvs [wormPageSize]KV
}

func newWormhole(opts wormConfig) *wormhole {
	leafCap := opts.leafCapacity
	if leafCap <= 0 {
		leafCap = 256
	}
	if leafCap < 4 {
		leafCap = 4
	}
	l := &wormLeaf{
		items: make([]wormRef, 0, leafCap+1),
		metas: make([]wormKeyMeta, 0, leafCap+1),
	}
	m := &wormhole{
		leaves:  []*wormLeaf{l},
		tail:    l,
		leafDir: make([]*wormLeaf, wormLeafDirSize),
		leafCap: leafCap,
	}
	m.store.init()
	return m
}

func (s *wormKVstore) init() {}

func (m *wormhole) clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	leafCap := m.leafCap
	if leafCap <= 0 {
		leafCap = 256
	}
	if leafCap < 4 {
		leafCap = 4
	}
	m.leafCap = leafCap

	m.store.clear()
	m.point.Store(nil)
	m.readCache.Store(nil)
	m.liveKeys.Store(0)
	if len(m.leafDir) != wormLeafDirSize {
		m.leafDir = make([]*wormLeaf, wormLeafDirSize)
	} else {
		clear(m.leafDir)
	}

	var first *wormLeaf
	if len(m.leaves) > 0 {
		first = m.leaves[0]
	}
	if first == nil {
		first = &wormLeaf{
			items: make([]wormRef, 0, leafCap+1),
			metas: make([]wormKeyMeta, 0, leafCap+1),
		}
	}

	for i, l := range m.leaves {
		if l == nil {
			continue
		}
		l.mu.Lock()
		l.anchor = ""
		l.high = ""
		l.anchorPrefix = 0
		l.highPrefix = 0
		l.prev = nil
		l.next = nil
		clear(l.metas)
		l.items = l.items[:0]
		l.metas = l.metas[:0]
		l.readHint.Store(0)
		l.mu.Unlock()
		if i > 0 {
			m.leaves[i] = nil
		}
	}

	if cap(first.items) < leafCap+1 {
		first.items = make([]wormRef, 0, leafCap+1)
	}
	if cap(first.metas) < leafCap+1 {
		first.metas = make([]wormKeyMeta, 0, leafCap+1)
	}
	if len(m.leaves) == 0 {
		m.leaves = append(m.leaves, first)
	} else {
		m.leaves[0] = first
		m.leaves = m.leaves[:1]
	}
	m.tail = first
}

func (s *wormKVstore) clear() {
	n := int(s.next.Load())
	for idx := 0; idx < n; {
		pageIdx := idx >> wormPageBits
		block := s.blocks[pageIdx>>wormBlockBits].Load()
		pageRemainder := wormPageSize - (idx & wormPageMask)
		if block == nil {
			idx += pageRemainder
			continue
		}
		page := block.pages[pageIdx&wormBlockMask].Load()
		if page == nil {
			idx += pageRemainder
			continue
		}
		start := idx & wormPageMask
		end := wormPageSize
		if remaining := n - idx; remaining < end-start {
			end = start + remaining
		}
		clear(page.kvs[start:end])
		idx += end - start
	}
	s.next.Store(0)
}

func (s *wormKVstore) append(kv KV) wormRef {
	idx := int(s.next.Add(1) - 1)
	pageIdx := idx >> wormPageBits
	offset := idx & wormPageMask
	blockIdx := pageIdx >> wormBlockBits
	if blockIdx >= wormDirSize {
		panic("wormhole: wormKVstore capacity exceeded")
	}
	block := s.blocks[blockIdx].Load()
	if block == nil {
		newBlock := &wormPageBlock{}
		if s.blocks[blockIdx].CompareAndSwap(nil, newBlock) {
			block = newBlock
		} else {
			block = s.blocks[blockIdx].Load()
		}
	}
	pageSlot := pageIdx & wormBlockMask
	page := block.pages[pageSlot].Load()
	if page == nil {
		newPage := &wormKvPage{}
		if block.pages[pageSlot].CompareAndSwap(nil, newPage) {
			page = newPage
		} else {
			page = block.pages[pageSlot].Load()
		}
	}
	page.kvs[offset] = kv
	return wormRef(idx)
}

func (s *wormKVstore) get(ref wormRef) KV {
	idx := int(ref)
	pageIdx := idx >> wormPageBits
	block := s.blocks[pageIdx>>wormBlockBits].Load()
	page := block.pages[pageIdx&wormBlockMask].Load()
	return page.kvs[idx&wormPageMask]
}

func (s *wormKVstore) key(ref wormRef) string {
	idx := int(ref)
	pageIdx := idx >> wormPageBits
	block := s.blocks[pageIdx>>wormBlockBits].Load()
	page := block.pages[pageIdx&wormBlockMask].Load()
	return page.kvs[idx&wormPageMask].Key
}

func (m *wormhole) Len() int64 {
	return m.liveKeys.Load()
}

func (m *wormhole) appendKV(kv KV) wormRef {
	return m.store.append(kv)
}

func wormKeyPrefix(key string) uint64 {
	n := len(key)
	if n > 8 {
		n = 8
	}
	var prefix uint64
	for i := 0; i < n; i++ {
		prefix = (prefix << 8) | uint64(key[i])
	}
	return prefix << uint((8-n)*8)
}

func (l *wormLeaf) len() int {
	return len(l.items)
}

func (l *wormLeaf) entryAt(pos int) wormKeyMeta {
	return l.metas[pos]
}

func (l *wormLeaf) refAt(pos int) wormRef {
	return l.items[pos]
}

func wormLeafDirBucket(keyPrefix uint64) int {
	return int(keyPrefix >> 48)
}

// BuildPointIndex builds a hash index for fast point lookups. In shared-access
// mode the base index is immutable for lock-free reads; later Put/Delete calls
// record per-key updates in a concurrent delta overlay. In exclusive mode,
// callers have already excluded all readers and writers, so updates can mutate
// the base index directly while no delta overlay exists.
func (m *wormhole) BuildPointIndex(x bool) {
	if !x {
		m.mu.Lock()
		defer m.mu.Unlock()
	}

	capHint := int(m.liveKeys.Load())
	if capHint < m.leafCap {
		capHint = m.leafCap
	}
	byKey := make(map[string]wormRef, capHint)
	for _, l := range m.leaves {
		if !x {
			l.mu.RLock()
		}
		for i, idx := range l.items {
			byKey[l.metas[i].key] = idx
		}
		if !x {
			l.mu.RUnlock()
		}
	}

	m.point.Store(&pointIndex{base: byKey})
}

func (m *wormhole) Put(kv KV, x bool) (old KV, replaced bool) {
	if x {
		return m.putExclusive(kv)
	}

	key := kv.Key
	keyPrefix := wormKeyPrefix(key)

	if old, replaced, done := m.putTailAppendFast(kv, keyPrefix); done {
		return old, replaced
	}

	for {
		m.mu.RLock()
		l := m.tailLeafLocked(key)
		if l == nil {
			l = m.findLeafByDirLocked(key, keyPrefix)
			if l == nil {
				idx := m.findLeafIndexLockedWithPrefix(key, keyPrefix)
				l = m.leaves[idx]
				m.storeLeafDir(keyPrefix, l)
			}
		}
		l.mu.Lock()
		pos, found := m.findInLeafWithPrefix(l, key, keyPrefix)
		canStayReadLocked := found || (pos > 0 && l.len() < m.leafCap)
		if canStayReadLocked {
			oldLen := l.len()
			if found {
				old = m.store.get(l.refAt(pos))
			}
			kvIdx := m.appendKV(kv)
			replaced = l.putAt(pos, found, key, keyPrefix, kvIdx)
			if !replaced && pos == oldLen {
				l.high = key
				l.highPrefix = keyPrefix
			}
			m.updatePointIndexPut(key, kvIdx, x)
			l.mu.Unlock()
			m.mu.RUnlock()
			if !replaced {
				m.liveKeys.Add(1)
			}
			return old, replaced
		}
		l.mu.Unlock()
		m.mu.RUnlock()

		m.mu.Lock()
		idx := m.findLeafIndexLockedWithPrefix(key, keyPrefix)
		l = m.leaves[idx]
		l.mu.Lock()
		pos, found = m.findInLeafWithPrefix(l, key, keyPrefix)
		if found {
			old = m.store.get(l.refAt(pos))
		}
		oldLen := l.len()
		kvIdx := m.appendKV(kv)
		replaced = l.putAt(pos, found, key, keyPrefix, kvIdx)
		m.updatePointIndexPut(key, kvIdx, x)
		m.updateLeafBoundsAfterPut(l, oldLen, pos, key, keyPrefix, replaced)
		if l.len() > m.leafCap {
			m.splitLeafLocked(idx, l)
		}
		l.mu.Unlock()
		m.mu.Unlock()
		if !replaced {
			m.liveKeys.Add(1)
		}
		return old, replaced
	}
}

func (m *wormhole) putExclusive(kv KV) (old KV, replaced bool) {
	key := kv.Key
	keyPrefix := wormKeyPrefix(key)

	if old, replaced, done := m.putTailAppendFastExclusive(kv, keyPrefix); done {
		return old, replaced
	}

	l := m.findLeafByDirLocked(key, keyPrefix)
	if l == nil {
		idx := m.findLeafIndexLockedWithPrefix(key, keyPrefix)
		l = m.leaves[idx]
		m.storeLeafDir(keyPrefix, l)
	}
	pos, found := m.findInLeafWithPrefix(l, key, keyPrefix)
	if found {
		old = m.store.get(l.refAt(pos))
	}
	oldLen := l.len()
	kvIdx := m.appendKV(kv)
	replaced = l.putAt(pos, found, key, keyPrefix, kvIdx)
	m.updatePointIndexPut(key, kvIdx, true)
	m.updateLeafBoundsAfterPut(l, oldLen, pos, key, keyPrefix, replaced)
	if l.len() > m.leafCap {
		idx := m.findLeafIndexLockedWithPrefix(l.anchor, l.anchorPrefix)
		m.splitLeafLocked(idx, l)
	}
	if !replaced {
		m.liveKeys.Add(1)
	}
	return old, replaced
}

func (m *wormhole) putTailAppendFast(kv KV, keyPrefix uint64) (old KV, replaced bool, done bool) {
	key := kv.Key

	m.mu.RLock()
	l := m.tail
	if l == nil {
		m.mu.RUnlock()
		return KV{}, false, false
	}

	l.mu.Lock()
	if l.next != nil || (l.anchor != "" && key < l.anchor) {
		l.mu.Unlock()
		m.mu.RUnlock()
		return KV{}, false, false
	}
	n := l.len()
	if n == 0 {
		l.mu.Unlock()
		m.mu.RUnlock()
		return KV{}, false, false
	}

	lastKey := l.high
	switch {
	case key > lastKey && n < m.leafCap:
		kvIdx := m.appendKV(kv)
		l.appendEntry(key, keyPrefix, kvIdx)
		l.high = key
		l.highPrefix = keyPrefix
		m.updatePointIndexPut(key, kvIdx, false)
		l.mu.Unlock()
		m.mu.RUnlock()
		m.liveKeys.Add(1)
		return KV{}, false, true
	case key == lastKey:
		old = m.store.get(l.refAt(n - 1))
		kvIdx := m.appendKV(kv)
		l.items[n-1] = kvIdx
		l.high = key
		l.highPrefix = keyPrefix
		m.updatePointIndexPut(key, kvIdx, false)
		l.mu.Unlock()
		m.mu.RUnlock()
		return old, true, true
	case key > lastKey:
		l.mu.Unlock()
		m.mu.RUnlock()
	default:
		l.mu.Unlock()
		m.mu.RUnlock()
		return KV{}, false, false
	}

	m.mu.Lock()
	l = m.tail
	if l != nil && l.next == nil && (l.anchor == "" || key >= l.anchor) {
		l.mu.Lock()
		n = l.len()
		if n == 0 {
			kvIdx := m.appendKV(kv)
			l.anchor = key
			l.high = key
			l.anchorPrefix = keyPrefix
			l.highPrefix = keyPrefix
			l.appendEntry(key, keyPrefix, kvIdx)
			m.updatePointIndexPut(key, kvIdx, false)
			l.mu.Unlock()
			m.mu.Unlock()
			m.liveKeys.Add(1)
			return KV{}, false, true
		}
		lastKey = l.high
		switch {
		case key > lastKey && n < m.leafCap:
			kvIdx := m.appendKV(kv)
			l.appendEntry(key, keyPrefix, kvIdx)
			l.high = key
			l.highPrefix = keyPrefix
			m.updatePointIndexPut(key, kvIdx, false)
			l.mu.Unlock()
			m.mu.Unlock()
			m.liveKeys.Add(1)
			return KV{}, false, true
		case key > lastKey:
			kvIdx := m.appendTailLeafLocked(l, kv, keyPrefix)
			m.updatePointIndexPut(key, kvIdx, false)
			l.mu.Unlock()
			m.mu.Unlock()
			m.liveKeys.Add(1)
			return KV{}, false, true
		case key == lastKey:
			old = m.store.get(l.refAt(n - 1))
			kvIdx := m.appendKV(kv)
			l.items[n-1] = kvIdx
			l.high = key
			l.highPrefix = keyPrefix
			m.updatePointIndexPut(key, kvIdx, false)
			l.mu.Unlock()
			m.mu.Unlock()
			return old, true, true
		}
		l.mu.Unlock()
	}
	m.mu.Unlock()
	return KV{}, false, false
}

func (m *wormhole) putTailAppendFastExclusive(kv KV, keyPrefix uint64) (old KV, replaced bool, done bool) {
	key := kv.Key
	l := m.tail
	if l == nil || l.next != nil || (l.anchor != "" && key < l.anchor) {
		return KV{}, false, false
	}

	n := l.len()
	if n == 0 {
		kvIdx := m.appendKV(kv)
		l.anchor = key
		l.high = key
		l.anchorPrefix = keyPrefix
		l.highPrefix = keyPrefix
		l.appendEntry(key, keyPrefix, kvIdx)
		m.updatePointIndexPut(key, kvIdx, true)
		m.liveKeys.Add(1)
		return KV{}, false, true
	}

	lastKey := l.high
	switch {
	case key > lastKey && n < m.leafCap:
		kvIdx := m.appendKV(kv)
		l.appendEntry(key, keyPrefix, kvIdx)
		l.high = key
		l.highPrefix = keyPrefix
		m.updatePointIndexPut(key, kvIdx, true)
		m.liveKeys.Add(1)
		return KV{}, false, true
	case key > lastKey:
		kvIdx := m.appendTailLeafLocked(l, kv, keyPrefix)
		m.updatePointIndexPut(key, kvIdx, true)
		m.liveKeys.Add(1)
		return KV{}, false, true
	case key == lastKey:
		old = m.store.get(l.refAt(n - 1))
		kvIdx := m.appendKV(kv)
		l.items[n-1] = kvIdx
		l.high = key
		l.highPrefix = keyPrefix
		m.updatePointIndexPut(key, kvIdx, true)
		return old, true, true
	default:
		return KV{}, false, false
	}
}

func (m *wormhole) Get(key string, x bool) (got KV, found bool) {
	if idx := m.point.Load(); idx != nil {
		if idx.delta == nil {
			kvIdx, found1 := idx.base[key]
			if !found1 {
				return KV{}, false
			}
			return m.store.get(kvIdx), true
		}
		return idx.getWithDelta(m, key)
	}

	if x {
		return m.getExclusive(key)
	}

	m.mu.RLock()
	l := m.cachedLeafLocked(key)
	if l == nil {
		idx := m.findLeafIndexLocked(key)
		l = m.leaves[idx]
		m.readCache.Store(l)
	}
	l.mu.RLock()
	m.mu.RUnlock()

	pos, found2 := m.findInLeafForRead(l, key)
	if !found2 {
		l.mu.RUnlock()
		return
	}
	out := m.store.get(l.refAt(pos))
	l.mu.RUnlock()
	return out, true
}

func (m *wormhole) getExclusive(key string) (KV, bool) {
	l := m.cachedLeafLocked(key)
	if l == nil {
		idx := m.findLeafIndexLocked(key)
		l = m.leaves[idx]
		m.readCache.Store(l)
	}
	pos, found := m.findInLeafForRead(l, key)
	if !found {
		return KV{}, false
	}
	return m.store.get(l.refAt(pos)), true
}

func (m *wormhole) Delete(key string, x bool) bool {
	if !x {
		m.mu.Lock()
	}
	idx := m.findLeafIndexLocked(key)
	l := m.leaves[idx]
	if !x {
		l.mu.Lock()
	}
	pos, found := m.findInLeaf(l, key)
	if !found {
		if !x {
			l.mu.Unlock()
			m.mu.Unlock()
		}
		return false
	}
	copy(l.items[pos:], l.items[pos+1:])
	copy(l.metas[pos:], l.metas[pos+1:])
	l.items = l.items[:len(l.items)-1]
	l.metas[len(l.metas)-1] = wormKeyMeta{}
	l.metas = l.metas[:len(l.metas)-1]
	m.updatePointIndexDelete(key, x)
	l.readHint.Store(0)
	if l.len() == 0 && len(m.leaves) > 1 {
		m.unlinkLeafLocked(idx, l)
	} else {
		m.refreshLeafBounds(l)
	}
	if !x {
		l.mu.Unlock()
		m.mu.Unlock()
	}
	m.liveKeys.Add(-1)
	return true
}

func (m *wormhole) Ascend(start string, x bool, fn func(KV) bool) {
	m.AscendRange(start, "", x, fn)
}

func (m *wormhole) AscendRange(start, end string, x bool, fn func(KV) bool) {
	if !x {
		m.mu.RLock()
		defer m.mu.RUnlock()
	}

	idx := 0
	if start != "" {
		idx = m.findLeafIndexLocked(start)
	}
	for ; idx < len(m.leaves); idx++ {
		l := m.leaves[idx]
		if !x {
			l.mu.RLock()
		}
		if start == "" && end == "" {
			for _, kvIdx := range l.items {
				if !fn(m.store.get(kvIdx)) {
					if !x {
						l.mu.RUnlock()
					}
					return
				}
			}
			if !x {
				l.mu.RUnlock()
			}
			continue
		}
		for i, kvIdx := range l.items {
			key := l.metas[i].key
			if start != "" && key < start {
				continue
			}
			if end != "" && key >= end {
				if !x {
					l.mu.RUnlock()
				}
				return
			}
			kv := m.store.get(kvIdx)
			if !fn(kv) {
				if !x {
					l.mu.RUnlock()
				}
				return
			}
		}
		if !x {
			l.mu.RUnlock()
		}
	}
}

func (m *wormhole) Descend(start string, x bool, fn func(KV) bool) {
	m.DescendRange(start, "", x, fn)
}

func (m *wormhole) DescendRange(start, end string, x bool, fn func(KV) bool) {
	if !x {
		m.mu.RLock()
		defer m.mu.RUnlock()
	}

	idx := len(m.leaves) - 1
	if start != "" {
		idx = m.findLeafIndexLocked(start)
	}
	for ; idx >= 0; idx-- {
		l := m.leaves[idx]
		if !x {
			l.mu.RLock()
		}
		if start == "" && end == "" {
			for i := l.len() - 1; i >= 0; i-- {
				if !fn(m.store.get(l.items[i])) {
					if !x {
						l.mu.RUnlock()
					}
					return
				}
			}
			if !x {
				l.mu.RUnlock()
			}
			continue
		}
		for i := l.len() - 1; i >= 0; i-- {
			key := l.metas[i].key
			if start != "" && key > start {
				continue
			}
			if end != "" && key <= end {
				if !x {
					l.mu.RUnlock()
				}
				return
			}
			kv := m.store.get(l.items[i])
			if !fn(kv) {
				if !x {
					l.mu.RUnlock()
				}
				return
			}
		}
		if !x {
			l.mu.RUnlock()
		}
	}
}

func (m *wormhole) SeekGE(target string, strict bool, x bool) (KV, bool) {
	if !x {
		m.mu.RLock()
		defer m.mu.RUnlock()
	}

	idx := 0
	if target != "" {
		idx = m.findLeafIndexLocked(target)
	}
	for ; idx < len(m.leaves); idx++ {
		l := m.leaves[idx]
		if !x {
			l.mu.RLock()
		}
		for i, kvIdx := range l.items {
			key := l.metas[i].key
			if target == "" || key > target || (!strict && key >= target) {
				if !x {
					l.mu.RUnlock()
				}
				return m.store.get(kvIdx), true
			}
		}
		if !x {
			l.mu.RUnlock()
		}
	}
	return KV{}, false
}

func (m *wormhole) SeekLE(target string, strict bool, x bool) (KV, bool) {
	if !x {
		m.mu.RLock()
		defer m.mu.RUnlock()
	}

	idx := len(m.leaves) - 1
	if target != "" {
		idx = m.findLeafIndexLocked(target)
	}
	for ; idx >= 0; idx-- {
		l := m.leaves[idx]
		if !x {
			l.mu.RLock()
		}
		for i := l.len() - 1; i >= 0; i-- {
			key := l.metas[i].key
			if target == "" || key < target || (!strict && key <= target) {
				if !x {
					l.mu.RUnlock()
				}
				return m.store.get(l.items[i]), true
			}
		}
		if !x {
			l.mu.RUnlock()
		}
	}
	return KV{}, false
}

func (m *wormhole) findLeafIndexLocked(key string) int {
	return m.findLeafIndexLockedWithPrefix(key, wormKeyPrefix(key))
}

func (m *wormhole) findLeafIndexLockedWithPrefix(key string, keyPrefix uint64) int {
	if len(m.leaves) == 1 {
		return 0
	}
	lo, hi := 0, len(m.leaves)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		l := m.leaves[mid]
		if l.anchorPrefix > keyPrefix || (l.anchorPrefix == keyPrefix && l.anchor > key) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	if lo == 0 {
		return 0
	}
	return lo - 1
}

func (m *wormhole) storeLeafDir(keyPrefix uint64, l *wormLeaf) {
	if len(m.leafDir) == wormLeafDirSize {
		m.leafDir[wormLeafDirBucket(keyPrefix)] = l
	}
}

func (m *wormhole) findLeafByDirLocked(key string, keyPrefix uint64) *wormLeaf {
	if len(m.leafDir) != wormLeafDirSize {
		return nil
	}
	l := m.leafDir[wormLeafDirBucket(keyPrefix)]
	if l == nil {
		return nil
	}
	if wormLeafMayContainWithPrefix(l, key, keyPrefix) {
		return l
	}
	for l.next != nil && !wormLeafAnchorGreaterThan(l.next, key, keyPrefix) {
		l = l.next
	}
	for l.prev != nil && wormLeafAnchorGreaterThan(l, key, keyPrefix) {
		l = l.prev
	}
	if !wormLeafMayContainWithPrefix(l, key, keyPrefix) {
		return nil
	}
	m.storeLeafDir(keyPrefix, l)
	return l
}

func wormLeafMayContainWithPrefix(l *wormLeaf, key string, keyPrefix uint64) bool {
	return !wormLeafAnchorGreaterThan(l, key, keyPrefix) &&
		(l.next == nil || wormLeafAnchorGreaterThan(l.next, key, keyPrefix))
}

func wormLeafAnchorGreaterThan(l *wormLeaf, key string, keyPrefix uint64) bool {
	if l == nil {
		return true
	}
	return l.anchorPrefix > keyPrefix || (l.anchorPrefix == keyPrefix && l.anchor > key)
}

func (m *wormhole) cachedLeafLocked(key string) *wormLeaf {
	l := m.readCache.Load()
	if l == nil || !m.leafContainsKeyLocked(l, key) {
		return nil
	}
	return l
}

func (m *wormhole) tailLeafLocked(key string) *wormLeaf {
	l := m.tail
	if l == nil || l.next != nil {
		return nil
	}
	if l.anchor != "" && key < l.anchor {
		return nil
	}
	return l
}

func (m *wormhole) leafContainsKeyLocked(l *wormLeaf, key string) bool {
	if l.anchor != "" && key < l.anchor {
		return false
	}
	if l.next != nil && l.next.anchor != "" && key >= l.next.anchor {
		return false
	}
	return true
}

func (m *wormhole) findInLeaf(l *wormLeaf, key string) (int, bool) {
	return m.findStringInLeafWithPrefix(l, key, wormKeyPrefix(key))
}

func (m *wormhole) findInLeafWithPrefix(l *wormLeaf, key string, keyPrefix uint64) (int, bool) {
	return m.findStringInLeafWithPrefix(l, key, keyPrefix)
}

func (m *wormhole) findInLeafForRead(l *wormLeaf, key string) (int, bool) {
	return m.findStringInLeafWithHint(l, key)
}

func (m *wormhole) updatePointIndexPut(key string, kvIdx wormRef, x bool) {
	if idx := m.mutablePointIndex(x); idx != nil {
		idx.put(key, kvIdx, x)
	}
}

func (idx *pointIndex) getWithDelta(m *wormhole, key string) (KV, bool) {
	if v, found := idx.delta.Load(key); found {
		mut := v.(pointMutation)
		if mut.deleted {
			return KV{}, false
		}
		return m.store.get(mut.idx), true
	}
	kvIdx, found := idx.base[key]
	if !found {
		return KV{}, false
	}
	return m.store.get(kvIdx), true
}

func (m *wormhole) mutablePointIndex(x bool) *pointIndex {
	for {
		idx := m.point.Load()
		if idx == nil {
			return nil
		}
		if idx.delta != nil {
			return idx
		}
		if x {
			return idx
		}
		next := &pointIndex{
			base:  idx.base,
			delta: &sync.Map{},
		}
		if m.point.CompareAndSwap(idx, next) {
			return next
		}
	}
}

func (m *wormhole) updatePointIndexDelete(key string, x bool) {
	if idx := m.mutablePointIndex(x); idx != nil {
		if x && idx.delta == nil {
			delete(idx.base, key)
			return
		}
		idx.delta.Store(key, pointMutation{deleted: true})
	}
}

func (idx *pointIndex) put(key string, kvIdx wormRef, x bool) {
	if x && idx.delta == nil {
		idx.base[key] = kvIdx
		return
	}
	idx.delta.Store(key, pointMutation{idx: kvIdx})
}

func (m *wormhole) splitLeafLocked(idx int, l *wormLeaf) {
	mid := len(l.items) / 2
	rightItems := make([]wormRef, len(l.items)-mid, m.leafCap+1)
	copy(rightItems, l.items[mid:])
	rightMetas := make([]wormKeyMeta, len(l.metas)-mid, m.leafCap+1)
	copy(rightMetas, l.metas[mid:])
	clear(l.metas[mid:])
	l.items = l.items[:mid]
	l.metas = l.metas[:mid]
	m.refreshLeafBounds(l)

	r := &wormLeaf{
		anchor:       rightMetas[0].key,
		high:         rightMetas[len(rightMetas)-1].key,
		anchorPrefix: rightMetas[0].prefix,
		highPrefix:   rightMetas[len(rightMetas)-1].prefix,
		items:        rightItems,
		metas:        rightMetas,
		prev:         l,
		next:         l.next,
	}
	if l.next != nil {
		l.next.prev = r
	}
	l.next = r
	if r.next == nil {
		m.tail = r
	}

	m.leaves = append(m.leaves, nil)
	copy(m.leaves[idx+2:], m.leaves[idx+1:])
	m.leaves[idx+1] = r
	m.storeLeafDir(l.metas[0].prefix, l)
	m.storeLeafDir(r.metas[0].prefix, r)
}

func (m *wormhole) appendTailLeafLocked(l *wormLeaf, kv KV, keyPrefix uint64) wormRef {
	kvIdx := m.appendKV(kv)
	r := &wormLeaf{
		anchor:       kv.Key,
		high:         kv.Key,
		anchorPrefix: keyPrefix,
		highPrefix:   keyPrefix,
		items:        make([]wormRef, 0, m.leafCap+1),
		metas:        make([]wormKeyMeta, 0, m.leafCap+1),
		prev:         l,
	}
	r.appendEntry(kv.Key, keyPrefix, kvIdx)
	l.next = r
	m.tail = r
	m.leaves = append(m.leaves, r)
	m.storeLeafDir(keyPrefix, r)
	return kvIdx
}

func (m *wormhole) unlinkLeafLocked(idx int, l *wormLeaf) {
	if l.prev != nil {
		l.prev.next = l.next
	}
	if l.next != nil {
		l.next.prev = l.prev
	}
	if m.tail == l {
		m.tail = l.prev
		if m.tail == nil {
			m.tail = l.next
		}
	}
	if m.readCache.Load() == l {
		m.readCache.Store(nil)
	}

	copy(m.leaves[idx:], m.leaves[idx+1:])
	m.leaves[len(m.leaves)-1] = nil
	m.leaves = m.leaves[:len(m.leaves)-1]

	l.prev = nil
	l.next = nil
	l.anchor = ""
	l.high = ""
	l.anchorPrefix = 0
	l.highPrefix = 0
	for i, cached := range m.leafDir {
		if cached == l {
			m.leafDir[i] = nil
		}
	}
	clear(l.metas)
	l.items = l.items[:0]
	l.metas = l.metas[:0]
}

func (m *wormhole) findStringInLeaf(l *wormLeaf, key string) (int, bool) {
	return m.findStringInLeafWithPrefix(l, key, wormKeyPrefix(key))
}

func (m *wormhole) findStringInLeafWithPrefix(l *wormLeaf, key string, keyPrefix uint64) (int, bool) {
	metas := l.metas
	lo, hi := 0, len(metas)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		meta := metas[mid]
		if meta.prefix > keyPrefix || (meta.prefix == keyPrefix && meta.key >= key) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	if lo >= len(metas) {
		return lo, false
	}
	meta := metas[lo]
	return lo, meta.prefix == keyPrefix && meta.key == key
}

func (m *wormhole) findStringInLeafWithHint(l *wormLeaf, key string) (int, bool) {
	metas := l.metas
	n := len(metas)
	if n == 0 {
		return 0, false
	}
	keyPrefix := wormKeyPrefix(key)

	hint := int(l.readHint.Load())
	if uint(hint) < uint(n) {
		hintMeta := metas[hint]
		if hintMeta.prefix == keyPrefix && hintMeta.key == key {
			return hint, true
		}
		if hintMeta.prefix < keyPrefix || (hintMeta.prefix == keyPrefix && hintMeta.key < key) {
			limit := hint + 4
			if limit >= n {
				limit = n - 1
			}
			for i := hint + 1; i <= limit; i++ {
				meta := metas[i]
				if meta.prefix > keyPrefix || (meta.prefix == keyPrefix && meta.key >= key) {
					found := meta.prefix == keyPrefix && meta.key == key
					if found {
						l.readHint.Store(int64(i))
					}
					return i, found
				}
			}
		} else {
			limit := hint - 4
			if limit < 0 {
				limit = 0
			}
			for i := hint - 1; i >= limit; i-- {
				meta := metas[i]
				if meta.prefix < keyPrefix || (meta.prefix == keyPrefix && meta.key <= key) {
					if meta.prefix == keyPrefix && meta.key == key {
						l.readHint.Store(int64(i))
						return i, true
					}
					return i + 1, false
				}
			}
		}
	}

	pos, found := m.findStringInLeaf(l, key)
	if found {
		l.readHint.Store(int64(pos))
	}
	return pos, found
}

func (l *wormLeaf) putAt(pos int, found bool, key string, keyPrefix uint64, kvIdx wormRef) bool {
	if found {
		l.items[pos] = kvIdx
		l.metas[pos] = wormKeyMeta{key: key, prefix: keyPrefix}
		return true
	}
	l.items = append(l.items, wormRef(0))
	copy(l.items[pos+1:], l.items[pos:])
	l.items[pos] = kvIdx
	l.metas = append(l.metas, wormKeyMeta{})
	copy(l.metas[pos+1:], l.metas[pos:])
	l.metas[pos] = wormKeyMeta{key: key, prefix: keyPrefix}
	return false
}

func (l *wormLeaf) appendEntry(key string, keyPrefix uint64, kvIdx wormRef) {
	l.items = append(l.items, kvIdx)
	l.metas = append(l.metas, wormKeyMeta{key: key, prefix: keyPrefix})
}

func (m *wormhole) updateLeafBoundsAfterPut(l *wormLeaf, oldLen, pos int, key string, keyPrefix uint64, replaced bool) {
	if replaced {
		return
	}
	if oldLen == 0 {
		l.anchor = key
		l.high = key
		l.anchorPrefix = keyPrefix
		l.highPrefix = keyPrefix
		return
	}
	if pos == 0 {
		l.anchor = key
		l.anchorPrefix = keyPrefix
	}
	if pos == oldLen {
		l.high = key
		l.highPrefix = keyPrefix
	}
}

func (m *wormhole) refreshLeafBounds(l *wormLeaf) {
	if len(l.metas) > 0 {
		l.anchor = l.metas[0].key
		l.high = l.metas[len(l.metas)-1].key
		l.anchorPrefix = l.metas[0].prefix
		l.highPrefix = l.metas[len(l.metas)-1].prefix
	} else {
		l.anchor = ""
		l.high = ""
		l.anchorPrefix = 0
		l.highPrefix = 0
	}
}
