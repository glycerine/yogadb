package yogadb

import (
	"sync"
	"sync/atomic"
)

const (
	wormPageBits  = 11
	wormPageSize  = 1 << wormPageBits
	wormPageMask  = wormPageSize - 1
	wormBlockBits = 10
	wormBlockSize = 1 << wormBlockBits
	wormBlockMask = wormBlockSize - 1
	wormDirSize   = 1024
)

type wormRef uint32

type wormConfig struct {
	leafCapacity int
}

// wormhole is our memtable. The name comes from the wormhole data structure
// used in the original C FlexDB implementation for the memtable. We
// have optimized our Go version of wormhole to the point where,
// besides the name, the internal algorithms are probably
// un-recognizable and completely different. But still, its a good name.
//
// wormhole now benchmarks better than the alternative memtables we tried:
// tidwall's Btree, pebble's skiplist, our own keyStable arena.
type wormhole struct {
	mu        sync.RWMutex
	leaves    []*wormLeaf
	tail      *wormLeaf
	store     wormKVstore
	point     atomic.Pointer[pointIndex]
	leafCap   int
	liveKeys  atomic.Int64
	readCache atomic.Pointer[wormLeaf]
}

type wormLeaf struct {
	mu       sync.RWMutex
	anchor   string
	high     string
	prev     *wormLeaf
	next     *wormLeaf
	items    []wormRef
	readHint atomic.Int64
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
	l := &wormLeaf{items: make([]wormRef, 0, leafCap+1)}
	m := &wormhole{
		leaves:  []*wormLeaf{l},
		tail:    l,
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

	var first *wormLeaf
	if len(m.leaves) > 0 {
		first = m.leaves[0]
	}
	if first == nil {
		first = &wormLeaf{items: make([]wormRef, 0, leafCap+1)}
	}

	for i, l := range m.leaves {
		if l == nil {
			continue
		}
		l.mu.Lock()
		l.anchor = ""
		l.high = ""
		l.prev = nil
		l.next = nil
		l.items = l.items[:0]
		l.readHint.Store(0)
		l.mu.Unlock()
		if i > 0 {
			m.leaves[i] = nil
		}
	}

	if cap(first.items) < leafCap+1 {
		first.items = make([]wormRef, 0, leafCap+1)
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
		for _, idx := range l.items {
			byKey[m.store.key(idx)] = idx
		}
		if !x {
			l.mu.RUnlock()
		}
	}

	m.point.Store(&pointIndex{base: byKey})
}

func (m *wormhole) Put(kv KV, x bool) (replaced bool) {
	if x {
		return m.putExclusive(kv)
	}

	key := kv.Key

	if replaced, done := m.putTailAppendFast(kv); done {
		return replaced
	}

	for {
		m.mu.RLock()
		l := m.tailLeafLocked(key)
		if l == nil {
			idx := m.findLeafIndexLocked(key)
			l = m.leaves[idx]
		}
		l.mu.Lock()
		pos, found := m.findInLeaf(l, key)
		canStayReadLocked := found || (pos > 0 && len(l.items) < m.leafCap)
		if canStayReadLocked {
			oldLen := len(l.items)
			kvIdx := m.appendKV(kv)
			replaced = l.putAt(pos, found, kvIdx)
			if !replaced && pos == oldLen {
				l.high = key
			}
			m.updatePointIndexPut(key, kvIdx, x)
			l.mu.Unlock()
			m.mu.RUnlock()
			if !replaced {
				m.liveKeys.Add(1)
			}
			return replaced
		}
		l.mu.Unlock()
		m.mu.RUnlock()

		m.mu.Lock()
		idx := m.findLeafIndexLocked(key)
		l = m.leaves[idx]
		l.mu.Lock()
		pos, found = m.findInLeaf(l, key)
		kvIdx := m.appendKV(kv)
		replaced = l.putAt(pos, found, kvIdx)
		m.updatePointIndexPut(key, kvIdx, x)
		m.refreshLeafBounds(l)
		if len(l.items) > m.leafCap {
			m.splitLeafLocked(idx, l)
		}
		l.mu.Unlock()
		m.mu.Unlock()
		if !replaced {
			m.liveKeys.Add(1)
		}
		return replaced
	}
}

func (m *wormhole) putExclusive(kv KV) (replaced bool) {
	key := kv.Key

	if replaced, done := m.putTailAppendFastExclusive(kv); done {
		return replaced
	}

	idx := m.findLeafIndexLocked(key)
	l := m.leaves[idx]
	pos, found := m.findInLeaf(l, key)
	kvIdx := m.appendKV(kv)
	replaced = l.putAt(pos, found, kvIdx)
	m.updatePointIndexPut(key, kvIdx, true)
	m.refreshLeafBounds(l)
	if len(l.items) > m.leafCap {
		m.splitLeafLocked(idx, l)
	}
	if !replaced {
		m.liveKeys.Add(1)
	}
	return replaced
}

func (m *wormhole) putTailAppendFast(kv KV) (replaced bool, done bool) {
	key := kv.Key

	m.mu.RLock()
	l := m.tail
	if l == nil {
		m.mu.RUnlock()
		return false, false
	}

	l.mu.Lock()
	if l.next != nil || (l.anchor != "" && key < l.anchor) {
		l.mu.Unlock()
		m.mu.RUnlock()
		return false, false
	}
	n := len(l.items)
	if n == 0 {
		l.mu.Unlock()
		m.mu.RUnlock()
		return false, false
	}

	lastKey := l.high
	switch {
	case key > lastKey && n < m.leafCap:
		kvIdx := m.appendKV(kv)
		l.items = append(l.items, kvIdx)
		l.high = key
		m.updatePointIndexPut(key, kvIdx, false)
		l.mu.Unlock()
		m.mu.RUnlock()
		m.liveKeys.Add(1)
		return false, true
	case key == lastKey:
		kvIdx := m.appendKV(kv)
		l.items[n-1] = kvIdx
		l.high = key
		m.updatePointIndexPut(key, kvIdx, false)
		l.mu.Unlock()
		m.mu.RUnlock()
		return true, true
	case key > lastKey:
		l.mu.Unlock()
		m.mu.RUnlock()
	default:
		l.mu.Unlock()
		m.mu.RUnlock()
		return false, false
	}

	m.mu.Lock()
	l = m.tail
	if l != nil && l.next == nil && (l.anchor == "" || key >= l.anchor) {
		l.mu.Lock()
		n = len(l.items)
		if n == 0 {
			kvIdx := m.appendKV(kv)
			l.anchor = key
			l.high = key
			l.items = append(l.items, kvIdx)
			m.updatePointIndexPut(key, kvIdx, false)
			l.mu.Unlock()
			m.mu.Unlock()
			m.liveKeys.Add(1)
			return false, true
		}
		lastKey = l.high
		switch {
		case key > lastKey && n < m.leafCap:
			kvIdx := m.appendKV(kv)
			l.items = append(l.items, kvIdx)
			l.high = key
			m.updatePointIndexPut(key, kvIdx, false)
			l.mu.Unlock()
			m.mu.Unlock()
			m.liveKeys.Add(1)
			return false, true
		case key > lastKey:
			kvIdx := m.appendTailLeafLocked(l, kv)
			m.updatePointIndexPut(key, kvIdx, false)
			l.mu.Unlock()
			m.mu.Unlock()
			m.liveKeys.Add(1)
			return false, true
		case key == lastKey:
			kvIdx := m.appendKV(kv)
			l.items[n-1] = kvIdx
			l.high = key
			m.updatePointIndexPut(key, kvIdx, false)
			l.mu.Unlock()
			m.mu.Unlock()
			return true, true
		}
		l.mu.Unlock()
	}
	m.mu.Unlock()
	return false, false
}

func (m *wormhole) putTailAppendFastExclusive(kv KV) (replaced bool, done bool) {
	key := kv.Key
	l := m.tail
	if l == nil || l.next != nil || (l.anchor != "" && key < l.anchor) {
		return false, false
	}

	n := len(l.items)
	if n == 0 {
		kvIdx := m.appendKV(kv)
		l.anchor = key
		l.high = key
		l.items = append(l.items, kvIdx)
		m.updatePointIndexPut(key, kvIdx, true)
		m.liveKeys.Add(1)
		return false, true
	}

	lastKey := l.high
	switch {
	case key > lastKey && n < m.leafCap:
		kvIdx := m.appendKV(kv)
		l.items = append(l.items, kvIdx)
		l.high = key
		m.updatePointIndexPut(key, kvIdx, true)
		m.liveKeys.Add(1)
		return false, true
	case key > lastKey:
		kvIdx := m.appendTailLeafLocked(l, kv)
		m.updatePointIndexPut(key, kvIdx, true)
		m.liveKeys.Add(1)
		return false, true
	case key == lastKey:
		kvIdx := m.appendKV(kv)
		l.items[n-1] = kvIdx
		l.high = key
		m.updatePointIndexPut(key, kvIdx, true)
		return true, true
	default:
		return false, false
	}
}

func (m *wormhole) Get(key string, x bool) (KV, bool) {
	if idx := m.point.Load(); idx != nil {
		if idx.delta == nil {
			kvIdx, found := idx.base[key]
			if !found {
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

	pos, found := m.findInLeafForRead(l, key)
	if !found {
		l.mu.RUnlock()
		return KV{}, false
	}
	out := m.store.get(l.items[pos])
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
	return m.store.get(l.items[pos]), true
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
	l.items = l.items[:len(l.items)-1]
	m.updatePointIndexDelete(key, x)
	l.readHint.Store(0)
	if len(l.items) == 0 && len(m.leaves) > 1 {
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
		for _, kvIdx := range l.items {
			kv := m.store.get(kvIdx)
			if start != "" && kv.Key < start {
				continue
			}
			if end != "" && kv.Key >= end {
				if !x {
					l.mu.RUnlock()
				}
				return
			}
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
		for i := len(l.items) - 1; i >= 0; i-- {
			kv := m.store.get(l.items[i])
			if start != "" && kv.Key > start {
				continue
			}
			if end != "" && kv.Key <= end {
				if !x {
					l.mu.RUnlock()
				}
				return
			}
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
		for _, kvIdx := range l.items {
			kv := m.store.get(kvIdx)
			if target == "" || kv.Key > target || (!strict && kv.Key >= target) {
				if !x {
					l.mu.RUnlock()
				}
				return kv, true
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
		for i := len(l.items) - 1; i >= 0; i-- {
			kv := m.store.get(l.items[i])
			if target == "" || kv.Key < target || (!strict && kv.Key <= target) {
				if !x {
					l.mu.RUnlock()
				}
				return kv, true
			}
		}
		if !x {
			l.mu.RUnlock()
		}
	}
	return KV{}, false
}

func (m *wormhole) findLeafIndexLocked(key string) int {
	if len(m.leaves) == 1 {
		return 0
	}
	lo, hi := 0, len(m.leaves)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if m.leaves[mid].anchor > key {
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
	return m.findStringInLeaf(l, key)
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
	l.items = l.items[:mid]
	m.refreshLeafBounds(l)

	r := &wormLeaf{
		anchor: m.store.key(rightItems[0]),
		high:   m.store.key(rightItems[len(rightItems)-1]),
		items:  rightItems,
		prev:   l,
		next:   l.next,
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
}

func (m *wormhole) appendTailLeafLocked(l *wormLeaf, kv KV) wormRef {
	kvIdx := m.appendKV(kv)
	r := &wormLeaf{
		anchor: kv.Key,
		high:   kv.Key,
		items:  make([]wormRef, 0, m.leafCap+1),
		prev:   l,
	}
	r.items = append(r.items, kvIdx)
	l.next = r
	m.tail = r
	m.leaves = append(m.leaves, r)
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
}

func (m *wormhole) findStringInLeaf(l *wormLeaf, key string) (int, bool) {
	items := l.items
	lo, hi := 0, len(items)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if m.store.key(items[mid]) >= key {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo, lo < len(items) && m.store.key(items[lo]) == key
}

func (m *wormhole) findStringInLeafWithHint(l *wormLeaf, key string) (int, bool) {
	items := l.items
	n := len(items)
	if n == 0 {
		return 0, false
	}

	hint := int(l.readHint.Load())
	if uint(hint) < uint(n) {
		hintKey := m.store.key(items[hint])
		if hintKey == key {
			return hint, true
		}
		if hintKey < key {
			limit := hint + 4
			if limit >= n {
				limit = n - 1
			}
			for i := hint + 1; i <= limit; i++ {
				itemKey := m.store.key(items[i])
				if itemKey >= key {
					found := itemKey == key
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
				itemKey := m.store.key(items[i])
				if itemKey <= key {
					if itemKey == key {
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

func (l *wormLeaf) putAt(pos int, found bool, kvIdx wormRef) bool {
	if found {
		l.items[pos] = kvIdx
		return true
	}
	l.items = append(l.items, wormRef(0))
	copy(l.items[pos+1:], l.items[pos:])
	l.items[pos] = kvIdx
	return false
}

func (m *wormhole) refreshLeafBounds(l *wormLeaf) {
	if len(l.items) > 0 {
		l.anchor = m.store.key(l.items[0])
		l.high = m.store.key(l.items[len(l.items)-1])
	} else {
		l.anchor = ""
		l.high = ""
	}
}
