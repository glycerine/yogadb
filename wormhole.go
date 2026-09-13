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
	keys     []string
	prefixes []uint64
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
	l := &wormLeaf{
		items:    make([]wormRef, 0, leafCap+1),
		keys:     make([]string, 0, leafCap+1),
		prefixes: make([]uint64, 0, leafCap+1),
	}
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
		clear(l.keys)
		clear(l.prefixes)
		l.items = l.items[:0]
		l.keys = l.keys[:0]
		l.prefixes = l.prefixes[:0]
		l.readHint.Store(0)
		l.mu.Unlock()
		if i > 0 {
			m.leaves[i] = nil
		}
	}

	if cap(first.items) < leafCap+1 {
		first.items = make([]wormRef, 0, leafCap+1)
	}
	if cap(first.keys) < leafCap+1 {
		first.keys = make([]string, 0, leafCap+1)
	}
	if cap(first.prefixes) < leafCap+1 {
		first.prefixes = make([]uint64, 0, leafCap+1)
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

func wormKeyAtGE(keys []string, prefixes []uint64, pos int, key string, keyPrefix uint64) bool {
	prefix := prefixes[pos]
	if prefix != keyPrefix {
		return prefix > keyPrefix
	}
	return keys[pos] >= key
}

func wormKeyAtLE(keys []string, prefixes []uint64, pos int, key string, keyPrefix uint64) bool {
	prefix := prefixes[pos]
	if prefix != keyPrefix {
		return prefix < keyPrefix
	}
	return keys[pos] <= key
}

func wormKeyAtEQ(keys []string, prefixes []uint64, pos int, key string, keyPrefix uint64) bool {
	return prefixes[pos] == keyPrefix && keys[pos] == key
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
			byKey[l.keys[i]] = idx
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
			idx := m.findLeafIndexLockedWithPrefix(key, keyPrefix)
			l = m.leaves[idx]
		}
		l.mu.Lock()
		pos, found := m.findInLeafWithPrefix(l, key, keyPrefix)
		canStayReadLocked := found || (pos > 0 && len(l.items) < m.leafCap)
		if canStayReadLocked {
			oldLen := len(l.items)
			if found {
				old = m.store.get(l.items[pos])
			}
			kvIdx := m.appendKV(kv)
			replaced = l.putAt(pos, found, key, keyPrefix, kvIdx)
			if !replaced && pos == oldLen {
				l.high = key
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
			old = m.store.get(l.items[pos])
		}
		kvIdx := m.appendKV(kv)
		replaced = l.putAt(pos, found, key, keyPrefix, kvIdx)
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
		return old, replaced
	}
}

func (m *wormhole) putExclusive(kv KV) (old KV, replaced bool) {
	key := kv.Key
	keyPrefix := wormKeyPrefix(key)

	if old, replaced, done := m.putTailAppendFastExclusive(kv, keyPrefix); done {
		return old, replaced
	}

	idx := m.findLeafIndexLockedWithPrefix(key, keyPrefix)
	l := m.leaves[idx]
	pos, found := m.findInLeafWithPrefix(l, key, keyPrefix)
	if found {
		old = m.store.get(l.items[pos])
	}
	kvIdx := m.appendKV(kv)
	replaced = l.putAt(pos, found, key, keyPrefix, kvIdx)
	m.updatePointIndexPut(key, kvIdx, true)
	m.refreshLeafBounds(l)
	if len(l.items) > m.leafCap {
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
	n := len(l.items)
	if n == 0 {
		l.mu.Unlock()
		m.mu.RUnlock()
		return KV{}, false, false
	}

	lastKey := l.high
	switch {
	case key > lastKey && n < m.leafCap:
		kvIdx := m.appendKV(kv)
		l.items = append(l.items, kvIdx)
		l.keys = append(l.keys, key)
		l.prefixes = append(l.prefixes, keyPrefix)
		l.high = key
		m.updatePointIndexPut(key, kvIdx, false)
		l.mu.Unlock()
		m.mu.RUnlock()
		m.liveKeys.Add(1)
		return KV{}, false, true
	case key == lastKey:
		old = m.store.get(l.items[n-1])
		kvIdx := m.appendKV(kv)
		l.items[n-1] = kvIdx
		l.keys[n-1] = key
		l.prefixes[n-1] = keyPrefix
		l.high = key
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
		n = len(l.items)
		if n == 0 {
			kvIdx := m.appendKV(kv)
			l.anchor = key
			l.high = key
			l.items = append(l.items, kvIdx)
			l.keys = append(l.keys, key)
			l.prefixes = append(l.prefixes, keyPrefix)
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
			l.items = append(l.items, kvIdx)
			l.keys = append(l.keys, key)
			l.prefixes = append(l.prefixes, keyPrefix)
			l.high = key
			m.updatePointIndexPut(key, kvIdx, false)
			l.mu.Unlock()
			m.mu.Unlock()
			m.liveKeys.Add(1)
			return KV{}, false, true
		case key > lastKey:
			kvIdx := m.appendTailLeafLocked(l, kv)
			m.updatePointIndexPut(key, kvIdx, false)
			l.mu.Unlock()
			m.mu.Unlock()
			m.liveKeys.Add(1)
			return KV{}, false, true
		case key == lastKey:
			old = m.store.get(l.items[n-1])
			kvIdx := m.appendKV(kv)
			l.items[n-1] = kvIdx
			l.keys[n-1] = key
			l.prefixes[n-1] = keyPrefix
			l.high = key
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

	n := len(l.items)
	if n == 0 {
		kvIdx := m.appendKV(kv)
		l.anchor = key
		l.high = key
		l.items = append(l.items, kvIdx)
		l.keys = append(l.keys, key)
		l.prefixes = append(l.prefixes, keyPrefix)
		m.updatePointIndexPut(key, kvIdx, true)
		m.liveKeys.Add(1)
		return KV{}, false, true
	}

	lastKey := l.high
	switch {
	case key > lastKey && n < m.leafCap:
		kvIdx := m.appendKV(kv)
		l.items = append(l.items, kvIdx)
		l.keys = append(l.keys, key)
		l.prefixes = append(l.prefixes, keyPrefix)
		l.high = key
		m.updatePointIndexPut(key, kvIdx, true)
		m.liveKeys.Add(1)
		return KV{}, false, true
	case key > lastKey:
		kvIdx := m.appendTailLeafLocked(l, kv)
		m.updatePointIndexPut(key, kvIdx, true)
		m.liveKeys.Add(1)
		return KV{}, false, true
	case key == lastKey:
		old = m.store.get(l.items[n-1])
		kvIdx := m.appendKV(kv)
		l.items[n-1] = kvIdx
		l.keys[n-1] = key
		l.prefixes[n-1] = keyPrefix
		l.high = key
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
	copy(l.keys[pos:], l.keys[pos+1:])
	copy(l.prefixes[pos:], l.prefixes[pos+1:])
	l.items = l.items[:len(l.items)-1]
	l.keys[len(l.keys)-1] = ""
	l.keys = l.keys[:len(l.keys)-1]
	l.prefixes[len(l.prefixes)-1] = 0
	l.prefixes = l.prefixes[:len(l.prefixes)-1]
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
			key := l.keys[i]
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
			for i := len(l.items) - 1; i >= 0; i-- {
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
		for i := len(l.items) - 1; i >= 0; i-- {
			key := l.keys[i]
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
			key := l.keys[i]
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
		for i := len(l.items) - 1; i >= 0; i-- {
			key := l.keys[i]
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
		var anchorPrefix uint64
		if len(l.prefixes) > 0 {
			anchorPrefix = l.prefixes[0]
		} else {
			anchorPrefix = wormKeyPrefix(l.anchor)
		}
		if anchorPrefix > keyPrefix || (anchorPrefix == keyPrefix && l.anchor > key) {
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
	rightKeys := make([]string, len(l.keys)-mid, m.leafCap+1)
	copy(rightKeys, l.keys[mid:])
	rightPrefixes := make([]uint64, len(l.prefixes)-mid, m.leafCap+1)
	copy(rightPrefixes, l.prefixes[mid:])
	clear(l.keys[mid:])
	clear(l.prefixes[mid:])
	l.items = l.items[:mid]
	l.keys = l.keys[:mid]
	l.prefixes = l.prefixes[:mid]
	m.refreshLeafBounds(l)

	r := &wormLeaf{
		anchor:   rightKeys[0],
		high:     rightKeys[len(rightKeys)-1],
		items:    rightItems,
		keys:     rightKeys,
		prefixes: rightPrefixes,
		prev:     l,
		next:     l.next,
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
		anchor:   kv.Key,
		high:     kv.Key,
		items:    make([]wormRef, 0, m.leafCap+1),
		keys:     make([]string, 0, m.leafCap+1),
		prefixes: make([]uint64, 0, m.leafCap+1),
		prev:     l,
	}
	r.items = append(r.items, kvIdx)
	r.keys = append(r.keys, kv.Key)
	r.prefixes = append(r.prefixes, wormKeyPrefix(kv.Key))
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
	clear(l.keys)
	clear(l.prefixes)
	l.items = l.items[:0]
	l.keys = l.keys[:0]
	l.prefixes = l.prefixes[:0]
}

func (m *wormhole) findStringInLeaf(l *wormLeaf, key string) (int, bool) {
	return m.findStringInLeafWithPrefix(l, key, wormKeyPrefix(key))
}

func (m *wormhole) findStringInLeafWithPrefix(l *wormLeaf, key string, keyPrefix uint64) (int, bool) {
	keys := l.keys
	prefixes := l.prefixes
	lo, hi := 0, len(keys)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if wormKeyAtGE(keys, prefixes, mid, key, keyPrefix) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo, lo < len(keys) && wormKeyAtEQ(keys, prefixes, lo, key, keyPrefix)
}

func (m *wormhole) findStringInLeafWithHint(l *wormLeaf, key string) (int, bool) {
	keys := l.keys
	prefixes := l.prefixes
	n := len(keys)
	if n == 0 {
		return 0, false
	}
	keyPrefix := wormKeyPrefix(key)

	hint := int(l.readHint.Load())
	if uint(hint) < uint(n) {
		hintPrefix := prefixes[hint]
		if hintPrefix == keyPrefix && keys[hint] == key {
			return hint, true
		}
		if hintPrefix < keyPrefix || (hintPrefix == keyPrefix && keys[hint] < key) {
			limit := hint + 4
			if limit >= n {
				limit = n - 1
			}
			for i := hint + 1; i <= limit; i++ {
				if wormKeyAtGE(keys, prefixes, i, key, keyPrefix) {
					found := wormKeyAtEQ(keys, prefixes, i, key, keyPrefix)
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
				if wormKeyAtLE(keys, prefixes, i, key, keyPrefix) {
					if wormKeyAtEQ(keys, prefixes, i, key, keyPrefix) {
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
		l.keys[pos] = key
		l.prefixes[pos] = keyPrefix
		return true
	}
	l.items = append(l.items, wormRef(0))
	copy(l.items[pos+1:], l.items[pos:])
	l.items[pos] = kvIdx
	l.keys = append(l.keys, "")
	copy(l.keys[pos+1:], l.keys[pos:])
	l.keys[pos] = key
	l.prefixes = append(l.prefixes, 0)
	copy(l.prefixes[pos+1:], l.prefixes[pos:])
	l.prefixes[pos] = keyPrefix
	return false
}

func (m *wormhole) refreshLeafBounds(l *wormLeaf) {
	if len(l.keys) > 0 {
		l.anchor = l.keys[0]
		l.high = l.keys[len(l.keys)-1]
	} else {
		l.anchor = ""
		l.high = ""
	}
}
