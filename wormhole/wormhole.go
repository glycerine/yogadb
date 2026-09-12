package wormhole

import (
	"sort"
	"sync"
	"sync/atomic"
)

const DefaultLeafCapacity = 128

type Compare func(a, b string) int

type Options struct {
	LeafCapacity int
	Compare      Compare
}

type Map struct {
	mu               sync.RWMutex
	leaves           []*leaf
	tail             *leaf
	cmp              Compare
	useStringCompare bool
	leafCap          int
	liveKeys         atomic.Int64
	readCache        atomic.Pointer[leaf]
}

type leaf struct {
	mu       sync.RWMutex
	anchor   string
	prev     *leaf
	next     *leaf
	items    []KV
	readHint atomic.Int64
}

func New(opts Options) *Map {
	cmp := opts.Compare
	useStringCompare := cmp == nil
	if cmp == nil {
		cmp = stringCompare
	}
	leafCap := opts.LeafCapacity
	if leafCap <= 0 {
		leafCap = DefaultLeafCapacity
	}
	if leafCap < 4 {
		leafCap = 4
	}
	l := &leaf{items: make([]KV, 0, leafCap+1)}
	return &Map{
		leaves:           []*leaf{l},
		tail:             l,
		cmp:              cmp,
		useStringCompare: useStringCompare,
		leafCap:          leafCap,
	}
}

func stringCompare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func (m *Map) Len() int64 {
	return m.liveKeys.Load()
}

func (m *Map) Put(kv KV) (replaced bool) {
	key := kv.Key

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
			replaced = l.putAt(pos, found, kv)
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
		replaced = l.putAt(pos, found, kv)
		l.refreshAnchor()
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

func (m *Map) Get(key string) (KV, bool) {
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
	out := l.items[pos]
	l.mu.RUnlock()
	return out, true
}

func (m *Map) Delete(key string) bool {
	m.mu.Lock()
	idx := m.findLeafIndexLocked(key)
	l := m.leaves[idx]
	l.mu.Lock()
	pos, found := m.findInLeaf(l, key)
	if !found {
		l.mu.Unlock()
		m.mu.Unlock()
		return false
	}
	copy(l.items[pos:], l.items[pos+1:])
	l.items[len(l.items)-1] = KV{}
	l.items = l.items[:len(l.items)-1]
	l.readHint.Store(0)
	if len(l.items) == 0 && len(m.leaves) > 1 {
		m.unlinkLeafLocked(idx, l)
	} else {
		l.refreshAnchor()
	}
	l.mu.Unlock()
	m.mu.Unlock()
	m.liveKeys.Add(-1)
	return true
}

func (m *Map) Ascend(start string, fn func(KV) bool) {
	m.AscendRange(start, "", fn)
}

func (m *Map) AscendRange(start, end string, fn func(KV) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	idx := 0
	if start != "" {
		idx = m.findLeafIndexLocked(start)
	}
	for ; idx < len(m.leaves); idx++ {
		l := m.leaves[idx]
		l.mu.RLock()
		for _, kv := range l.items {
			if start != "" && m.compare(kv.Key, start) < 0 {
				continue
			}
			if end != "" && m.compare(kv.Key, end) >= 0 {
				l.mu.RUnlock()
				return
			}
			if !fn(kv) {
				l.mu.RUnlock()
				return
			}
		}
		l.mu.RUnlock()
	}
}

func (m *Map) Descend(start string, fn func(KV) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	idx := len(m.leaves) - 1
	if start != "" {
		idx = m.findLeafIndexLocked(start)
	}
	for ; idx >= 0; idx-- {
		l := m.leaves[idx]
		l.mu.RLock()
		for i := len(l.items) - 1; i >= 0; i-- {
			kv := l.items[i]
			if start != "" && m.compare(kv.Key, start) > 0 {
				continue
			}
			if !fn(kv) {
				l.mu.RUnlock()
				return
			}
		}
		l.mu.RUnlock()
	}
}

func (m *Map) findLeafIndexLocked(key string) int {
	if len(m.leaves) == 1 {
		return 0
	}
	if m.useStringCompare {
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
	idx := sort.Search(len(m.leaves), func(i int) bool {
		return m.cmp(m.leaves[i].anchor, key) > 0
	})
	if idx == 0 {
		return 0
	}
	return idx - 1
}

func (m *Map) cachedLeafLocked(key string) *leaf {
	l := m.readCache.Load()
	if l == nil || !m.leafContainsKeyLocked(l, key) {
		return nil
	}
	return l
}

func (m *Map) tailLeafLocked(key string) *leaf {
	l := m.tail
	if l == nil || l.next != nil {
		return nil
	}
	if l.anchor != "" && m.compare(key, l.anchor) < 0 {
		return nil
	}
	return l
}

func (m *Map) leafContainsKeyLocked(l *leaf, key string) bool {
	if l.anchor != "" && m.compare(key, l.anchor) < 0 {
		return false
	}
	if l.next != nil && l.next.anchor != "" && m.compare(key, l.next.anchor) >= 0 {
		return false
	}
	return true
}

func (m *Map) compare(a, b string) int {
	if m.useStringCompare {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	}
	return m.cmp(a, b)
}

func (m *Map) findInLeaf(l *leaf, key string) (int, bool) {
	if m.useStringCompare {
		return l.findString(key)
	}
	return l.find(m.cmp, key)
}

func (m *Map) findInLeafForRead(l *leaf, key string) (int, bool) {
	if m.useStringCompare {
		return l.findStringWithHint(key)
	}
	return l.find(m.cmp, key)
}

func (m *Map) splitLeafLocked(idx int, l *leaf) {
	mid := len(l.items) / 2
	rightItems := make([]KV, len(l.items)-mid, m.leafCap+1)
	copy(rightItems, l.items[mid:])
	l.items = l.items[:mid]
	l.refreshAnchor()

	r := &leaf{
		anchor: rightItems[0].Key,
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

func (m *Map) unlinkLeafLocked(idx int, l *leaf) {
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
}

func (l *leaf) find(cmp Compare, key string) (int, bool) {
	idx := sort.Search(len(l.items), func(i int) bool {
		return cmp(l.items[i].Key, key) >= 0
	})
	return idx, idx < len(l.items) && cmp(l.items[idx].Key, key) == 0
}

func (l *leaf) findString(key string) (int, bool) {
	items := l.items
	lo, hi := 0, len(items)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if items[mid].Key >= key {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo, lo < len(items) && items[lo].Key == key
}

func (l *leaf) findStringWithHint(key string) (int, bool) {
	items := l.items
	n := len(items)
	if n == 0 {
		return 0, false
	}

	hint := int(l.readHint.Load())
	if uint(hint) < uint(n) {
		hintKey := items[hint].Key
		if hintKey == key {
			return hint, true
		}
		if hintKey < key {
			limit := hint + 4
			if limit >= n {
				limit = n - 1
			}
			for i := hint + 1; i <= limit; i++ {
				itemKey := items[i].Key
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
				itemKey := items[i].Key
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

	pos, found := l.findString(key)
	if found {
		l.readHint.Store(int64(pos))
	}
	return pos, found
}

func (l *leaf) putAt(pos int, found bool, kv KV) bool {
	if found {
		l.items[pos] = kv
		return true
	}
	l.items = append(l.items, KV{})
	copy(l.items[pos+1:], l.items[pos:])
	l.items[pos] = kv
	if pos == 0 {
		l.anchor = kv.Key
	}
	return false
}

func (l *leaf) refreshAnchor() {
	if len(l.items) > 0 {
		l.anchor = l.items[0].Key
	} else {
		l.anchor = ""
	}
}
