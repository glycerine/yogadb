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
	mu       sync.RWMutex
	leaves   []*leaf
	cmp      Compare
	leafCap  int
	liveKeys atomic.Int64
}

type leaf struct {
	mu     sync.RWMutex
	anchor string
	prev   *leaf
	next   *leaf
	items  []KV
}

func New(opts Options) *Map {
	cmp := opts.Compare
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
		leaves:  []*leaf{l},
		cmp:     cmp,
		leafCap: leafCap,
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
	kv.Value = append([]byte(nil), kv.Value...)
	key := kv.Key

	for {
		m.mu.RLock()
		idx := m.findLeafIndexLocked(key)
		l := m.leaves[idx]
		l.mu.Lock()
		pos, found := l.find(m.cmp, key)
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
		idx = m.findLeafIndexLocked(key)
		l = m.leaves[idx]
		l.mu.Lock()
		pos, found = l.find(m.cmp, key)
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
	idx := m.findLeafIndexLocked(key)
	l := m.leaves[idx]
	l.mu.RLock()
	m.mu.RUnlock()

	pos, found := l.find(m.cmp, key)
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
	pos, found := l.find(m.cmp, key)
	if !found {
		l.mu.Unlock()
		m.mu.Unlock()
		return false
	}
	copy(l.items[pos:], l.items[pos+1:])
	l.items[len(l.items)-1] = KV{}
	l.items = l.items[:len(l.items)-1]
	l.refreshAnchor()
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
			if start != "" && m.cmp(kv.Key, start) < 0 {
				continue
			}
			if end != "" && m.cmp(kv.Key, end) >= 0 {
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
			if start != "" && m.cmp(kv.Key, start) > 0 {
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
	idx := sort.Search(len(m.leaves), func(i int) bool {
		return m.cmp(m.leaves[i].anchor, key) > 0
	})
	if idx == 0 {
		return 0
	}
	return idx - 1
}

func (m *Map) splitLeafLocked(idx int, l *leaf) {
	mid := len(l.items) / 2
	rightItems := append([]KV(nil), l.items[mid:]...)
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

	m.leaves = append(m.leaves, nil)
	copy(m.leaves[idx+2:], m.leaves[idx+1:])
	m.leaves[idx+1] = r
}

func (l *leaf) find(cmp Compare, key string) (int, bool) {
	idx := sort.Search(len(l.items), func(i int) bool {
		return cmp(l.items[i].Key, key) >= 0
	})
	return idx, idx < len(l.items) && cmp(l.items[idx].Key, key) == 0
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
	}
}
