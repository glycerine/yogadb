package ramflextree

import (
	"github.com/tidwall/btree"
)

// ====================== memtable ======================

type memtable struct {
	// backing in memory B-tree (was skiplist in C).
	bt *btree.BTreeG[KV]

	size  int64 // approximate bytes in this memtable
	empty bool  // true when memtable has no data (freshly created or flushed+cleared)
}

func newMemtable() *memtable {
	return &memtable{
		// default degree is 32, to change it:
		bt:    btree.NewBTreeGOptions[KV](kvLess, btree.Options{Degree: 32}),
		empty: true,
	}
}

func (m *memtable) reset() {
	m.empty = true
	m.size = 0
}

// caller should set m.empty to false after calling put()
// (e.g. db.go:165 in Batch.Commit)
// Returns the previous KV for the same key and whether it was replaced.
func (m *memtable) put(kv KV) (KV, bool) {
	old, replaced := m.bt.Set(kv)
	if replaced {
		m.size -= int64(kvSizeApprox(&old))
	}
	m.size += int64(kvSizeApprox(&kv))
	if m.size <= 0 {
		panicf("bad: memtable with some content should have size(%v) > 0: %#v", m.size, m)
	}
	return old, replaced
}

func (m *memtable) get(key string) (KV, bool) {
	return m.bt.Get(KV{Key: key})
}
