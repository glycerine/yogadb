package yogadb

import (
	"sort"
	"strings"
)

// keyUniq is an alternate sorted-slice memtable core that interns keys with
// unique.Handle[string] instead of borrowing strings from a byte arena.
//
// INVAR: stable holds every interned key handle ever inserted since the last
// clear. sorted holds stable indexes for currently active keys in ascending
// key order. tomb holds stable indexes for keys removed by delKey.
//
// LIFETIME INVAR: key strings returned through KVX.Key come from
// unique.Handle[string].Value(). They are ordinary Go strings, not views into
// keyUniq-owned mutable storage, so they remain valid after keyUniq is mutated
// or cleared. The handles retained in stable keep active canonical values in
// the unique table; clear zeros those handles so the runtime can reclaim
// canonical values once no user strings or handles remain.
type keyUniq struct {
	stable []Key
	sorted []int
	tomb   []int

	kvs           []KV
	valueAliasKey []bool

	imap        map[Key]int // handle -> stable index + 1
	sortedDirty bool
}

func makeKeyUniq(n int) keyUniq {
	if n <= 0 {
		n = 256 << 10
	}
	return keyUniq{
		stable: make([]Key, 0, n),
		sorted: make([]int, 0, n),
		kvs:    make([]KV, 0, n),
		imap:   make(map[Key]int, n),
	}
}

func newKeyUniq(n int) *keyUniq {
	s := makeKeyUniq(n)
	return &s
}

func (s *keyUniq) clear() {
	var zeroHandle Key
	for i := range s.stable {
		s.stable[i] = zeroHandle
	}
	for i := range s.kvs {
		s.kvs[i] = KV{}
	}
	s.stable = s.stable[:0]
	s.sorted = s.sorted[:0]
	s.tomb = s.tomb[:0]
	s.kvs = s.kvs[:0]
	s.valueAliasKey = s.valueAliasKey[:0]
	clear(s.imap)
	s.sortedDirty = false
}

func (s *keyUniq) addKey(key []byte) (whereInStable int) {
	return s.addKeyString(string(key))
}

func (s *keyUniq) addKeyString(key string) (whereInStable int) {
	h := keyH(key)
	whereInStable, found := s.findStableByHandle(h)
	if found {
		return whereInStable
	}
	return s.appendKeyHandle(h)
}

func (s *keyUniq) appendKeyString(key string) (whereInStable int) {
	return s.appendKeyHandle(keyH(key))
}

func (s *keyUniq) appendKeyHandle(h Key) (whereInStable int) {
	if s.imap == nil {
		s.ensureImap()
	}
	whereInStable = len(s.stable)
	s.stable = append(s.stable, h)
	s.kvs = append(s.kvs, KV{})
	s.valueAliasKey = append(s.valueAliasKey, false)
	s.imap[h] = whereInStable + 1
	s.sorted = append(s.sorted, whereInStable)
	s.sortedDirty = true
	return whereInStable
}

func (s *keyUniq) Less(i, j int) bool {
	if i == j {
		return false
	}
	return s.at(s.sorted[i]) < s.at(s.sorted[j])
}

func (s *keyUniq) at(i int) string {
	return keyString(s.stable[i])
}

func (s *keyUniq) Swap(i, j int) {
	s.sorted[i], s.sorted[j] = s.sorted[j], s.sorted[i]
}

func (s *keyUniq) Len() int {
	return len(s.sorted)
}

func (s *keyUniq) findKey(needle []byte) (where int, found bool) {
	s.ensureSorted()
	return sort.Find(len(s.sorted), func(i int) int {
		return compareBytesString(needle, s.at(s.sorted[i]))
	})
}

func (s *keyUniq) findKeyString(needle string) (where int, found bool) {
	s.ensureSorted()
	return sort.Find(len(s.sorted), func(i int) int {
		return strings.Compare(needle, s.at(s.sorted[i]))
	})
}

func (s *keyUniq) ensureSorted() {
	if !s.sortedDirty {
		return
	}
	sort.Sort(s)
	s.sortedDirty = false
}

func (s *keyUniq) ensureImap() {
	if s.imap != nil {
		return
	}
	s.imap = make(map[Key]int, len(s.stable))
	for _, stableIdx := range s.sorted {
		s.imap[s.stable[stableIdx]] = stableIdx + 1
	}
}

func (s *keyUniq) findStableByBytes(key []byte) (stableIdx int, found bool) {
	return s.findStableByString(string(key))
}

func (s *keyUniq) findStableByString(key string) (stableIdx int, found bool) {
	return s.findStableByHandle(keyH(key))
}

func (s *keyUniq) findStableByHandle(h Key) (stableIdx int, found bool) {
	s.ensureImap()
	stableIdx = s.imap[h] - 1
	if stableIdx < 0 {
		return 0, false
	}
	return stableIdx, true
}

func (s *keyUniq) removeStableFromImap(stableIdx int) {
	h := s.stable[stableIdx]
	if s.imap[h] == stableIdx+1 {
		delete(s.imap, h)
	}
}

func compareBytesString(a []byte, b string) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

func (s *keyUniq) kvAt(stableIdx int) KVX {
	kv := s.kvs[stableIdx]
	kvx := KVX{
		Key:   s.stable[stableIdx],
		Value: kv.Value,
		Vptr:  kv.Vptr,
		Hlc:   kv.Hlc,
	}
	if s.valueAliasKey[stableIdx] {
		kvx.Value = []byte(s.at(stableIdx))
		kvx.valueAliasKey = true
	}
	return kvx
}

func (s *keyUniq) storeKVAt(stableIdx int, kv KV) {
	valueAliasKey := slottedInlineValueAliasesKey(kv)
	kv.Key = ""
	if valueAliasKey {
		kv.Value = nil
	}
	s.kvs[stableIdx] = kv
	s.valueAliasKey[stableIdx] = valueAliasKey
}

func (s *keyUniq) set(kv KV) (old KVX, replaced bool) {
	h := keyH(kv.Key)
	stableIdx, found := s.findStableByHandle(h)
	if found {
		old = s.kvAt(stableIdx)
		s.storeKVAt(stableIdx, kv)
		return old, true
	}
	stableIdx = s.appendKeyHandle(h)
	s.storeKVAt(stableIdx, kv)
	return KVX{}, false
}

func (s *keyUniq) get(key string) (KVX, bool) {
	stableIdx, found := s.findStableByString(key)
	if !found {
		return KVX{}, false
	}
	return s.kvAt(stableIdx), true
}

func (s *keyUniq) seekGE(target string, strict bool) (KVX, bool) {
	w, found := s.findKeyString(target)
	if strict && found {
		w++
	}
	if w >= len(s.sorted) {
		return KVX{}, false
	}
	return s.kvAt(s.sorted[w]), true
}

func (s *keyUniq) seekLE(target string, strict bool) (KVX, bool) {
	if len(s.sorted) == 0 {
		return KVX{}, false
	}
	s.ensureSorted()
	if target == "" {
		return s.kvAt(s.sorted[len(s.sorted)-1]), true
	}
	w, found := s.findKeyString(target)
	if !found || strict {
		w--
	}
	if w < 0 {
		return KVX{}, false
	}
	return s.kvAt(s.sorted[w]), true
}

func (s *keyUniq) Ascend(pivot KVX, iter func(KVX) bool) {
	w, _ := s.findKeyString(keyString(pivot.Key))
	for ; w < len(s.sorted); w++ {
		if !iter(s.kvAt(s.sorted[w])) {
			return
		}
	}
}

func (s *keyUniq) Descend(pivot KVX, iter func(KVX) bool) {
	pivotKey := keyString(pivot.Key)
	if pivotKey == "" {
		return
	}
	w, found := s.findKeyString(pivotKey)
	if !found {
		w--
	}
	for ; w >= 0; w-- {
		if !iter(s.kvAt(s.sorted[w])) {
			return
		}
	}
}

func (s *keyUniq) Scan(iter func(KVX) bool) {
	s.ensureSorted()
	for _, stableIdx := range s.sorted {
		if !iter(s.kvAt(stableIdx)) {
			return
		}
	}
}

func (s *keyUniq) Reverse(iter func(KVX) bool) {
	s.ensureSorted()
	for i := len(s.sorted) - 1; i >= 0; i-- {
		if !iter(s.kvAt(s.sorted[i])) {
			return
		}
	}
}

func (s *keyUniq) AscendOwnedKeys(pivot KVX, iter func(KVX) bool) {
	s.Ascend(pivot, iter)
}

func (s *keyUniq) delKey(needle []byte) (found bool) {
	var w int
	w, found = s.findKey(needle)
	if !found {
		return
	}
	deleted := s.sorted[w]
	s.removeStableFromImap(deleted)
	tw, _ := sort.Find(len(s.tomb), func(i int) int {
		return strings.Compare(s.at(deleted), s.at(s.tomb[i]))
	})
	s.tomb = append(s.tomb, 0)
	if tw < len(s.tomb)-1 {
		copy(s.tomb[tw+1:], s.tomb[tw:])
	}
	s.tomb[tw] = deleted
	s.kvs[deleted] = KV{}
	s.valueAliasKey[deleted] = false

	last := len(s.sorted) - 1
	if w < last {
		copy(s.sorted[w:], s.sorted[w+1:])
	}
	s.sorted = s.sorted[:last]
	return
}
