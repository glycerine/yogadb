package yogadb

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/cespare/xxhash/v2"
)

// keyStable is a place to keep your keys
// when you think of them like horses.
// Horses live in a stable. Get it? It's a pun.
//
// You can also read it as: "key's table".
//
// More importantly, the point is _stable_ storage:
// The array index stable[i] is a stable and never changing
// integer reference to the key string (stable for the lifetime
// of this memtable generation, before it is cleared).
//
// We do a single key copy, and we avoid making
// work for the garbage collector, because there are
// almost no pointers.
//
// The benefit: we improved write throughput significantly in the
// afterbulk_test.go benchmarks of newly written key-value pairs;
// up to 2x fold for some cases.
//
// We use keyStable for our memtable. In-memory B-trees and
// skip-lists are used as the memtable by other databases,
// but we took inspiration from Entity-Component-System (ECS)
// designs and just use integer indexing to avoid alot of pointers.
//
// INVAR: if i is the index into stable for a given key (the i-th key we added):
// key i=0 is stored in s.keys[0             :s.stable[i]]
// key i>0 is stored in s.keys[s.stable[i-1] :s.stable[i]]
//
// We no longer inspect or analyze or change the KV that we store.
// We are not responsible for the lifetime of the KV.Key or KV.Value
// on the KV that we index.
//
// We do not support storing the empty string as a key.
// Keys must have at least one byte of content. The
// db.go returns ErrKeyEmpty from validateUserKey on empty keys;
// so we should never see them.
// An empty string back from a query means the key was not present.
//
// Since deletes are just set with KV.Vptr.Length = rawVlenTombstone,
// and set can replace an old KV, we do not need special handling
// for deletes or tombstones here. memtable.go only does
// set(), get(), and clear().
type keyStable struct {
	keys []byte // arena with a copy of all keys, stacked end to end.

	// stable store. only appended to, or overwritten.
	stable []int // where to find the string in keys[]
	sorted []int // sorted indexes of stable in ascending key order.

	kvs      []KV  // parallel to stable
	hashNext []int // collision chain for imap; parallel to stable.

	// xxhash.Sum64(key) -> index in stable.
	imap        map[uint64]int
	sortedDirty bool
}

func makeKeyStable(n int) keyStable {
	if n <= 0 {
		n = 256 << 10
	}
	return keyStable{
		keys:     make([]byte, 0, 8<<20),
		stable:   make([]int, 0, n),
		hashNext: make([]int, 0, n),
		sorted:   make([]int, 0, n),
		kvs:      make([]KV, 0, n),
		imap:     make(map[uint64]int, n),
	}
}

func newKeyStable(n int) *keyStable {
	s := makeKeyStable(n)
	return &s
}

func (s *keyStable) clear() {
	for i := range s.kvs {
		s.kvs[i] = KV{}
	}
	s.keys = s.keys[:0]
	s.stable = s.stable[:0]
	s.sorted = s.sorted[:0]
	s.kvs = s.kvs[:0]
	s.hashNext = s.hashNext[:0]
	clear(s.imap)
	s.sortedDirty = false
}

func (s *keyStable) addKey(key []byte) (whereInStable int) {

	// INVAR: string i=0 is stored in s.keys[0             :s.stable[i]]
	//        string i>0 is stored in s.keys[s.stable[i-1] :s.stable[i]]

	h := xxhash.Sum64(key)
	whereInStable, found := s.findStableByBytes(key, h)
	if found {
		return whereInStable
	}
	return s.appendKeyBytes(key, h)
}

func (s *keyStable) appendKeyBytes(key []byte, h uint64) (whereInStable int) {
	whereInStable = s.appendKeyCommon(len(key), h)
	s.keys = append(s.keys, key...)
	return whereInStable
}

func (s *keyStable) appendKeyString(key string, h uint64) (whereInStable int) {
	whereInStable = s.appendKeyCommon(len(key), h)
	s.keys = append(s.keys, key...)
	return whereInStable
}

func (s *keyStable) appendKeyCommon(keylen int, h uint64) (whereInStable int) {
	if s.imap == nil {
		s.ensureImap()
	}
	whereInStable = len(s.stable)
	s.stable = append(s.stable, len(s.keys)+keylen)
	s.kvs = append(s.kvs, KV{})
	s.hashNext = append(s.hashNext, s.imap[h]-1)
	// so hashNext of -1 means: end of chain; no earlier value,
	// since imap[h] gives 0 for no h present.
	//
	// but if there was an earlier imap[h], we overwrite it now in imap:
	s.imap[h] = whereInStable + 1

	s.sorted = append(s.sorted, whereInStable)
	s.sortedDirty = true
	return whereInStable
}

func (s *keyStable) Less(i, j int) bool {
	if i == j {
		return false
	}
	ib := s.at(s.sorted[i])
	jb := s.at(s.sorted[j])
	return bytes.Compare(ib, jb) < 0
}

func (s *keyStable) at(i int) []byte {
	if i == 0 {
		return s.keys[:s.stable[0]]
	}
	return s.keys[s.stable[i-1]:s.stable[i]]
}

func (s *keyStable) Swap(i, j int) {
	s.sorted[i], s.sorted[j] = s.sorted[j], s.sorted[i]
}

func (s *keyStable) Len() int {
	return len(s.sorted)
}

func (s *keyStable) findKey(needle []byte) (where int, found bool) {
	s.ensureSorted()
	return sort.Find(len(s.sorted), func(i int) int {
		return bytes.Compare(needle, s.at(s.sorted[i]))
	})
}

func (s *keyStable) findKeyString(needle string) (where int, found bool) {
	s.ensureSorted()
	return sort.Find(len(s.sorted), func(i int) int {
		return compareStringBytes(needle, s.at(s.sorted[i]))
	})
}

func (s *keyStable) ensureSorted() {
	if !s.sortedDirty {
		return
	}
	sort.Sort(s)
	s.sortedDirty = false
}

func (s *keyStable) ensureImap() {
	if s.imap != nil {
		return
	}
	s.imap = make(map[uint64]int, len(s.stable))
	s.hashNext = s.hashNext[:0]
	for range s.stable {
		s.hashNext = append(s.hashNext, -1)
	}
	for _, stableIdx := range s.sorted {
		h := xxhash.Sum64(s.at(stableIdx))
		// becaue the value 0 back from imap means not present, we undo the +1 bump
		// (below) by subtracting 1 after pulling from imap
		s.hashNext[stableIdx] = s.imap[h] - 1
		s.imap[h] = stableIdx + 1
	}
}

func (s *keyStable) findStableByBytes(key []byte, h uint64) (stableIdx int, found bool) {
	s.ensureImap()
	for stableIdx = s.imap[h] - 1; stableIdx >= 0; stableIdx = s.hashNext[stableIdx] {
		if bytes.Equal(key, s.at(stableIdx)) {
			return stableIdx, true
		}
	}
	return 0, false
}

func (s *keyStable) findStableByString(key string, h uint64) (stableIdx int, found bool) {
	s.ensureImap()
	for stableIdx = s.imap[h] - 1; stableIdx >= 0; stableIdx = s.hashNext[stableIdx] {
		if compareStringBytes(key, s.at(stableIdx)) == 0 {
			return stableIdx, true
		}
	}
	return 0, false
}

func (s *keyStable) removeStableFromImap(stableIdx int) {
	h := xxhash.Sum64(s.at(stableIdx))
	prev := -1
	for cur := s.imap[h] - 1; cur >= 0; cur = s.hashNext[cur] {
		if cur == stableIdx {
			if prev < 0 {
				s.imap[h] = s.hashNext[cur] + 1
				if s.imap[h] == 0 {
					delete(s.imap, h)
				}
			} else {
				s.hashNext[prev] = s.hashNext[cur]
			}
			s.hashNext[cur] = -1
			return
		}
		prev = cur
	}
}

func compareStringBytes(a string, b []byte) int {
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

func (s *keyStable) kvAt(stableIdx int) KV {
	kv := s.kvs[stableIdx]
	return kv
}

func (s *keyStable) storeKVAt(stableIdx int, kv KV) {
	s.kvs[stableIdx] = kv
}

func (s *keyStable) set(kv KV) (old KV, replaced bool) {
	h := xxhash.Sum64String(kv.Key)
	stableIdx, found := s.findStableByString(kv.Key, h)
	//vv("set(): kv.Key='%v' was found='%v'; stableIdx=%v", kv.Key, found, stableIdx)
	if found {
		old = s.kvAt(stableIdx)
		s.storeKVAt(stableIdx, kv)
		return old, true
	}
	stableIdx = s.appendKeyString(kv.Key, h)
	s.storeKVAt(stableIdx, kv)
	return KV{}, false
}

func (s *keyStable) get(key string) (kv KV, found bool) {
	var stableIdx int
	h := xxhash.Sum64String(key)
	stableIdx, found = s.findStableByString(key, h)
	if !found {
		return
	}
	return s.kvAt(stableIdx), true
}

func (s *keyStable) seekGE(target string, strict bool) (KV, bool) {
	if len(s.sorted) == 0 {
		return KV{}, false
	}
	s.ensureSorted()
	w, found := s.findKeyString(target)
	if strict && found {
		w++
	}
	if w >= len(s.sorted) {
		return KV{}, false
	}
	return s.kvAt(s.sorted[w]), true
}

func (s *keyStable) seekLE(target string, strict bool) (KV, bool) {
	if len(s.sorted) == 0 {
		return KV{}, false
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
		return KV{}, false
	}
	return s.kvAt(s.sorted[w]), true
}

func (s *keyStable) Ascend(pivot KV, iter func(KV) bool) {
	w, _ := s.findKeyString(pivot.Key)
	for ; w < len(s.sorted); w++ {
		if !iter(s.kvAt(s.sorted[w])) {
			return
		}
	}
}

func (s *keyStable) Descend(pivot KV, iter func(KV) bool) {
	if pivot.Key == "" {
		return
	}
	w, found := s.findKeyString(pivot.Key)
	if !found {
		w--
	}
	for ; w >= 0; w-- {
		if !iter(s.kvAt(s.sorted[w])) {
			return
		}
	}
}

func (s *keyStable) Scan(iter func(KV) bool) {
	s.ensureSorted()
	for _, stableIdx := range s.sorted {
		if !iter(s.kvAt(stableIdx)) {
			return
		}
	}
}

func (s *keyStable) Reverse(iter func(KV) bool) {
	s.ensureSorted()
	for i := len(s.sorted) - 1; i >= 0; i-- {
		if !iter(s.kvAt(s.sorted[i])) {
			return
		}
	}
}

func (s *keyStable) delKey(needle []byte) (found bool) {
	var w int // where the needle lives in s.sorted
	w, found = s.findKey(needle)
	if !found {
		return
	}
	deleted := s.sorted[w]
	s.removeStableFromImap(deleted)
	s.kvs[deleted] = KV{}

	last := len(s.sorted) - 1
	if w < last {
		copy(s.sorted[w:], s.sorted[w+1:])
	}
	s.sorted = s.sorted[:last]
	return
}

func (s *keyStable) String() string {
	keys := "["
	for i := range s.stable {
		keys += fmt.Sprintf("'%v', ", string(s.at(i)))
	}
	keys += "]"
	return fmt.Sprintf(`keyStable{
	keys: %v
	stable: %#v
	sorted: %#v
	kvs: %#v
	hashNext: %#v
	imap: %#v
	sortedDirty: %v
}
`, keys,
		s.stable,
		s.sorted,
		s.kvs,
		s.hashNext,
		s.imap,
		s.sortedDirty)
}

/*

linux with keyStable:  WITH THE FLUSH WORKER ON!

=== RUN   Test_Writes_Occuring_After_Bulk_Load_YogaDB

afterbulk_test.go:121 [pid 3422026] 2026-09-10 17:20:31.795255672 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 620_198.5509284749 writes/sec
afterbulk_test.go:121 [pid 3420968] 2026-09-10 17:18:35.710861022 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 614_811.3140495906 writes/sec
afterbulk_test.go:121 [pid 3409600] 2026-09-10 16:56:18.524389771 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 614_871.7071904588 writes/sec

=== RUN   Test_Replacement_After_Bulk_Load_YogaDB

afterbulk_test.go:268 [pid 3409600] 2026-09-10 16:56:50.500138943 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 156168.7063311166 writes/sec
afterbulk_test.go:268 [pid 3420968] 2026-09-10 17:19:07.808563604 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 154378.33792037226 writes/sec
afterbulk_test.go:268 [pid 3422026] 2026-09-10 17:21:03.585589518 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 155652.17542936638 writes/sec

linux with btree:
=== RUN   Test_Writes_Occuring_After_Bulk_Load_YogaDB
afterbulk_test.go:121 [pid 3408587] 2026-09-10 16:54:51.361718215 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 244695.69555943864 writes/sec

=== RUN   Test_Replacement_After_Bulk_Load_YogaDB
afterbulk_test.go:268 [pid 3408587] 2026-09-10 16:55:22.887938873 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 125743.61291149577 writes/sec


---------

// Performance improvements of keyStable versus tidwall.Btree, with background flush worker OFF!

linux, master so tidwall.Btree, no background flush!

=== RUN   Test_Writes_Occuring_After_Bulk_Load_YogaDB

afterbulk_test.go:127 [pid 3447216] 2026-09-10 18:05:25.571037641 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 347489.31783522404 writes/sec
afterbulk_test.go:127 [pid 3448197] 2026-09-10 18:07:12.181517326 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 361239.03865359817 writes/sec
afterbulk_test.go:127 [pid 3448970] 2026-09-10 18:08:32.350211851 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 355542.4884051915 writes/sec

=== RUN   Test_Replacement_After_Bulk_Load_YogaDB

afterbulk_test.go:276 [pid 3447216] 2026-09-10 18:06:12.388077590 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 78691.83666816386 writes/sec
afterbulk_test.go:276 [pid 3448197] 2026-09-10 18:07:59.627287917 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 76119.42318910093 writes/sec
afterbulk_test.go:276 [pid 3448970] 2026-09-10 18:09:19.616095469 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 78531.6417831247 writes/sec

------------------------
versus

branch: keystable, on darwin, NO flush worker.

=== RUN   Test_Writes_Occuring_After_Bulk_Load_YogaDB (keystable is 1.27x faster than Btree on darwin)

afterbulk_test.go:127 [pid 94944] 2026-09-10 18:30:12.328605000 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 199659.24547906208 writes/sec
afterbulk_test.go:127 [pid 95065] 2026-09-10 18:32:25.581476000 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 195350.39843514893 writes/sec

=== RUN   Test_Replacement_After_Bulk_Load_YogaDB (keystable is 1.10x faster than Btree on darwin)

afterbulk_test.go:276 [pid 94944] 2026-09-10 18:31:12.601705000 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 68180.53187860642 writes/sec
afterbulk_test.go:276 [pid 95065] 2026-09-10 18:33:28.042795000 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 65842.10609431572 writes/sec


branch: keystable, on linux, NO flush worker.

=== RUN   Test_Writes_Occuring_After_Bulk_Load_YogaDB  (keyStable is 1.67x faster than tidwall.Btree)

afterbulk_test.go:127 [pid 3451407] 2026-09-10 18:12:31.846136821 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 579224.8673116597 writes/sec
afterbulk_test.go:127 [pid 3451921] 2026-09-10 18:13:18.923597476 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 611722.5271822995 writes/sec
afterbulk_test.go:127 [pid 3452399] 2026-09-10 18:13:57.422257574 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 598945.6145828298 writes/sec
afterbulk_test.go:127 [pid 3452874] 2026-09-10 18:14:33.402962839 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 608296.0492488866 writes/sec
afterbulk_test.go:127 [pid 3453477] 2026-09-10 18:15:28.420317783 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 604677.0311693986 writes/sec
afterbulk_test.go:127 [pid 3454160] 2026-09-10 18:16:39.211856161 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 607372.3811374991 writes/sec
afterbulk_test.go:127 [pid 3455015] 2026-09-10 18:18:06.805850210 +0000 UTC after bulkload terminated with AllowReads: yogadb insert 604881.2055699311 writes/sec

=== RUN   Test_Replacement_After_Bulk_Load_YogaDB  (keyStable is 1.21x faster than tidwall.Btree)

afterbulk_test.go:276 [pid 3453477] 2026-09-10 18:16:12.188771876 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 96030.38864593704 writes/sec
afterbulk_test.go:276 [pid 3454160] 2026-09-10 18:17:23.527680992 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 95323.40862664096 writes/sec
afterbulk_test.go:276 [pid 3455015] 2026-09-10 18:18:50.992023927 +0000 UTC after bulkload terminated with AllowReads: yogadb replacements: 95488.230878552 writes/sec

// earlier measurements when flush worker was sometimes on:
// afterbulk_test.go tests:
//
// Linux:
// Test_Writes_Occuring_After_Bulk_Load_YogaDB 614_872 writes/sec  vs btree: 244_696 (keyStable is 2.5x faster)
// and... next runs were: 620K, 614K, 601K.
//
// Test_Replacement_After_Bulk_Load_YogaDB     156_169 writes/sec  vs btree: 125_744 (keyStable is 1.24x faster)
// and... next runs were: 155K, 153K, 155K writes/sec
//
// Darwin:
// Test_Writes_Occuring_After_Bulk_Load_YogaDB 154_688 writes/sec  vs btree: 113K writes/sec (keyStable is 1.36x faster)
// Test_Replacement_After_Bulk_Load_YogaDB     108_258 writes/sec  vs btree:  89K writes/sec (keyStable is 1.21x faster)

*/
