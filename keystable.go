package yogadb

import (
	"fmt"
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// Currently our wormhole has 2x better performance on frequently
// mixed and interleaved reads and writes, but keystable is
// better with batches of writes and the batches of reads;
// as well as point sets (Puts).
//
// compare:
// go test -v -run=xxx -bench BenchmarkWormhole_Mixed_ReadsWrites   67.8 ns/op      23 B/op ; 2x faster
// go test -v -run=xxx -bench BenchmarkKeyStable_Mixed_ReadsWrites   152 ns/op     117 B/op
// 50% load self-managed hash chain: (worse)
// BenchmarkKeyStable_Mixed_ReadsWrites-48     7557370       148.1 ns/op     128 B/op       0 allocs/op
//
// BenchmarkWormholeGet-48      6712690        162.7 ns/op       0 B/op       0 allocs/op ; 4x slower
// note:
// BenchmarkWormholeGet-48     31249660        38.24 ns/op       0 B/op       0 allocs/op (if s.BuildPointIndex(x) called, but that is cheating becasue the mixed read/write bench does not call s.BuildPointIndex(x)).
//
// BenchmarkKeyStableGet-48    21890116        46.09 ns/op       0 B/op       0 allocs/op
// 50% load self-managed hash chain:
// BenchmarkKeyStableGet-48    35613524        33.46 ns/op       0 B/op       0 allocs/op
//
// more thoroughly A/B:
//
// Benchmark    keyStable             wormhole                winner
// -----------------------------------------------------------------
// Set / Put    40.37 ns/op, 0 B/op   102.0 ns/op, 145 B/op   keyStable, 2.55x
// Get          35-40 ns/op           32-39 ns/op             tie
//
// Mixed_ReadsWrites 167.0 ns/op, 145 B/op   121.0 ns/op, 23 B/op    wormhole, 1.38x
//
// Initial Load Then Ordered Scan Medians:
//
// Size                       keyStable                           wormhole    Winner
// ━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━
// 4096       1.48 ms, 491 KB, 6 allocs    1.86 ms, 993KB, 83 allocs           tie, ks slightly better
// ───────  ─────────────────────────────  ─────────────────────────────────  ──────────────────
// 65536    28.06 ms, 7.86 MB, 6 allocs    30.5 ms, 7.6 MB, 1149 allocs        tie, ks slightly better
//
// After_Bulk
//
// Test                                keyStable              wormhole    Winner
// ━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━
// Fresh writes after bulk    561,504 writes/sec    539,947 writes/sec    keyStable, but only 1.04x faster
// ─────────────────────────  ────────────────────  ────────────────────  ──────────────────
// Replacements after bulk     94,476 writes/sec     84,803 writes/sec    keyStable, but only 1.11x faster
//
// Heap deltas were comparable on fresh writes, slightly favoring keystable:
//
// fresh keyStable:  HeapAlloc diff 318,765,704; HeapInuse diff 307,691,520
// fresh wormhole:   HeapAlloc diff 317_359_704; HeapInuse diff 314_040_320
//
// Replacement HeapInuse favored wormhole just barely:
//
// replace keyStable: HeapAlloc diff 311,687,688; HeapInuse diff 307,388,416
// replace wormhole:  HeapAlloc diff 310_157_760; HeapInuse diff 300_515_328
//
// conclude: wormhole wins the synthetic mixed reads/write benchmark (2x faster),
// but keyStable has 2.55x better point Set(Put) writes, and 4-11% faster
// batch writes in the After_Bulk tests, and 4x faster Get point queries.
//
// What is keyStable? A key stable is a place to keep your keys
// when you think of them like horses.
// Horses live in a stable. KeyStable is stable storage for your memtable KV.
//
// You can also read it as: "key's table".
//
// The benefit: we improved write throughput significantly in the
// afterbulk_test.go benchmarks of newly written key-value pairs;
// up to 2x fold for some cases (...versus the in-memory tidwall.Btree, was it?)
//
// keyStable is kept as an alternate in-memory table implementation. Earlier
// yogadb versions used it as the memtable. In-memory B-trees and
// skip-lists are used as the memtable by other databases,
// but we took inspiration from Entity-Component-System (ECS)
// designs and just use integer indexing to avoid alot of pointers.
// As a point of validation, it turns out the TurtleKV mem-table
// design is almost exactly this design too.
//
// INVAR: if i is the index for a given key, s.kvs[i].Key is that key.
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
// set(), get(), and clear(); and ascend/descend ranges.
type keyStable struct {

	// kvs is only appended to, or overwritten.
	kvs          []KV     // stable slot storage.
	hashes       []uint64 // parallel to kvs. hashes[i] holds the hash of kvs[i].Key
	nextSameHash []int    // collision chain for headmap; parallel to kvs.

	// headmap is a hashmap. We Hash bucket -> index into kvs.
	// Stored as slot+1 so zero means empty.
	headmap []int // we manage the growth of this hashmap ourselves.

	sorted []int // indexes of kvs in ascending key order.

	// get() calls can force sorts which are mutation, and
	// get() can be concurrent from multiple readers at once. so protect
	// the sorting when the call argument x is false. An x == true means
	// exclusive db access; that the db.topMutRW is already write locked
	// by the caller. In this case, we can skip locking mu.
	mu sync.Mutex

	// does the sorted slice need sorting?
	sortedDirty bool // at end to avoid messing with other member's alignment
}

// A sync.Mutex must not be copied after first use, but we
// return a copy from makeKeyStable, so be sure not
// to use the the mu during makeKeyStable().
func makeKeyStable(n int) keyStable {
	if n <= 0 {
		n = 256 << 10
	}
	return keyStable{
		nextSameHash: make([]int, 0, n),
		hashes:       make([]uint64, 0, n),
		sorted:       make([]int, 0, n),
		kvs:          make([]KV, 0, n),
		headmap:      make([]int, keyStableBucketCount(n)),
	}
}

func keyStableBucketCount(n int) int {
	if n < 8 {
		n = 8
	}
	n *= 4
	buckets := 1
	for buckets < n {
		buckets <<= 1
	}
	return buckets
}

func newKeyStable(n int) *keyStable {
	s := makeKeyStable(n)
	return &s
}

func (s *keyStable) clear(x bool) {
	if !x {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	for i := range s.kvs {
		s.kvs[i] = KV{}
	}
	s.sorted = s.sorted[:0]
	s.kvs = s.kvs[:0]
	s.hashes = s.hashes[:0]
	s.nextSameHash = s.nextSameHash[:0]
	clear(s.headmap)
	s.sortedDirty = false
}

// test-only use by keystable_test.go, so does not need to lock mu.
func (s *keyStable) addKey(key []byte) (slotIdx int) {
	h := xxhash.Sum64(key)
	slotIdx, found := s.findSlotByBytes(key, h)
	if found {
		return slotIdx
	}
	return s.appendKeyBytes(key, h)
}

// test-only, no mu lock needed.
func (s *keyStable) appendKeyBytes(key []byte, h uint64) (slotIdx int) {
	slotIdx = s.appendKeyCommon(h)
	s.kvs[slotIdx].Key = string(key)
	return slotIdx
}

// internal: called by set(), mu or x must hold already.
func (s *keyStable) appendKeyString(key string, h uint64) (slotIdx int) {
	slotIdx = s.appendKeyCommon(h)
	s.kvs[slotIdx].Key = key
	return slotIdx
}

// internal: called by appendKeyString() which is called by set(), mu or x must hold already.
func (s *keyStable) appendKeyCommon(h uint64) (slotIdx int) {
	if len(s.headmap) == 0 {
		s.ensureHeadmap()
	}
	slotIdx = len(s.kvs)
	s.kvs = append(s.kvs, KV{})
	s.hashes = append(s.hashes, h)
	s.nextSameHash = append(s.nextSameHash, -1)
	if len(s.kvs)*2 > len(s.headmap) {
		s.rebuildHeadmap(keyStableBucketCount(len(s.kvs)))
	}
	bucket := s.bucket(h)
	s.nextSameHash[slotIdx] = s.headmap[bucket] - 1
	// so nextSameHash of -1 means: end of chain; no earlier value,
	// since headmap[bucket] gives 0 for no h present.
	//
	// but if there was an earlier headmap[bucket], we overwrite it now in headmap:
	s.headmap[bucket] = slotIdx + 1

	s.sorted = append(s.sorted, slotIdx)
	s.sortedDirty = true
	return slotIdx
}

// internal: mu or x hold is already handled by our caller.
func (s *keyStable) Less(i, j int) bool {
	if i == j {
		return false
	}
	return s.at(s.sorted[i]) < s.at(s.sorted[j])
}

// internal
func (s *keyStable) at(i int) string {
	return s.kvs[i].Key
}

// internal
func (s *keyStable) Swap(i, j int) {
	s.sorted[i], s.sorted[j] = s.sorted[j], s.sorted[i]
}

// internal
func (s *keyStable) Len() int {
	return len(s.sorted)
}

// internal
func (s *keyStable) findKey(needle []byte) (where int, found bool) {
	s.ensureSorted()
	return sort.Find(len(s.sorted), func(i int) int {
		return compareBytesString(needle, s.at(s.sorted[i]))
	})
}

// internal
func (s *keyStable) findKeyString(needle string) (where int, found bool) {
	s.ensureSorted()
	return sort.Find(len(s.sorted), func(i int) int {
		return compareStringString(needle, s.at(s.sorted[i]))
	})
}

// internal
func (s *keyStable) ensureSorted() {

	if !s.sortedDirty {
		return
	}
	sort.Sort(s)
	s.sortedDirty = false
}

// internal
func (s *keyStable) ensureHeadmap() {
	if len(s.headmap) != 0 {
		return
	}
	s.rebuildHeadmap(keyStableBucketCount(len(s.kvs)))
}

func (s *keyStable) rebuildHeadmap(bucketCount int) {
	if bucketCount < keyStableBucketCount(len(s.kvs)) {
		bucketCount = keyStableBucketCount(len(s.kvs))
	}
	s.headmap = make([]int, bucketCount)
	s.nextSameHash = s.nextSameHash[:0]
	for range s.kvs {
		s.nextSameHash = append(s.nextSameHash, -1)
	}
	for _, slotIdx := range s.sorted {
		h := s.hashes[slotIdx]
		bucket := s.bucket(h)
		// because the value 0 back from headmap means not present, we undo the +1 bump
		// (below) by subtracting 1 after pulling from headmap
		s.nextSameHash[slotIdx] = s.headmap[bucket] - 1
		s.headmap[bucket] = slotIdx + 1
	}
}

func (s *keyStable) bucket(h uint64) int {
	return int(h & uint64(len(s.headmap)-1))
}

// internal
func (s *keyStable) findSlotByBytes(key []byte, h uint64) (slotIdx int, found bool) {
	s.ensureHeadmap()
	for slotIdx = s.headmap[s.bucket(h)] - 1; slotIdx >= 0; slotIdx = s.nextSameHash[slotIdx] {
		if s.hashes[slotIdx] == h && compareBytesString(key, s.at(slotIdx)) == 0 {
			return slotIdx, true
		}
	}
	return 0, false
}

// internal
func (s *keyStable) findSlotByString(key string, h uint64) (slotIdx int, found bool) {
	s.ensureHeadmap()
	for slotIdx = s.headmap[s.bucket(h)] - 1; slotIdx >= 0; slotIdx = s.nextSameHash[slotIdx] {
		if s.hashes[slotIdx] == h && key == s.at(slotIdx) {
			return slotIdx, true
		}
	}
	return 0, false
}

// internal
func (s *keyStable) removeSlotFromHeadmap(slotIdx int) {
	h := s.hashes[slotIdx]
	prev := -1
	bucket := s.bucket(h)
	for cur := s.headmap[bucket] - 1; cur >= 0; cur = s.nextSameHash[cur] {
		if cur == slotIdx {
			if prev < 0 {
				s.headmap[bucket] = s.nextSameHash[cur] + 1
			} else {
				s.nextSameHash[prev] = s.nextSameHash[cur]
			}
			s.nextSameHash[cur] = -1
			return
		}
		prev = cur
	}
}

func compareStringString(a string, b string) int {
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

// internal
func (s *keyStable) kvAt(slotIdx int) KV {
	kv := s.kvs[slotIdx]
	return kv
}

// external
func (s *keyStable) set(kv KV, x bool) (old KV, replaced bool) {
	h := xxhash.Sum64String(kv.Key)

	if !x {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	slotIdx, found := s.findSlotByString(kv.Key, h)
	//vv("set(): kv.Key='%v' was found='%v'; slotIdx=%v", kv.Key, found, slotIdx)
	if found {
		old = s.kvAt(slotIdx)
		s.kvs[slotIdx] = kv
		return old, true
	}
	slotIdx = s.appendKeyString(kv.Key, h)
	s.kvs[slotIdx] = kv
	return KV{}, false
}

// external
func (s *keyStable) get(key string, x bool) (kv KV, found bool) {

	var slotIdx int
	h := xxhash.Sum64String(key)

	if !x {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	slotIdx, found = s.findSlotByString(key, h)
	if !found {
		return
	}
	return s.kvAt(slotIdx), true
}

// external
func (s *keyStable) seekGE(target string, strict bool, x bool) (KV, bool) {
	// called by iter.go Iter.Seek etc.
	if !x {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
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

// external
func (s *keyStable) seekLE(target string, strict bool, x bool) (KV, bool) {
	if !x {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

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

// external
func (s *keyStable) Ascend(x bool, pivot KV, iter func(KV) bool) {
	if !x {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	w, _ := s.findKeyString(pivot.Key)
	for ; w < len(s.sorted); w++ {
		if !iter(s.kvAt(s.sorted[w])) {
			return
		}
	}
}

// external
func (s *keyStable) Descend(x bool, pivot KV, iter func(KV) bool) {
	if !x {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	w := len(s.sorted)
	if w == 0 {
		return
	}
	var found bool
	if pivot.Key == "" {
		// start from largest key, like tx.go:576 says.
		s.ensureSorted()
	} else {
		// calls sort.Find(), which returns w = len(s.sorted) if not found
		w, found = s.findKeyString(pivot.Key)
	}
	if !found {
		w-- // so w is len(s.sorted) - 1, the last legal index.
	}
	for ; w >= 0; w-- {
		if !iter(s.kvAt(s.sorted[w])) {
			return
		}
	}
}

// external
func (s *keyStable) Scan(x bool, iter func(KV) bool) {
	if !x {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	s.ensureSorted()
	for _, slotIdx := range s.sorted {
		if !iter(s.kvAt(slotIdx)) {
			return
		}
	}
}

// external
func (s *keyStable) Reverse(x bool, iter func(KV) bool) {
	if !x {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	s.ensureSorted()
	for i := len(s.sorted) - 1; i >= 0; i-- {
		if !iter(s.kvAt(s.sorted[i])) {
			return
		}
	}
}

// test-only use by keystable_test.go, so does not need to lock mu.
// might be able to get rid of, but keystable_test.go uses it a bit.
func (s *keyStable) delKey(needle []byte) (found bool) {

	var w int // where the needle lives in s.sorted
	w, found = s.findKey(needle)
	if !found {
		return
	}
	deleted := s.sorted[w]
	s.removeSlotFromHeadmap(deleted)
	s.kvs[deleted] = KV{}

	last := len(s.sorted) - 1
	if w < last {
		copy(s.sorted[w:], s.sorted[w+1:])
	}
	s.sorted = s.sorted[:last]
	return
}

// test/debugging only. does not lock mu.
func (s *keyStable) String() string {
	keys := "["
	for i := range s.kvs {
		keys += fmt.Sprintf("'%v', ", string(s.at(i)))
	}
	keys += "]"
	return fmt.Sprintf(`keyStable{
	keys: %v
	sorted: %#v
	kvs: %#v
	nextSameHash: %#v
	hashes: %#v
	headmap: %#v
	sortedDirty: %v
}
`, keys,
		s.sorted,
		s.kvs,
		s.nextSameHash,
		s.hashes,
		s.headmap,
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
