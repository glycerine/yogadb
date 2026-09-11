package yogadb

import "unsafe"

type bulkIngestRef uint64

type bulkIngestSegment struct {
	kvs          []KV
	aliasKeys    []string
	aliasKeysHLC HLC
}

func (s *bulkIngestSegment) len() int {
	if len(s.aliasKeys) > 0 {
		return len(s.aliasKeys)
	}
	return len(s.kvs)
}

func (s *bulkIngestSegment) key(i int) string {
	if len(s.aliasKeys) > 0 {
		return s.aliasKeys[i]
	}
	return s.kvs[i].Key
}

func (s *bulkIngestSegment) kv(i int) KV {
	if len(s.aliasKeys) > 0 {
		key := s.aliasKeys[i]
		return valueIsKeyKV(key, s.aliasKeysHLC)
	}
	return s.kvs[i]
}

type bulkIngestBuilder struct {
	segments               []bulkIngestSegment
	order                  []bulkIngestRef
	sortAux                []bulkIngestRef
	keys                   []string
	keyAux                 []string
	index                  map[string]bulkIngestRef
	fixedKeyLen            int
	sorted                 bool
	sortedHasDuplicates    bool
	hasTombstones          bool
	lastKey                string
	dirty                  bool
	allSmallInlineZeroVtyp bool
	allValuesAliasKeys     bool
	count                  int
	size                   int64
}

const invalidBulkIngestRef bulkIngestRef = ^bulkIngestRef(0)

func makeBulkIngestRef(seg, idx int) bulkIngestRef {
	return bulkIngestRef(uint64(uint32(seg))<<32 | uint64(uint32(idx)))
}

func bulkIngestRefSeg(ref bulkIngestRef) int {
	return int(uint64(ref) >> 32)
}

func bulkIngestRefIdx(ref bulkIngestRef) int {
	return int(uint32(ref))
}

func (b *bulkIngestBuilder) reset() {
	b.segments = b.segments[:0]
	b.order = b.order[:0]
	b.sortAux = b.sortAux[:0]
	b.keys = b.keys[:0]
	b.keyAux = b.keyAux[:0]
	b.index = nil
	b.fixedKeyLen = -1
	b.sorted = true
	b.sortedHasDuplicates = false
	b.hasTombstones = false
	b.lastKey = ""
	b.dirty = false
	b.allSmallInlineZeroVtyp = true
	b.allValuesAliasKeys = true
	b.count = 0
	b.size = 0
}

func (b *bulkIngestBuilder) appendBatch(kvs []KV, valuesAliasKeys bool) {
	if len(kvs) == 0 {
		return
	}
	if b.count == 0 && len(b.segments) == 0 {
		b.allSmallInlineZeroVtyp = true
		b.allValuesAliasKeys = true
		b.sorted = true
		b.sortedHasDuplicates = false
		b.hasTombstones = false
		b.lastKey = ""
	}
	if !valuesAliasKeys {
		b.allValuesAliasKeys = false
	}
	b.segments = append(b.segments, bulkIngestSegment{kvs: kvs})
	for i := range kvs {
		if b.sorted {
			if b.count > 0 && b.lastKey > kvs[i].Key {
				b.sorted = false
			} else if b.count > 0 && b.lastKey == kvs[i].Key {
				b.sortedHasDuplicates = true
				b.lastKey = kvs[i].Key
			} else {
				b.lastKey = kvs[i].Key
			}
		}
		b.size += int64(kvSizeApprox(&kvs[i]))
		if kvs[i].isTombstone() {
			b.hasTombstones = true
		}
		if !slottedKVSmallInlineZeroVtyp(kvs[i]) {
			b.allSmallInlineZeroVtyp = false
		}
		b.count++
	}
	b.index = nil
	b.dirty = true
}

func (b *bulkIngestBuilder) appendValueIsKeyBatch(keys []string, hlc HLC) {
	if len(keys) == 0 {
		return
	}
	if b.count == 0 && len(b.segments) == 0 {
		b.allSmallInlineZeroVtyp = true
		b.allValuesAliasKeys = true
		b.sorted = true
		b.sortedHasDuplicates = false
		b.hasTombstones = false
		b.lastKey = ""
	}
	b.segments = append(b.segments, bulkIngestSegment{aliasKeys: keys, aliasKeysHLC: hlc})
	for i := range keys {
		if b.sorted {
			if b.count > 0 && b.lastKey > keys[i] {
				b.sorted = false
			} else if b.count > 0 && b.lastKey == keys[i] {
				b.sortedHasDuplicates = true
				b.lastKey = keys[i]
			} else {
				b.lastKey = keys[i]
			}
		}
		b.size += int64(24 + len(keys[i]))
		if len(keys[i]) > vlogInlineThreshold {
			b.allSmallInlineZeroVtyp = false
		}
		b.count++
	}
	b.index = nil
	b.dirty = true
}

func (b *bulkIngestBuilder) appendKV(kv KV) {
	b.appendBatch([]KV{kv}, slottedInlineValueAliasesKey(kv))
}

func (b *bulkIngestBuilder) kv(ref bulkIngestRef) KV {
	return b.segments[bulkIngestRefSeg(ref)].kv(bulkIngestRefIdx(ref))
}

func (b *bulkIngestBuilder) hlc(ref bulkIngestRef) HLC {
	seg := &b.segments[bulkIngestRefSeg(ref)]
	if len(seg.aliasKeys) > 0 {
		return seg.aliasKeysHLC
	}
	return seg.kvs[bulkIngestRefIdx(ref)].Hlc
}

func (b *bulkIngestBuilder) ensureIndex() {
	if b.count == 0 || b.index != nil {
		return
	}
	b.index = make(map[string]bulkIngestRef, b.count)
	for si := range b.segments {
		seg := &b.segments[si]
		for ki, n := 0, seg.len(); ki < n; ki++ {
			b.index[seg.key(ki)] = makeBulkIngestRef(si, ki)
		}
	}
}

func (b *bulkIngestBuilder) get(key string, x bool) (KV, bool) {
	b.ensureIndex()
	ref, ok := b.index[key]
	if !ok {
		return KV{}, false
	}
	return b.kv(ref), true
}

func (b *bulkIngestBuilder) buildOrder() []bulkIngestRef {
	b.order = b.order[:0]
	b.fixedKeyLen = -1
	if b.count == 0 {
		return b.order
	}
	if cap(b.order) < b.count {
		b.order = make([]bulkIngestRef, 0, b.count)
	}
	if cap(b.keys) < b.count {
		b.keys = make([]string, 0, b.count)
	}
	b.keys = b.keys[:0]
	fixedKeyLen := -1
	fixedKeyLenOK := true
	for si := range b.segments {
		seg := &b.segments[si]
		for ki, n := 0, seg.len(); ki < n; ki++ {
			b.order = append(b.order, makeBulkIngestRef(si, ki))
			key := seg.key(ki)
			b.keys = append(b.keys, key)
			if fixedKeyLen < 0 {
				fixedKeyLen = len(key)
			} else if fixedKeyLenOK && len(key) != fixedKeyLen {
				fixedKeyLenOK = false
			}
		}
	}
	if fixedKeyLenOK && fixedKeyLen >= 0 {
		b.fixedKeyLen = fixedKeyLen
		if !b.sorted {
			b.ensureSortAux()
			sortBulkIngestRefsByFixedKeyLen(b.order, b.sortAux, b.keys, b.keyAux, fixedKeyLen)
		}
	} else {
		b.fixedKeyLen = -1
		if !b.sorted {
			b.ensureSortAux()
			sortBulkIngestRefsByKey(b.order, b.sortAux, b.keys, b.keyAux)
		}
	}
	return b.order
}

func valueIsKeyKV(key string, hlc HLC) KV {
	var value []byte
	if len(key) > 0 {
		value = unsafe.Slice(unsafe.StringData(key), len(key))
	}
	return KV{
		Key:   key,
		Value: value,
		Vptr:  VPtr{Length: uint64(len(key))},
		Hlc:   hlc,
	}
}

func (b *bulkIngestBuilder) ensureSortAux() {
	if cap(b.sortAux) < b.count {
		b.sortAux = make([]bulkIngestRef, b.count)
	} else {
		b.sortAux = b.sortAux[:b.count]
	}
	if cap(b.keyAux) < b.count {
		b.keyAux = make([]string, b.count)
	} else {
		b.keyAux = b.keyAux[:b.count]
	}
}

func sortBulkIngestRefsByKey(order, aux []bulkIngestRef, keys, keyAux []string) {
	if len(order) < 2 {
		return
	}
	if sortBulkIngestRefsByKeyMSD(order, aux, keys, keyAux, 0) {
		copy(order, aux[:len(order)])
		copy(keys, keyAux[:len(keys)])
	}
}

func sortBulkIngestRefsByFixedKeyLen(order, aux []bulkIngestRef, keys, keyAux []string, keyLen int) {
	if len(order) < 2 || keyLen == 0 {
		return
	}
	if sortBulkIngestRefsByFixedKeyLenMSD(order, aux, keys, keyAux, 0, keyLen) {
		copy(order, aux[:len(order)])
		copy(keys, keyAux[:len(keys)])
	}
}

func sortBulkIngestRefsByFixedKeyLenMSD(order, aux []bulkIngestRef, keys, keyAux []string, depth, keyLen int) bool {
	if len(order) <= bulkRadixInsertionCutoff || depth >= keyLen {
		insertionSortBulkIngestRefs(order, keys)
		return false
	}

	var count [256]int
	for _, key := range keys {
		count[key[depth]]++
	}
	sum := 0
	for i := 0; i < 256; i++ {
		n := count[i]
		count[i] = sum
		sum += n
	}
	start := count
	for i, ref := range order {
		key := keys[i]
		c := key[depth]
		aux[count[c]] = ref
		keyAux[count[c]] = key
		count[c]++
	}

	for c := 0; c < 256; c++ {
		lo := start[c]
		hi := count[c]
		if hi-lo > 1 {
			if sortBulkIngestRefsByFixedKeyLenMSD(aux[lo:hi], order[lo:hi], keyAux[lo:hi], keys[lo:hi], depth+1, keyLen) {
				copy(aux[lo:hi], order[lo:hi])
				copy(keyAux[lo:hi], keys[lo:hi])
			}
		}
	}
	return true
}

func sortBulkIngestRefsByKeyMSD(order, aux []bulkIngestRef, keys, keyAux []string, depth int) bool {
	if len(order) <= bulkRadixInsertionCutoff {
		insertionSortBulkIngestRefs(order, keys)
		return false
	}

	var count [258]int
	for _, key := range keys {
		count[bulkKeyByte(key, depth)+1]++
	}
	for i := 0; i < 257; i++ {
		count[i+1] += count[i]
	}
	start := count
	for i, ref := range order {
		key := keys[i]
		c := bulkKeyByte(key, depth)
		aux[count[c]] = ref
		keyAux[count[c]] = key
		count[c]++
	}

	for c := 1; c < 257; c++ {
		lo := start[c]
		hi := count[c]
		if hi-lo > 1 {
			if sortBulkIngestRefsByKeyMSD(aux[lo:hi], order[lo:hi], keyAux[lo:hi], keys[lo:hi], depth+1) {
				copy(aux[lo:hi], order[lo:hi])
				copy(keyAux[lo:hi], keys[lo:hi])
			}
		}
	}
	return true
}

func insertionSortBulkIngestRefs(order []bulkIngestRef, keys []string) {
	for i := 1; i < len(order); i++ {
		v := order[i]
		vk := keys[i]
		j := i - 1
		for j >= 0 && vk < keys[j] {
			order[j+1] = order[j]
			keys[j+1] = keys[j]
			j--
		}
		order[j+1] = v
		keys[j+1] = vk
	}
}

func bulkIngestKeysEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	if a[len(a)-1] != b[len(b)-1] {
		return false
	}
	return a == b
}
