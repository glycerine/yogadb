package yogadb

type bulkIngestRef uint64

type bulkIngestSegment struct {
	kvs []KV
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
	lastKey                string
	dirty                  bool
	allSmallInlineZeroVtyp bool
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
	b.lastKey = ""
	b.dirty = false
	b.allSmallInlineZeroVtyp = true
	b.count = 0
	b.size = 0
}

func (b *bulkIngestBuilder) appendBatch(kvs []KV) {
	if len(kvs) == 0 {
		return
	}
	if b.count == 0 && len(b.segments) == 0 {
		b.allSmallInlineZeroVtyp = true
		b.sorted = true
		b.lastKey = ""
	}
	b.segments = append(b.segments, bulkIngestSegment{kvs: kvs})
	for i := range kvs {
		if b.sorted {
			if b.count > 0 && b.lastKey > kvs[i].Key {
				b.sorted = false
			} else {
				b.lastKey = kvs[i].Key
			}
		}
		b.size += int64(kvSizeApprox(&kvs[i]))
		if !slottedKVSmallInlineZeroVtyp(kvs[i]) {
			b.allSmallInlineZeroVtyp = false
		}
		b.count++
	}
	b.index = nil
	b.dirty = true
}

func (b *bulkIngestBuilder) appendKV(kv KV) {
	b.appendBatch([]KV{kv})
}

func (b *bulkIngestBuilder) kv(ref bulkIngestRef) KV {
	return b.segments[bulkIngestRefSeg(ref)].kvs[bulkIngestRefIdx(ref)]
}

func (b *bulkIngestBuilder) ensureIndex() {
	if b.count == 0 || b.index != nil {
		return
	}
	b.index = make(map[string]bulkIngestRef, b.count)
	for si := range b.segments {
		kvs := b.segments[si].kvs
		for ki := range kvs {
			b.index[kvs[ki].Key] = makeBulkIngestRef(si, ki)
		}
	}
}

func (b *bulkIngestBuilder) get(key string) (KV, bool) {
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
	b.keys = b.keys[:0]
	fixedKeyLen := -1
	fixedKeyLenOK := true
	for si := range b.segments {
		kvs := b.segments[si].kvs
		for ki := range kvs {
			b.order = append(b.order, makeBulkIngestRef(si, ki))
			key := kvs[ki].Key
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
			sortBulkIngestRefsByFixedKeyLen(b.order, b.sortAux, b.keys, b.keyAux, fixedKeyLen)
		}
	} else {
		b.fixedKeyLen = -1
		if !b.sorted {
			sortBulkIngestRefsByKey(b.order, b.sortAux, b.keys, b.keyAux)
		}
	}
	return b.order
}

func sortBulkIngestRefsByKey(order, aux []bulkIngestRef, keys, keyAux []string) {
	if len(order) < 2 {
		return
	}
	sortBulkIngestRefsByKeyMSD(order, aux, keys, keyAux, 0)
}

func sortBulkIngestRefsByFixedKeyLen(order, aux []bulkIngestRef, keys, keyAux []string, keyLen int) {
	if len(order) < 2 || keyLen == 0 {
		return
	}
	sortBulkIngestRefsByFixedKeyLenMSD(order, aux, keys, keyAux, 0, keyLen)
}

func sortBulkIngestRefsByFixedKeyLenMSD(order, aux []bulkIngestRef, keys, keyAux []string, depth, keyLen int) {
	if len(order) <= bulkRadixInsertionCutoff || depth >= keyLen {
		insertionSortBulkIngestRefs(order, keys)
		return
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
	copy(order, aux[:len(order)])
	copy(keys, keyAux[:len(keys)])

	for c := 0; c < 256; c++ {
		lo := start[c]
		hi := count[c]
		if hi-lo > 1 {
			sortBulkIngestRefsByFixedKeyLenMSD(order[lo:hi], aux[lo:hi], keys[lo:hi], keyAux[lo:hi], depth+1, keyLen)
		}
	}
}

func sortBulkIngestRefsByKeyMSD(order, aux []bulkIngestRef, keys, keyAux []string, depth int) {
	if len(order) <= bulkRadixInsertionCutoff {
		insertionSortBulkIngestRefs(order, keys)
		return
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
	copy(order, aux[:len(order)])
	copy(keys, keyAux[:len(keys)])

	for c := 1; c < 257; c++ {
		lo := start[c]
		hi := count[c]
		if hi-lo > 1 {
			sortBulkIngestRefsByKeyMSD(order[lo:hi], aux[lo:hi], keys[lo:hi], keyAux[lo:hi], depth+1)
		}
	}
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
