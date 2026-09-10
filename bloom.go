package yogadb

import (
	"math/bits"

	"github.com/cespare/xxhash/v2"
)

const (
	keyBloomInitialCapacity = 1 << 20
	keyBloomBitsPerKey      = 10
	keyBloomProbes          = 3
)

type keyBloom struct {
	levels []keyBloomLevel
}

type keyBloomLevel struct {
	bits     []uint64
	mask     uint64
	capacity uint64
	count    uint64
}

func newKeyBloom() *keyBloom {
	return &keyBloom{}
}

func (b *keyBloom) reset() {
	b.levels = nil
}

func (b *keyBloom) addString(key string) {
	h1, h2 := keyBloomHashes(key)
	if b.mayContainHashes(h1, h2) {
		return
	}
	b.addHashesKnownAbsent(h1, h2)
}

func (b *keyBloom) addStringKnownAbsent(key string) {
	h1, h2 := keyBloomHashes(key)
	b.addHashesKnownAbsent(h1, h2)
}

func (b *keyBloom) addHashesKnownAbsent(h1, h2 uint64) {
	if len(b.levels) == 0 || b.levels[len(b.levels)-1].count >= b.levels[len(b.levels)-1].capacity {
		capacity := uint64(keyBloomInitialCapacity)
		if len(b.levels) > 0 {
			capacity = b.levels[len(b.levels)-1].capacity << 1
		}
		b.levels = append(b.levels, newKeyBloomLevel(capacity))
	}
	b.levels[len(b.levels)-1].addHashes(h1, h2)
}

func (b *keyBloom) mayContainString(key string) bool {
	if b == nil || len(b.levels) == 0 {
		return false
	}
	h1, h2 := keyBloomHashes(key)
	return b.mayContainHashes(h1, h2)
}

func (b *keyBloom) mayContainHashes(h1, h2 uint64) bool {
	for i := range b.levels {
		if b.levels[i].mayContainHashes(h1, h2) {
			return true
		}
	}
	return false
}

func newKeyBloomLevel(capacity uint64) keyBloomLevel {
	if capacity == 0 {
		capacity = 1
	}
	bitCount := capacity * keyBloomBitsPerKey
	if bitCount < 64 {
		bitCount = 64
	}
	bitCount = uint64(1) << bits.Len64(bitCount-1)
	return keyBloomLevel{
		bits:     make([]uint64, bitCount>>6),
		mask:     bitCount - 1,
		capacity: capacity,
	}
}

func keyBloomHashes(key string) (uint64, uint64) {
	h1 := xxhash.Sum64String(key)
	h2 := bits.RotateLeft64(h1, 31) ^ 0x9e3779b97f4a7c15
	return h1, h2 | 1
}

func (l *keyBloomLevel) addHashes(h1, h2 uint64) {
	for i := uint64(0); i < keyBloomProbes; i++ {
		bit := (h1 + i*h2) & l.mask
		l.bits[bit>>6] |= uint64(1) << (bit & 63)
	}
	l.count++
}

func (l *keyBloomLevel) mayContainHashes(h1, h2 uint64) bool {
	for i := uint64(0); i < keyBloomProbes; i++ {
		bit := (h1 + i*h2) & l.mask
		if l.bits[bit>>6]&(uint64(1)<<(bit&63)) == 0 {
			return false
		}
	}
	return true
}
