package yogadb

// brute force flextree extent management equivalent for testing.

// BruteForceExtent is for comparison to FlexTree in testing
type BruteForceExtent struct {
	Loff uint64 `zid:"0"`
	Len  uint32 `zid:"1"`
	Poff uint64 `zid:"2"`
	Tag  uint16 `zid:"3"`
}

// BruteForce is for comparison to FlexTree in testing.
type BruteForce struct {
	MaxLoff       uint64             `zid:"0"`
	MaxExtentSize uint32             `zid:"1"`
	Extents       []BruteForceExtent `zid:"2"`
}

func OpenBruteForce(maxExtentSize uint32) *BruteForce {
	return &BruteForce{
		MaxExtentSize: maxExtentSize,
		Extents:       make([]BruteForceExtent, 0, 1024),
	}
}

func (bf *BruteForce) Close() {}

func (bf *BruteForce) findPos(loff uint64) int {
	hi := len(bf.Extents)
	lo := 0
	for lo+1 < hi {
		target := int((uint64(lo) + uint64(hi)) >> 1)

		if bf.Extents[target].Loff <= loff {
			lo = target
		} else {
			hi = target
		}
	}
	target := lo
	for target < len(bf.Extents) {
		ext := &bf.Extents[target]
		if (ext.Loff <= loff && ext.Loff+uint64(ext.Len) > loff) ||
			ext.Loff > loff {

			break
		}
		target++
	}
	return target
}

func (bf *BruteForce) isExtentSequential(ext *BruteForceExtent, loff, poff uint64, length uint32) bool {
	return ext.Poff+uint64(ext.Len) == poff &&
		ext.Loff+uint64(ext.Len) == loff &&
		ext.Len+length <= bf.MaxExtentSize
}

func (bf *BruteForce) insertR(loff, poff uint64, length uint32, tag uint16) int {
	if length == 0 {
		return 0
	}
	if length > bf.MaxExtentSize {
		return -1
	}
	if loff > bf.MaxLoff {
		hlen := loff - bf.MaxLoff
		hloff := bf.MaxLoff
		const FLEXTREE_HOLE = 1 << 47
		hpoff := uint64(FLEXTREE_HOLE)
		for hlen != 0 {
			thlen := bf.MaxExtentSize
			if hlen < uint64(bf.MaxExtentSize) {
				thlen = uint32(hlen)
			}
			if r := bf.insertR(hloff, hpoff, thlen, 0); r != 0 {
				return -1
			}
			hlen -= uint64(thlen)
			hloff += uint64(thlen)
			hpoff += uint64(thlen)
		}
	}

	const FLEXTREE_POFF_MASK = 0xffffffffffff
	t := BruteForceExtent{Loff: loff, Len: length, Poff: poff & FLEXTREE_POFF_MASK, Tag: tag}
	target := bf.findPos(loff)

	shift := 1
	if target == len(bf.Extents) {
		if target > 0 && tag == 0 && bf.isExtentSequential(&bf.Extents[target-1], loff, poff, length) {
			bf.Extents[target-1].Len += length
		} else {
			bf.Extents = append(bf.Extents, t)
		}
	} else {
		curr := &bf.Extents[target]
		if curr.Loff == loff {
			if target > 0 && tag == 0 && bf.isExtentSequential(&bf.Extents[target-1], loff, poff, length) {
				bf.Extents[target-1].Len += length
				shift = 0
			} else {
				bf.Extents = append(bf.Extents, BruteForceExtent{})
				copy(bf.Extents[target+1:], bf.Extents[target:])
				bf.Extents[target] = t
			}
		} else {
			shift = 2
			so := uint32(loff - curr.Loff)
			left := BruteForceExtent{Loff: curr.Loff, Poff: curr.Poff, Len: so, Tag: curr.Tag}
			right := BruteForceExtent{Loff: curr.Loff + uint64(so), Poff: curr.Poff + uint64(so), Len: curr.Len - so, Tag: 0}

			bf.Extents = append(bf.Extents, BruteForceExtent{}, BruteForceExtent{})
			copy(bf.Extents[target+3:], bf.Extents[target+1:])

			bf.Extents[target] = left
			bf.Extents[target+2] = right
			bf.Extents[target+1] = t
		}
	}

	for i := target + shift; i < len(bf.Extents); i++ {
		bf.Extents[i].Loff += uint64(length)
	}
	bf.MaxLoff += uint64(length)
	return 0
}

func (bf *BruteForce) Insert(loff, poff uint64, length uint32) int {
	return bf.insertR(loff, poff, length, 0)
}

func (bf *BruteForce) InsertWTag(loff, poff uint64, length uint32, tag uint16) int {
	return bf.insertR(loff, poff, length, tag)
}

func (bf *BruteForce) rangeCount(loff, length uint64) uint64 {
	ret := uint64(0)
	oloff := loff
	olen := length
	target := bf.findPos(oloff)
	for olen > 0 && target < len(bf.Extents) {
		ext := &bf.Extents[target]
		tlen := ext.Loff + uint64(ext.Len) - oloff
		if tlen > olen {
			tlen = olen
		}
		oloff += tlen
		olen -= tlen
		ret++
		target++
	}
	if olen > 0 {
		return 0 // Matches logic where it might not have enough
	}
	return ret
}

func (bf *BruteForce) Query(loff, length uint64) *FlextreeQueryResult {
	if loff+length > bf.MaxLoff {
		return nil
	}
	count := bf.rangeCount(loff, length)
	if count == 0 {
		return nil
	}
	rr := &FlextreeQueryResult{
		Loff:  loff,
		Len:   length,
		Count: 0,
		V:     make([]Offlen, count),
	}

	i := uint64(0)
	oloff := loff
	olen := length
	target := bf.findPos(oloff)
	for olen > 0 {
		if target >= len(bf.Extents) {
			return nil
		}
		ext := &bf.Extents[target]
		if ext.Loff > oloff || ext.Loff+uint64(ext.Len) <= oloff {
			return nil
		}
		tlen := ext.Loff + uint64(ext.Len) - oloff
		if tlen > olen {
			tlen = olen
		}
		rr.V[i].Poff = ext.Poff + (oloff - ext.Loff)
		rr.V[i].Len = tlen
		oloff += tlen
		olen -= tlen
		i++
		target++
	}
	rr.Count = i
	return rr
}

func (bf *BruteForce) GetMaxLoff() uint64 {
	return bf.MaxLoff
}

func (bf *BruteForce) PQuery(loff uint64) uint64 {
	if loff >= bf.MaxLoff {
		return ^uint64(0)
	}
	target := bf.findPos(loff)
	if target >= len(bf.Extents) {
		return ^uint64(0)
	}
	ext := &bf.Extents[target]
	if ext.Loff <= loff && ext.Loff+uint64(ext.Len) > loff {
		return ext.Poff + loff - ext.Loff
	}
	return ^uint64(0)
}

func (bf *BruteForce) Delete(loff, length uint64) int {
	if loff+length > bf.MaxLoff {
		return -1
	}
	olen := length
	for olen > 0 {
		target := bf.findPos(loff)
		if target >= len(bf.Extents) {
			break
		}
		ext := &bf.Extents[target]
		tlen := uint32(ext.Loff + uint64(ext.Len) - loff)
		if uint64(tlen) > olen {
			tlen = uint32(olen)
		}

		shift := 1
		if ext.Loff == loff {
			ext.Len -= tlen
			ext.Poff += uint64(tlen)
			ext.Tag = 0
			if ext.Len == 0 {
				bf.Extents = append(bf.Extents[:target], bf.Extents[target+1:]...)
				shift = 0
			}
		} else {
			tmp := uint32(loff - ext.Loff)
			if ext.Len-tmp == tlen {
				ext.Len -= tlen
			} else {
				right := BruteForceExtent{Loff: loff + uint64(tlen), Poff: ext.Poff + uint64(tmp) + uint64(tlen), Len: ext.Len - tmp - tlen, Tag: 0}
				bf.Extents = append(bf.Extents, BruteForceExtent{})
				copy(bf.Extents[target+2:], bf.Extents[target+1:])
				bf.Extents[target].Len = tmp // use index; ext pointer is stale after append realloc
				bf.Extents[target+1] = right
			}
		}
		for i := target + shift; i < len(bf.Extents); i++ {
			bf.Extents[i].Loff -= uint64(tlen)
		}
		olen -= uint64(tlen)
	}
	bf.MaxLoff -= length
	return 0
}

func (bf *BruteForce) PDelete(loff uint64) int {
	return bf.Delete(loff, 1)
}

func (bf *BruteForce) SetTag(loff uint64, tag uint16) int {
	if loff >= bf.MaxLoff {
		return -1
	}
	target := bf.findPos(loff)
	if target >= len(bf.Extents) {
		return -1
	}

	ext := &bf.Extents[target]
	if ext.Loff == loff {
		ext.Tag = tag
	} else {
		so := uint32(loff - ext.Loff)
		left := BruteForceExtent{Loff: ext.Loff, Poff: ext.Poff, Len: so, Tag: ext.Tag}
		right := BruteForceExtent{Loff: ext.Loff + uint64(so), Poff: ext.Poff + uint64(so), Len: ext.Len - so, Tag: tag}

		bf.Extents = append(bf.Extents, BruteForceExtent{})
		copy(bf.Extents[target+2:], bf.Extents[target+1:])

		bf.Extents[target] = left
		bf.Extents[target+1] = right
	}
	return 0
}

func (bf *BruteForce) GetTag(loff uint64) (uint16, int) {
	if loff >= bf.MaxLoff {
		return 0, -1
	}
	target := bf.findPos(loff)
	if target >= len(bf.Extents) {
		return 0, -1
	}
	ext := &bf.Extents[target]
	if ext.Loff == loff && ext.Tag != 0 {
		return ext.Tag, 0
	}
	return 0, -1
}
