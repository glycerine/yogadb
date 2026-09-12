/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 * Modifications copyright (C) 2017 Andy Kimball and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pebbleskip

import (
	"bytes"
	"errors"
	"math"
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"unsafe"
)

const (
	maxHeight   = 20
	maxNodeSize = int(unsafe.Sizeof(node{}))
	linksSize   = int(unsafe.Sizeof(links{}))
	pValue      = 1 / math.E
)

// Compare defines the ordering over user keys.
type Compare func(a, b []byte) int

// DefaultCompare is bytes.Compare.
var DefaultCompare Compare = bytes.Compare

// KV is an iterator result. Key and Value alias immutable arena memory and
// remain valid for the lifetime of the skiplist.
type KV struct {
	Key   []byte
	Value []byte
}

// ErrRecordExists indicates that an entry with the specified key already
// exists. This fork keeps Pebble's immutable-node model: updates and deletes
// should be represented by higher-level version/tombstone keys if needed.
var ErrRecordExists = errors.New("record with this key already exists")

// Skiplist is a fork of Pebble's arena-backed concurrent skiplist. It supports
// concurrent inserts and concurrent forward/backward iteration. Keys and values
// are immutable once added. Deletion is not supported.
type Skiplist struct {
	arena  *Arena
	cmp    Compare
	head   *node
	tail   *node
	height atomic.Uint32 // Current height. 1 <= height <= maxHeight. CAS.

	testing bool
}

// Inserter caches splice positions across repeated Add calls by one goroutine.
// It is not safe for concurrent use by multiple goroutines.
type Inserter struct {
	spl    [maxHeight]splice
	height uint32
}

// Add inserts key/value using this inserter's cached splice state.
func (ins *Inserter) Add(list *Skiplist, key, value []byte) error {
	return list.addInternal(key, value, ins)
}

var probabilities [maxHeight]uint32

func init() {
	p := float64(1.0)
	for i := 0; i < maxHeight; i++ {
		probabilities[i] = uint32(float64(math.MaxUint32) * p)
		p *= pValue
	}
}

// New constructs a skiplist with a fresh arena of arenaBytes bytes.
func New(arenaBytes int, cmp Compare) *Skiplist {
	return NewSkiplist(NewArena(make([]byte, arenaBytes)), cmp)
}

// NewSkiplist constructs and initializes a new, empty skiplist. All nodes,
// keys, and values are allocated from arena.
func NewSkiplist(arena *Arena, cmp Compare) *Skiplist {
	skl := &Skiplist{}
	skl.Reset(arena, cmp)
	return skl
}

// Reset empties and re-initializes the skiplist over arena.
func (s *Skiplist) Reset(arena *Arena, cmp Compare) {
	if arena == nil {
		panic("nil arena")
	}
	if cmp == nil {
		cmp = DefaultCompare
	}

	head, err := newRawNode(arena, maxHeight, 0, 0)
	if err != nil {
		panic("arena is not large enough to hold the head node")
	}
	head.keyOffset = 0

	tail, err := newRawNode(arena, maxHeight, 0, 0)
	if err != nil {
		panic("arena is not large enough to hold the tail node")
	}
	tail.keyOffset = 0

	headOffset := arena.getPointerOffset(unsafe.Pointer(head))
	tailOffset := arena.getPointerOffset(unsafe.Pointer(tail))
	for i := 0; i < maxHeight; i++ {
		head.tower[i].nextOffset.Store(tailOffset)
		tail.tower[i].prevOffset.Store(headOffset)
	}

	*s = Skiplist{
		arena: arena,
		cmp:   cmp,
		head:  head,
		tail:  tail,
	}
	s.height.Store(1)
}

// EnableTestingDelays adds scheduler yields at sensitive insertion points. It
// is useful for race/concurrency tests.
func (s *Skiplist) EnableTestingDelays(v bool) {
	s.testing = v
}

// Height returns the height of the highest tower ever allocated.
func (s *Skiplist) Height() uint32 { return s.height.Load() }

// Arena returns the arena backing this skiplist.
func (s *Skiplist) Arena() *Arena { return s.arena }

// Size returns the number of bytes allocated from the arena.
func (s *Skiplist) Size() uint32 { return s.arena.Size() }

// Add inserts key/value if key does not already exist.
func (s *Skiplist) Add(key, value []byte) error {
	var ins Inserter
	return s.addInternal(key, value, &ins)
}

// Get returns the value for key. The returned value aliases immutable arena
// memory and remains valid for the lifetime of the skiplist.
func (s *Skiplist) Get(key []byte) ([]byte, bool) {
	it := Iterator{list: s, nd: s.head}
	kv := it.SeekGE(key)
	if kv == nil || s.cmp(kv.Key, key) != 0 {
		return nil, false
	}
	return kv.Value, true
}

func (s *Skiplist) addInternal(key, value []byte, ins *Inserter) error {
	if s.findSplice(key, ins) {
		return ErrRecordExists
	}

	if s.testing {
		runtime.Gosched()
	}

	nd, height, err := s.newNode(key, value)
	if err != nil {
		return err
	}

	ndOffset := s.arena.getPointerOffset(unsafe.Pointer(nd))

	var found bool
	var invalidateSplice bool
	for i := 0; i < int(height); i++ {
		prev := ins.spl[i].prev
		next := ins.spl[i].next

		if prev == nil {
			if next != nil {
				panic("next is expected to be nil, since prev is nil")
			}
			prev = s.head
			next = s.tail
		}

		for {
			prevOffset := s.arena.getPointerOffset(unsafe.Pointer(prev))
			nextOffset := s.arena.getPointerOffset(unsafe.Pointer(next))
			nd.tower[i].init(prevOffset, nextOffset)

			nextPrevOffset := next.prevOffset(i)
			if nextPrevOffset != prevOffset {
				prevNextOffset := prev.nextOffset(i)
				if prevNextOffset == nextOffset {
					next.casPrevOffset(i, nextPrevOffset, prevOffset)
				}
			}

			if prev.casNextOffset(i, nextOffset, ndOffset) {
				if s.testing {
					runtime.Gosched()
				}
				next.casPrevOffset(i, prevOffset, ndOffset)
				break
			}

			prev, next, found = s.findSpliceForLevel(key, i, prev)
			if found {
				if i != 0 {
					panic("another goroutine inserted a node at a non-base level")
				}
				return ErrRecordExists
			}
			invalidateSplice = true
		}
	}

	if invalidateSplice {
		ins.height = 0
	} else {
		for i := uint32(0); i < height; i++ {
			ins.spl[i].prev = nd
		}
	}

	return nil
}

// NewIter returns a pooled iterator. Bounds are optional; nil disables a bound.
// It is safe to copy an iterator by value. Call Close when done if the iterator
// was allocated by NewIter.
func (s *Skiplist) NewIter(lower, upper []byte) *Iterator {
	it := iterPool.Get().(*Iterator)
	*it = Iterator{list: s, nd: s.head, lower: lower, upper: upper}
	return it
}

func (s *Skiplist) newNode(key, value []byte) (nd *node, height uint32, err error) {
	height = s.randomHeight()
	nd, err = newNode(s.arena, height, key, value)
	if err != nil {
		return nil, 0, err
	}

	listHeight := s.Height()
	for height > listHeight {
		if s.height.CompareAndSwap(listHeight, height) {
			break
		}
		listHeight = s.Height()
	}

	return nd, height, nil
}

func (s *Skiplist) randomHeight() uint32 {
	rnd := rand.Uint32()

	h := uint32(1)
	for h < maxHeight && rnd <= probabilities[h] {
		h++
	}

	return h
}

func (s *Skiplist) findSplice(key []byte, ins *Inserter) (found bool) {
	listHeight := s.Height()
	var level int

	prev := s.head
	if ins.height < listHeight {
		ins.height = listHeight
		level = int(ins.height)
	} else {
		for ; level < int(listHeight); level++ {
			spl := &ins.spl[level]
			if s.getNext(spl.prev, level) != spl.next {
				continue
			}
			if spl.prev != s.head && !s.keyIsAfterNode(spl.prev, key) {
				level = int(listHeight)
				break
			}
			if spl.next != s.tail && s.keyIsAfterNode(spl.next, key) {
				level = int(listHeight)
				break
			}
			prev = spl.prev
			break
		}
	}

	for level = level - 1; level >= 0; level-- {
		var next *node
		prev, next, found = s.findSpliceForLevel(key, level, prev)
		if next == nil {
			next = s.tail
		}
		ins.spl[level].init(prev, next)
	}

	return found
}

func (s *Skiplist) findSpliceForLevel(key []byte, level int, start *node) (prev, next *node, found bool) {
	prev = start

	for {
		next = s.getNext(prev, level)
		if next == s.tail {
			break
		}

		nextKey := next.getKeyBytes(s.arena)
		cmp := s.cmp(key, nextKey)
		if cmp < 0 {
			break
		}
		if cmp == 0 {
			found = true
			break
		}

		prev = next
	}

	return prev, next, found
}

func (s *Skiplist) keyIsAfterNode(nd *node, key []byte) bool {
	ndKey := nd.getKeyBytes(s.arena)
	return s.cmp(ndKey, key) < 0
}

func (s *Skiplist) getNext(nd *node, h int) *node {
	offset := nd.tower[h].nextOffset.Load()
	return (*node)(s.arena.getPointer(offset))
}

func (s *Skiplist) getPrev(nd *node, h int) *node {
	offset := nd.tower[h].prevOffset.Load()
	return (*node)(s.arena.getPointer(offset))
}
