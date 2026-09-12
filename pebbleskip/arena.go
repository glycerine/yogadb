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
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"
)

// Arena is a lock-free bump allocator for skiplist nodes, keys, and values.
// The skiplist stores 32-bit offsets into this buffer, so arenas larger than
// maxUint32 bytes are truncated.
type Arena struct {
	n   atomic.Uint64
	buf []byte
}

const (
	nodeAlignment = 4
	maxUint32     = uint64(^uint32(0))
)

// ErrArenaFull indicates that the arena is full and cannot perform any more
// allocations.
var ErrArenaFull = errors.New("allocation failed because arena is full")

// NewArena allocates a new arena using the specified buffer as the backing
// store.
func NewArena(buf []byte) *Arena {
	if uint64(len(buf)) > maxUint32 {
		buf = buf[:maxUint32]
	}
	a := &Arena{buf: buf}
	// Offset zero is reserved as a nil pointer.
	a.n.Store(1)
	return a
}

// Size returns the number of bytes allocated by the arena.
func (a *Arena) Size() uint32 {
	s := a.n.Load()
	if s > maxUint32 {
		return uint32(maxUint32)
	}
	return uint32(s)
}

// Capacity returns the capacity of the arena.
func (a *Arena) Capacity() uint32 {
	return uint32(len(a.buf))
}

func (a *Arena) alloc(size, alignment, overflow uint32) (uint32, error) {
	if alignment == 0 || (alignment&(alignment-1)) != 0 {
		panic(fmt.Sprintf("invalid alignment %d", alignment))
	}
	origSize := a.n.Load()
	if int(origSize) > len(a.buf) {
		return 0, ErrArenaFull
	}

	padded := uint64(size) + uint64(alignment) - 1
	newSize := a.n.Add(padded)
	if newSize+uint64(overflow) > uint64(len(a.buf)) {
		return 0, ErrArenaFull
	}

	offset := (uint32(newSize) - size) & ^(alignment - 1)
	return offset, nil
}

func (a *Arena) getBytes(offset uint32, size uint32) []byte {
	if offset == 0 {
		return nil
	}
	return a.buf[offset : offset+size : offset+size]
}

func (a *Arena) getPointer(offset uint32) unsafe.Pointer {
	if offset == 0 {
		return nil
	}
	return unsafe.Pointer(&a.buf[offset])
}

func (a *Arena) getPointerOffset(ptr unsafe.Pointer) uint32 {
	if ptr == nil {
		return 0
	}
	return uint32(uintptr(ptr) - uintptr(unsafe.Pointer(&a.buf[0])))
}
