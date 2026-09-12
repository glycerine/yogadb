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

import "sync"

type splice struct {
	prev *node
	next *node
}

func (s *splice) init(prev, next *node) {
	s.prev = prev
	s.next = next
}

// Iterator is an iterator over a skiplist. All iterator methods are safe to
// call while other goroutines are inserting into the same list.
type Iterator struct {
	list  *Skiplist
	nd    *node
	kv    KV
	lower string
	upper string

	lowerNode *node
	upperNode *node
}

var iterPool = sync.Pool{
	New: func() interface{} {
		return &Iterator{}
	},
}

// Close resets and returns the iterator to the pool.
func (it *Iterator) Close() {
	*it = Iterator{}
	iterPool.Put(it)
}

// SetBounds sets optional lower and upper bounds. The iterator must be
// repositioned after changing bounds.
func (it *Iterator) SetBounds(lower, upper string) {
	it.lower = lower
	it.upper = upper
	it.lowerNode = nil
	it.upperNode = nil
}

// SeekGE moves to the first key >= key.
func (it *Iterator) SeekGE(key string) *KV {
	_, it.nd, _ = it.seekForBaseSplice(key)
	if it.nd == it.list.tail || it.nd == it.upperNode {
		return nil
	}
	it.decodeKV()
	if it.upper != "" && it.list.cmp(it.upper, it.kv.Key) <= 0 {
		it.upperNode = it.nd
		return nil
	}
	return &it.kv
}

// SeekLT moves to the last key < key.
func (it *Iterator) SeekLT(key string) *KV {
	it.nd, _, _ = it.seekForBaseSplice(key)
	if it.nd == it.list.head || it.nd == it.lowerNode {
		return nil
	}
	it.decodeKV()
	if it.lower != "" && it.list.cmp(it.lower, it.kv.Key) > 0 {
		it.lowerNode = it.nd
		return nil
	}
	return &it.kv
}

// First moves to the first key.
func (it *Iterator) First() *KV {
	it.nd = it.list.getNext(it.list.head, 0)
	if it.nd == it.list.tail || it.nd == it.upperNode {
		return nil
	}
	it.decodeKV()
	if it.upper != "" && it.list.cmp(it.upper, it.kv.Key) <= 0 {
		it.upperNode = it.nd
		return nil
	}
	return &it.kv
}

// Last moves to the last key.
func (it *Iterator) Last() *KV {
	it.nd = it.list.getPrev(it.list.tail, 0)
	if it.nd == it.list.head || it.nd == it.lowerNode {
		return nil
	}
	it.decodeKV()
	if it.lower != "" && it.list.cmp(it.lower, it.kv.Key) > 0 {
		it.lowerNode = it.nd
		return nil
	}
	return &it.kv
}

// Next advances to the next key.
func (it *Iterator) Next() *KV {
	it.nd = it.list.getNext(it.nd, 0)
	if it.nd == it.list.tail || it.nd == it.upperNode {
		return nil
	}
	it.decodeKV()
	if it.upper != "" && it.list.cmp(it.upper, it.kv.Key) <= 0 {
		it.upperNode = it.nd
		return nil
	}
	return &it.kv
}

// Prev moves to the previous key.
func (it *Iterator) Prev() *KV {
	it.nd = it.list.getPrev(it.nd, 0)
	if it.nd == it.list.head || it.nd == it.lowerNode {
		return nil
	}
	it.decodeKV()
	if it.lower != "" && it.list.cmp(it.lower, it.kv.Key) > 0 {
		it.lowerNode = it.nd
		return nil
	}
	return &it.kv
}

func (it *Iterator) decodeKV() {
	it.kv.Key = it.nd.keyString(it.list.arena)
	it.kv.Value = it.nd.getValue(it.list.arena)
	it.kv.Vptr = it.nd.vptr
	it.kv.Hlc = it.nd.hlc
}

func (it *Iterator) seekForBaseSplice(key string) (prev, next *node, found bool) {
	level := int(it.list.Height() - 1)

	prev = it.list.head
	for {
		prev, next, found = it.list.findSpliceForLevel(key, level, prev)
		if found {
			if level != 0 {
				prev = it.list.getPrev(next, 0)
			}
			break
		}
		if level == 0 {
			break
		}
		level--
	}

	return prev, next, found
}
