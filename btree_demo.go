package yogadb

import (
	//"bytes"

	tbtree "github.com/tidwall/btree"
)

func demo() {
	const degree int = 32
	tree := tbtree.NewMap[string, string](degree)

	var seed [32]byte
	prng := newPRNG(seed)

	key := prng.NewCallID()
	val := prng.NewCallID()
	prev, replaced := tree.Set(key, val)
	if replaced {
		panicf("we should be fresh! instead we replace prev = '%v'", prev)
	}
	val2, found := tree.Get(key)
	if !found {
		panicf("why was just inserted key '%v' not found?", key)
	}
	if val2 != val {
		panicf("value stored under key '%v' changed from '%v' -> '%v'", key, val, val2)
	}
}
