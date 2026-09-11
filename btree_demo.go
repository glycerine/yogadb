package yogadb

func demo() {
	tree := makeKeyStable(0)

	var seed [32]byte
	prng := newPRNG(seed)

	const x = true
	key := prng.NewCallID()
	val := prng.NewCallID()
	kv := KV{Key: key, Value: []byte(val), Vptr: VPtr{Length: uint64(len(val))}}
	prev, replaced := tree.set(kv, x)
	if replaced {
		panicf("we should be fresh! instead we replace prev = '%v'", prev)
	}

	got, found := tree.get(key, x)
	if !found {
		panicf("why was just inserted key '%v' not found?", key)
	}
	if string(got.Value) != val {
		panicf("value stored under key '%v' changed from '%v' -> '%v'", key, val, string(got.Value))
	}
}
