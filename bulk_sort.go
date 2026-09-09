package yogadb

const bulkRadixInsertionCutoff = 8

func sortBulkOrderByKey(order []int, kvs []KV) {
	if len(order) < 2 {
		return
	}
	aux := make([]int, len(order))
	sortBulkOrderByKeyMSD(order, aux, kvs, 0)
}

func sortBulkOrderByKeyMSD(order, aux []int, kvs []KV, depth int) {
	if len(order) <= bulkRadixInsertionCutoff {
		insertionSortBulkOrder(order, kvs)
		return
	}

	var count [258]int
	for _, idx := range order {
		count[bulkKeyByte(kvs[idx].Key, depth)+1]++
	}
	for i := 0; i < 257; i++ {
		count[i+1] += count[i]
	}
	start := count
	for _, idx := range order {
		c := bulkKeyByte(kvs[idx].Key, depth)
		aux[count[c]] = idx
		count[c]++
	}
	copy(order, aux[:len(order)])

	for c := 1; c < 257; c++ {
		lo := start[c]
		hi := count[c]
		if hi-lo > 1 {
			sortBulkOrderByKeyMSD(order[lo:hi], aux[lo:hi], kvs, depth+1)
		}
	}
}

func bulkKeyByte(key string, depth int) int {
	if depth >= len(key) {
		return 0
	}
	return int(key[depth]) + 1
}

func insertionSortBulkOrder(order []int, kvs []KV) {
	for i := 1; i < len(order); i++ {
		v := order[i]
		vk := kvs[v].Key
		j := i - 1
		for j >= 0 && vk < kvs[order[j]].Key {
			order[j+1] = order[j]
			j--
		}
		order[j+1] = v
	}
}
