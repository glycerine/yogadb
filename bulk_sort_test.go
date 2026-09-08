package yogadb

import (
	"slices"
	"testing"
)

func TestSortBulkOrderByKey(t *testing.T) {
	kvs := []KV{
		{Key: "z"},
		{Key: ""},
		{Key: "aa"},
		{Key: "a"},
		{Key: "ab"},
		{Key: "b"},
		{Key: "aba"},
	}
	order := make([]int, len(kvs))
	for i := range order {
		order[i] = i
	}
	sortBulkOrderByKey(order, kvs)
	got := make([]string, len(order))
	for i, idx := range order {
		got[i] = kvs[idx].Key
	}
	want := []string{"", "a", "aa", "ab", "aba", "b", "z"}
	if !slices.Equal(got, want) {
		t.Fatalf("sorted keys = %#v, want %#v", got, want)
	}
}
