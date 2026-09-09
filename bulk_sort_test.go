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
	var builder bulkIngestBuilder
	builder.appendBatch(kvs)
	order := builder.buildOrder()
	got := make([]string, len(order))
	for i, ref := range order {
		got[i] = builder.kv(ref).Key
	}
	want := []string{"", "a", "aa", "ab", "aba", "b", "z"}
	if !slices.Equal(got, want) {
		t.Fatalf("sorted keys = %#v, want %#v", got, want)
	}
}
