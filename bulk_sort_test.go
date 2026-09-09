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
	builder.appendBatch(kvs, false)
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

func TestSortBulkOrderByFixedWidthKey(t *testing.T) {
	kvs := []KV{
		{Key: "k009"},
		{Key: "k001"},
		{Key: "k010"},
		{Key: "k000"},
		{Key: "k003"},
		{Key: "k002"},
	}
	var builder bulkIngestBuilder
	builder.appendBatch(kvs, false)
	order := builder.buildOrder()
	got := make([]string, len(order))
	for i, ref := range order {
		got[i] = builder.kv(ref).Key
	}
	want := []string{"k000", "k001", "k002", "k003", "k009", "k010"}
	if !slices.Equal(got, want) {
		t.Fatalf("sorted fixed-width keys = %#v, want %#v", got, want)
	}
}

func TestBulkIngestBuildOrderSkipsSortWhenAlreadySorted(t *testing.T) {
	var builder bulkIngestBuilder
	builder.appendBatch([]KV{{Key: "a001"}, {Key: "a002"}}, false)
	builder.appendBatch([]KV{{Key: "a002"}, {Key: "a003"}}, false)
	if !builder.sorted {
		t.Fatal("builder should recognize nondecreasing batch input as sorted")
	}
	order := builder.buildOrder()
	got := make([]string, len(order))
	for i, ref := range order {
		got[i] = builder.kv(ref).Key
	}
	want := []string{"a001", "a002", "a002", "a003"}
	if !slices.Equal(got, want) {
		t.Fatalf("sorted input order = %#v, want %#v", got, want)
	}
}

func TestBulkIngestBuildOrderSortsAfterDisorder(t *testing.T) {
	var builder bulkIngestBuilder
	builder.appendBatch([]KV{{Key: "b"}, {Key: "a"}, {Key: "c"}}, false)
	if builder.sorted {
		t.Fatal("builder should detect out-of-order input")
	}
	order := builder.buildOrder()
	got := make([]string, len(order))
	for i, ref := range order {
		got[i] = builder.kv(ref).Key
	}
	want := []string{"a", "b", "c"}
	if !slices.Equal(got, want) {
		t.Fatalf("disordered input order = %#v, want %#v", got, want)
	}
}

func TestBulkIngestKeysEqual(t *testing.T) {
	if !bulkIngestKeysEqual("same-prefix-a", "same-prefix-a") {
		t.Fatal("identical keys should match")
	}
	if bulkIngestKeysEqual("same-prefix-a", "same-prefix-b") {
		t.Fatal("different last byte should not match")
	}
	if bulkIngestKeysEqual("short", "shorter") {
		t.Fatal("different lengths should not match")
	}
}
