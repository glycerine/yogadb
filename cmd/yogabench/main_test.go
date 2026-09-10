package main

import "testing"

func TestParseCommonFlagsDefaultGBPreservesDefaultCount(t *testing.T) {
	cf, _ := parseCommonFlags(nil)
	if cf.GB != defaultDatasetGB {
		t.Fatalf("GB = %v, want %v", cf.GB, defaultDatasetGB)
	}
	if cf.Count != datasets["udb"].FillOps {
		t.Fatalf("Count = %d, want default FillOps %d", cf.Count, datasets["udb"].FillOps)
	}
}

func TestParseCommonFlagsGBScalesDatasetCount(t *testing.T) {
	cf, _ := parseCommonFlags([]string{"-dataset", "udb", "-gb", "1"})
	want := int64(840_000)
	if cf.Count != want {
		t.Fatalf("Count = %d, want %d", cf.Count, want)
	}
}

func TestParseCommonFlagsCountOverridesGB(t *testing.T) {
	cf, _ := parseCommonFlags([]string{"-dataset", "udb", "-gb", "1", "-count", "1234"})
	if cf.Count != 1234 {
		t.Fatalf("Count = %d, want explicit -count override", cf.Count)
	}
}
