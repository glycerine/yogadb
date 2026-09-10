package main

import "testing"

func TestParseCommonFlagsDefaultGBPreservesDefaultCount(t *testing.T) {
	cf, _ := parseCommonFlags(nil)
	if cf.GB != defaultDatasetGB {
		t.Fatalf("GB = %v, want %v", cf.GB, defaultDatasetGB)
	}
	if cf.FillTargetBytes != gbToBytes(defaultDatasetGB) {
		t.Fatalf("FillTargetBytes = %d, want default GB target", cf.FillTargetBytes)
	}
	if cf.Count != datasets["udb"].FillOps {
		t.Fatalf("Count = %d, want default FillOps %d", cf.Count, datasets["udb"].FillOps)
	}
	if cf.CountExplicit {
		t.Fatal("CountExplicit = true, want false")
	}
}

func TestParseCommonFlagsGBSetsFillTargetAndScalesFallbackCount(t *testing.T) {
	cf, _ := parseCommonFlags([]string{"-dataset", "udb", "-gb", "1"})
	if cf.FillTargetBytes != bytesPerGiB {
		t.Fatalf("FillTargetBytes = %d, want %d", cf.FillTargetBytes, bytesPerGiB)
	}
	wantCount := int64(840_000)
	if cf.Count != wantCount {
		t.Fatalf("Count = %d, want fallback %d", cf.Count, wantCount)
	}
	if cf.CountExplicit {
		t.Fatal("CountExplicit = true, want false")
	}
}

func TestParseCommonFlagsCountOverridesGB(t *testing.T) {
	cf, _ := parseCommonFlags([]string{"-dataset", "udb", "-gb", "1", "-count", "1234"})
	if cf.Count != 1234 {
		t.Fatalf("Count = %d, want explicit -count override", cf.Count)
	}
	if !cf.CountExplicit {
		t.Fatal("CountExplicit = false, want true")
	}
	if cf.FillTargetBytes != bytesPerGiB {
		t.Fatalf("FillTargetBytes = %d, want %d", cf.FillTargetBytes, bytesPerGiB)
	}
}
