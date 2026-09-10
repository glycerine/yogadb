package main

import (
	"io"
	"testing"
)

func TestParseYMergeArgsDestinationFirstAndTieFlag(t *testing.T) {
	cfg, code := parseYMergeArgs([]string{"-ties-to-dest", "dst.db", "src.db"}, io.Discard)
	if code != 0 {
		t.Fatalf("parseYMergeArgs code=%d, want 0", code)
	}
	if cfg.dstPath != "dst.db" {
		t.Fatalf("dstPath=%q, want dst.db", cfg.dstPath)
	}
	if cfg.srcPath != "src.db" {
		t.Fatalf("srcPath=%q, want src.db", cfg.srcPath)
	}
	if !cfg.tiesToDestination {
		t.Fatal("tiesToDestination=false, want true")
	}
}

func TestParseYMergeArgsRejectsSamePath(t *testing.T) {
	_, code := parseYMergeArgs([]string{"same.db", "same.db"}, io.Discard)
	if code == 0 {
		t.Fatal("parseYMergeArgs accepted identical destination and source")
	}
}
