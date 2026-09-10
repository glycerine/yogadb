package main

import "testing"

func BenchmarkYCSBFillOnlyUDRGB1(b *testing.B) {
	cf, _ := parseCommonFlags([]string{
		"-dataset", "udb",
		"-gb", "1",
		"-threads", "1",
		"-duration", "0s",
		"-omit-wal-sync",
	})
	p := cf.Profile
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db, err := openDB(b.TempDir(), cf)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := fillAndAllowReads(db, cf, cf.Threads, p); err != nil {
			b.Fatal(err)
		}
		db.Close()
	}
}

func TestYCSBAllowReadsFsyncCountAfter100MBFill(t *testing.T) {
	const targetBytes = int64(100 << 20)

	cf := &CommonFlags{
		Dataset:         "udb",
		Dir:             t.TempDir(),
		Threads:         1,
		GB:              float64(targetBytes) / float64(bytesPerGiB),
		FillTargetBytes: targetBytes,
		Profile:         datasets["udb"],
		OmitWALSync:     true,
	}

	db, err := openDB(cf.Dir, cf)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := parallelFillUntilProjectedDirSize(db, cf.Dir, cf.FillTargetBytes, cf.Threads, cf.Profile.KeyLen, cf.Profile.ValLen); err != nil {
		t.Fatal(err)
	}
	db.AllowReads()

	metricsBatch := db.NewBatch()
	_, m, err := metricsBatch.CommitGetMetrics(false)
	metricsBatch.Close()
	if err != nil {
		t.Fatal(err)
	}

	if m.KV128Fsyncs != 1 {
		t.Fatalf("KV128Fsyncs = %d, want 1", m.KV128Fsyncs)
	}
	if m.REDOLogFsyncs != 1 {
		t.Fatalf("REDOLogFsyncs = %d, want 1", m.REDOLogFsyncs)
	}
	if m.VLOGFsyncs != 1 {
		t.Fatalf("VLOGFsyncs = %d, want 1", m.VLOGFsyncs)
	}
	if m.MemWALFsyncs != 0 {
		t.Fatalf("MemWALFsyncs = %d, want 0", m.MemWALFsyncs)
	}
	if m.FlexTreePagesFsyncs != 1 {
		t.Fatalf("FlexTreePagesFsyncs = %d, want 1", m.FlexTreePagesFsyncs)
	}
	if m.FlexTreeCommitFsyncs != 1 {
		t.Fatalf("FlexTreeCommitFsyncs = %d, want 1", m.FlexTreeCommitFsyncs)
	}
	if m.TotalFsyncs != 5 {
		t.Fatalf("TotalFsyncs = %d, want 5", m.TotalFsyncs)
	}
}
