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
