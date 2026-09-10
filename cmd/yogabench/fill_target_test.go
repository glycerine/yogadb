package main

import "testing"

func TestFillAndAllowReadsUsesGBTarget(t *testing.T) {
	const targetBytes = int64(2 << 20)

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

	result, err := fillAndAllowReads(db, cf, cf.Threads, cf.Profile)
	if err != nil {
		t.Fatal(err)
	}
	if result.TotalOps <= 0 {
		t.Fatal("fill did not write any keys")
	}
	if cf.Count != result.TotalOps {
		t.Fatalf("CommonFlags.Count = %d, want filled key count %d", cf.Count, result.TotalOps)
	}

	size, err := dirSizeBytes(cf.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if size < targetBytes {
		t.Fatalf("directory size = %d, want at least target %d", size, targetBytes)
	}

	lastKey := string(hexKeyBuf(make([]byte, cf.Profile.KeyLen), uint64(result.TotalOps-1), cf.Profile.KeyLen))
	got, found, _, _, err := db.Get(lastKey)
	if err != nil {
		t.Fatal(err)
	}
	if !found || len(got) != cf.Profile.ValLen {
		t.Fatalf("Get(lastKey=%q) = len %d found %v, want len %d found true",
			lastKey, len(got), found, cf.Profile.ValLen)
	}
}
