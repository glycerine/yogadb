package yogadb

import (
	"fmt"
	"testing"
	"time"
)

func loadBatchForMergeTest(t *testing.T, db *FlexDB, records ...struct {
	key    string
	value  string
	delete bool
}) {
	t.Helper()
	b := db.NewBatch()
	defer b.Close()
	for _, rec := range records {
		if rec.delete {
			b.Delete(rec.key)
			continue
		}
		if err := b.Set(rec.key, []byte(rec.value), 0); err != nil {
			t.Fatalf("Set(%q): %v", rec.key, err)
		}
	}
	if _, err := b.Commit(false); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func mergeTestFutureSeed(deltaLC int64) HLC {
	base := (PhysicalTime48() + HLC(int64(time.Hour))) & getLC
	return base + HLC(deltaLC)*(getCount+1)
}

func mustPutWithHLCSeed(t *testing.T, db *FlexDB, seed HLC, key, value string) HLC {
	t.Helper()
	db.hlc.ReceiveMessageWithHLC(seed)
	hlc, err := db.Put(key, []byte(value), 0)
	if err != nil {
		t.Fatalf("Put(%q, %q): %v", key, value, err)
	}
	return hlc
}

func mustDeleteWithHLCSeed(t *testing.T, db *FlexDB, seed HLC, key string) HLC {
	t.Helper()
	db.hlc.ReceiveMessageWithHLC(seed)
	if err := db.Delete(key); err != nil {
		t.Fatalf("Delete(%q): %v", key, err)
	}
	return db.hlc.Aload()
}

func TestMergeFromDatabaseHigherHLCAndTombstoneWins(t *testing.T) {
	fs, dstDir := newTestFS(t)
	srcDir := dstDir + "-src"
	cfg := &Config{
		FS:                     fs,
		OmitMemWalFsync:        true,
		DisableBackgroundFlush: true,
	}

	dst, err := OpenFlexDB(dstDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	loadBatchForMergeTest(t, dst,
		struct {
			key    string
			value  string
			delete bool
		}{"a", "dst-a", false},
		struct {
			key    string
			value  string
			delete bool
		}{"b", "dst-b", false},
		struct {
			key    string
			value  string
			delete bool
		}{"c", "dst-c", false},
	)
	if err := dst.Sync(); err != nil {
		t.Fatal(err)
	}
	dst.AllowReads()
	defer dst.Close()

	src, err := OpenFlexDB(srcDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	src.AllowReads()
	mustPut(t, src, "b", "src-b")
	mustPut(t, src, "d", "src-d")
	mustDelete(t, src, "c")
	if err := src.Sync(); err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	stats, err := dst.MergeFrom(src)
	if err != nil {
		t.Fatal(err)
	}
	if stats.SourceKeys == 0 {
		t.Fatalf("MergeFrom stats did not count source keys: %#v", stats)
	}

	mustGet(t, dst, "a", "dst-a")
	mustGet(t, dst, "b", "src-b")
	mustNotGet(t, dst, "c")
	mustGet(t, dst, "d", "src-d")
	if got := dst.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3", got)
	}
	mustCheckIntegrity(t, dst)
}

func TestMergeFromEqualHLCDefaultSourceWins(t *testing.T) {
	fs, dstDir := newTestFS(t)
	srcDir := dstDir + "-src"
	cfg := &Config{
		FS:                     fs,
		OmitMemWalFsync:        true,
		DisableBackgroundFlush: true,
	}

	dst, err := OpenFlexDB(dstDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	dst.AllowReads()
	defer dst.Close()

	src, err := OpenFlexDB(srcDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	src.AllowReads()
	defer src.Close()

	seed := mergeTestFutureSeed(1)
	dstHLC := mustPutWithHLCSeed(t, dst, seed, "same", "dest")
	srcHLC := mustPutWithHLCSeed(t, src, seed, "same", "source")
	if dstHLC != srcHLC {
		t.Fatalf("test setup expected equal HLCs: dst=%d src=%d", dstHLC, srcHLC)
	}

	if _, err := dst.MergeFrom(src); err != nil {
		t.Fatal(err)
	}
	mustGet(t, dst, "same", "source")
	mustCheckIntegrity(t, dst)
}

func TestMergeFromEqualHLCTiesToDestinationOption(t *testing.T) {
	fs, dstDir := newTestFS(t)
	srcDir := dstDir + "-src"
	cfg := &Config{
		FS:                     fs,
		OmitMemWalFsync:        true,
		DisableBackgroundFlush: true,
	}

	dst, err := OpenFlexDB(dstDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	dst.AllowReads()
	defer dst.Close()

	src, err := OpenFlexDB(srcDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	src.AllowReads()
	defer src.Close()

	seed := mergeTestFutureSeed(2)
	dstHLC := mustPutWithHLCSeed(t, dst, seed, "same", "dest")
	srcHLC := mustPutWithHLCSeed(t, src, seed, "same", "source")
	if dstHLC != srcHLC {
		t.Fatalf("test setup expected equal HLCs: dst=%d src=%d", dstHLC, srcHLC)
	}

	if _, err := dst.MergeFromWithOptions(src, MergeOptions{TiesToDestination: true}); err != nil {
		t.Fatal(err)
	}
	mustGet(t, dst, "same", "dest")
	mustCheckIntegrity(t, dst)
}

func TestMergeFromTombstoneUsesHLCNotSourceRole(t *testing.T) {
	fs, dstDir := newTestFS(t)
	srcDir := dstDir + "-src"
	cfg := &Config{
		FS:                     fs,
		OmitMemWalFsync:        true,
		DisableBackgroundFlush: true,
	}

	dst, err := OpenFlexDB(dstDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	dst.AllowReads()
	defer dst.Close()

	src, err := OpenFlexDB(srcDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	src.AllowReads()
	defer src.Close()

	oldSeed := mergeTestFutureSeed(3)
	newSeed := mergeTestFutureSeed(4)
	tombHLC := mustDeleteWithHLCSeed(t, src, oldSeed, "same")
	valueHLC := mustPutWithHLCSeed(t, dst, newSeed, "same", "newer-dest")
	if tombHLC >= valueHLC {
		t.Fatalf("test setup expected source tombstone older than destination value: tomb=%d value=%d", tombHLC, valueHLC)
	}
	if _, err := dst.MergeFrom(src); err != nil {
		t.Fatal(err)
	}
	mustGet(t, dst, "same", "newer-dest")

	newerTombSeed := mergeTestFutureSeed(5)
	newerTombHLC := mustDeleteWithHLCSeed(t, src, newerTombSeed, "same")
	if newerTombHLC <= valueHLC {
		t.Fatalf("test setup expected source tombstone newer than destination value: tomb=%d value=%d", newerTombHLC, valueHLC)
	}
	if _, err := dst.MergeFrom(src); err != nil {
		t.Fatal(err)
	}
	mustNotGet(t, dst, "same")
	mustCheckIntegrity(t, dst)
}

func TestMergeFromDatabaseReusesUntouchedDestinationExtents(t *testing.T) {
	fs, dstDir := newTestFS(t)
	srcDir := dstDir + "-src"
	cfg := &Config{
		FS:                     fs,
		OmitMemWalFsync:        true,
		DisableBackgroundFlush: true,
	}

	dst, err := OpenFlexDB(dstDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	b := dst.NewBatch()
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("k%05d", i)
		if err := b.Set(key, []byte("dst-"+key), 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.Commit(false); err != nil {
		t.Fatal(err)
	}
	b.Close()
	if err := dst.Sync(); err != nil {
		t.Fatal(err)
	}
	dst.AllowReads()
	defer dst.Close()

	src, err := OpenFlexDB(srcDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	src.AllowReads()
	mustPut(t, src, "k01000", "src-k01000")
	if err := src.Sync(); err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	stats, err := dst.MergeFrom(src)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ReusedBytes == 0 || stats.ReusedExtents == 0 {
		t.Fatalf("MergeFrom did not reuse untouched destination extents: %#v", stats)
	}
	if stats.RewrittenPages == 0 {
		t.Fatalf("MergeFrom should rewrite the touched destination interval: %#v", stats)
	}
	mustGet(t, dst, "k00000", "dst-k00000")
	mustGet(t, dst, "k01000", "src-k01000")
	mustGet(t, dst, "k01999", "dst-k01999")
	mustCheckIntegrity(t, dst)
}

func TestMergeFromDatabaseCopiesSourceVLOGValues(t *testing.T) {
	fs, dstDir := newTestFS(t)
	srcDir := dstDir + "-src"
	cfg := &Config{
		FS:                     fs,
		OmitMemWalFsync:        true,
		DisableBackgroundFlush: true,
	}

	dst, err := OpenFlexDB(dstDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	dst.AllowReads()
	defer dst.Close()

	src, err := OpenFlexDB(srcDir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	src.AllowReads()
	large := make([]byte, vlogInlineThreshold+77)
	for i := range large {
		large[i] = byte('A' + i%23)
	}
	if _, err := src.Put("large", large, 99); err != nil {
		t.Fatal(err)
	}
	if err := src.Sync(); err != nil {
		t.Fatal(err)
	}

	if _, err := dst.MergeFrom(src); err != nil {
		t.Fatal(err)
	}
	src.Close()

	got, found, vtyp, _, err := dst.Get("large")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("merged large value not found")
	}
	if string(got) != string(large) {
		t.Fatalf("merged large value mismatch: len got %d want %d", len(got), len(large))
	}
	if vtyp != 99 {
		t.Fatalf("merged large value vtyp=%d, want 99", vtyp)
	}
	mustCheckIntegrity(t, dst)
}
