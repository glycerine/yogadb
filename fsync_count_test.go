package yogadb

import "testing"

const (
	fsyncCountBulkTargetBytes = int64(100 << 20)
	fsyncCountKeyLen          = 27
	fsyncCountValLen          = 127
	fsyncCountBatchSize       = 1024
)

func TestAllowReadsFsyncCountAfter100MBBulkLoad(t *testing.T) {
	db, err := OpenFlexDB(t.TempDir(), &Config{
		OmitMemWalFsync:        true,
		DisableBackgroundFlush: true,
	})
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}
	defer db.Close()

	val := fsyncCountValue(fsyncCountValLen)
	keyBuf := make([]byte, fsyncCountKeyLen)
	var projectedBytes int64
	var count uint64

	batch := db.NewBatch()
	defer batch.Close()
	for projectedBytes < fsyncCountBulkTargetBytes {
		for i := 0; i < fsyncCountBatchSize; i++ {
			key := string(fsyncCountHexKeyBuf(keyBuf, count, fsyncCountKeyLen))
			if err := batch.Set(key, val, 0); err != nil {
				t.Fatalf("Set(%q): %v", key, err)
			}
			count++
		}
		if _, err := batch.Commit(false); err != nil {
			t.Fatalf("bulk Commit: %v", err)
		}
		batch.Reset()
		projectedBytes += fsyncCountEstimatedDiskBytes(fsyncCountBatchSize, fsyncCountKeyLen, fsyncCountValLen)
	}

	metricsBatch := db.NewBatch()
	_, before, err := metricsBatch.CommitGetMetrics(false)
	metricsBatch.Close()
	if err != nil {
		t.Fatalf("pre-AllowReads CommitGetMetrics: %v", err)
	}
	if before.TotalFsyncs != 0 {
		t.Fatalf("pre-AllowReads TotalFsyncs = %d, want 0; metrics: %s", before.TotalFsyncs, before)
	}

	db.AllowReads()

	metricsBatch = db.NewBatch()
	_, m, err := metricsBatch.CommitGetMetrics(false)
	metricsBatch.Close()
	if err != nil {
		t.Fatalf("post-AllowReads CommitGetMetrics: %v", err)
	}

	if m.KV128Fsyncs != 1 {
		t.Fatalf("KV128Fsyncs = %d, want 1; metrics: %s", m.KV128Fsyncs, m)
	}
	if m.REDOLogFsyncs != 1 {
		t.Fatalf("REDOLogFsyncs = %d, want 1; metrics: %s", m.REDOLogFsyncs, m)
	}
	if m.VLOGFsyncs != 1 {
		t.Fatalf("VLOGFsyncs = %d, want 1; metrics: %s", m.VLOGFsyncs, m)
	}
	if m.MemWALFsyncs != 0 {
		t.Fatalf("MemWALFsyncs = %d, want 0; metrics: %s", m.MemWALFsyncs, m)
	}
	if m.FlexTreePagesFsyncs != 1 {
		t.Fatalf("FlexTreePagesFsyncs = %d, want 1; metrics: %s", m.FlexTreePagesFsyncs, m)
	}
	if m.FlexTreeCommitFsyncs != 1 {
		t.Fatalf("FlexTreeCommitFsyncs = %d, want 1; metrics: %s", m.FlexTreeCommitFsyncs, m)
	}
	if m.TotalFsyncs != 5 {
		t.Fatalf("TotalFsyncs = %d, want 5; metrics: %s", m.TotalFsyncs, m)
	}
	if got := uint64(db.Len()); got != count {
		t.Fatalf("Len() = %d, want %d", got, count)
	}
}

func fsyncCountHexKeyBuf(buf []byte, n uint64, klen int) []byte {
	const hextable = "0123456789abcdef"
	if len(buf) < klen {
		panic("fsyncCountHexKeyBuf: buffer too short")
	}
	for i := 0; i < 8; i++ {
		b := byte(n >> (56 - uint(i)*8))
		buf[i*2] = hextable[b>>4]
		buf[i*2+1] = hextable[b&0x0f]
	}
	for i := 16; i < klen; i++ {
		buf[i] = '!'
	}
	return buf[:klen]
}

func fsyncCountValue(vlen int) []byte {
	v := make([]byte, vlen)
	for i := range v {
		v[i] = byte('A' + (i % 26))
	}
	return v
}

func fsyncCountEstimatedDiskBytes(n int64, klen, vlen int) int64 {
	valueBytes := vlen
	vlogBytes := int64(0)
	if vlen > vlogInlineThreshold {
		valueBytes = vptrSize
		vlogBytes = n * int64(vlogEntryHeaderSize+vlen)
	}

	entryBytes := int64(4 + 1 + klen + valueBytes)
	pagePayload := int64(slottedPageMaxSize - slottedPageHeaderSize - slottedPageCRCSize)
	keysPerPage := pagePayload / entryBytes
	if keysPerPage < 1 {
		keysPerPage = 1
	}
	pages := (n + keysPerPage - 1) / keysPerPage
	kvBytes := n*entryBytes + pages*int64(slottedPageHeaderSize+slottedPageCRCSize)
	return vlogBytes + kvBytes
}
