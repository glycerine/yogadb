package yogadb

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	//"github.com/glycerine/vfs"
)

func TestSlottedPage_RoundTrip_Basic(t *testing.T) {
	kvs := []*KV{
		&KV{Key: []byte("alpha"), Value: []byte("value-alpha"), Hlc: 100},
		&KV{Key: []byte("bravo"), Value: []byte("value-bravo"), Hlc: 105},
		&KV{Key: []byte("charlie"), Value: []byte("value-charlie"), Hlc: 110},
	}

	encoded := slottedPageEncode(kvs)
	if encoded == nil {
		t.Fatal("encoded is nil")
	}
	if encoded[0] != slottedPageMagic {
		t.Fatalf("magic byte: got 0x%02x, want 0x00", encoded[0])
	}

	decoded, n, err := slottedPageDecode(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if n != len(encoded) {
		t.Fatalf("consumed %d bytes, want %d", n, len(encoded))
	}
	if len(decoded) != len(kvs) {
		t.Fatalf("decoded %d KVs, want %d", len(decoded), len(kvs))
	}

	for i := range kvs {
		if !bytes.Equal(decoded[i].Key, kvs[i].Key) {
			t.Errorf("kv[%d] key: got %q, want %q", i, decoded[i].Key, kvs[i].Key)
		}
		if !bytes.Equal(decoded[i].Value, kvs[i].Value) {
			t.Errorf("kv[%d] value: got %q, want %q", i, decoded[i].Value, kvs[i].Value)
		}
		if decoded[i].Hlc != kvs[i].Hlc {
			t.Errorf("kv[%d] HLC: got %d, want %d", i, decoded[i].Hlc, kvs[i].Hlc)
		}
	}

	t.Logf("Encoded %d KVs into %d bytes (payload=%d, overhead=%d, %.1f%%)",
		len(kvs), len(encoded),
		totalPayload(kvs), len(encoded)-totalPayload(kvs),
		100*float64(len(encoded)-totalPayload(kvs))/float64(len(encoded)))
}

func TestSlottedPage_RoundTrip_Tombstone(t *testing.T) {
	kvs := []*KV{
		&KV{Key: []byte("alive"), Value: []byte("val"), Hlc: 1},
		&KV{Key: []byte("dead"), Value: nil, Hlc: 2}, // tombstone
		&KV{Key: []byte("zombie"), Value: []byte("z"), Hlc: 3},
	}

	encoded := slottedPageEncode(kvs)
	decoded, _, err := slottedPageDecode(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if decoded[1].Value != nil {
		t.Errorf("tombstone value should be nil, got %q", decoded[1].Value)
	}
	if !decoded[1].isTombstone() {
		t.Error("tombstone not detected")
	}
}

func TestSlottedPage_RoundTrip_VPtr(t *testing.T) {
	kvs := []*KV{
		&KV{Key: []byte("small"), Value: []byte("inline"), Hlc: 1},
		&KV{Key: []byte("big"), HasVPtr: true, Vptr: VPtr{Offset: 12345, Length: 67890}, Hlc: 2},
	}

	encoded := slottedPageEncode(kvs)
	decoded, _, err := slottedPageDecode(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if !decoded[1].HasVPtr {
		t.Error("VPtr not set on decoded entry")
	}
	if decoded[1].Vptr.Offset != 12345 || decoded[1].Vptr.Length != 67890 {
		t.Errorf("VPtr: got %+v, want {12345, 67890}", decoded[1].Vptr)
	}
}

func TestSlottedPage_RoundTrip_SameHLC(t *testing.T) {
	// All same HLC → all deltas are 0 → minimal HLC region.
	kvs := make([]*KV, 100)
	for i := range kvs {
		kvs[i] = &KV{
			Key:   []byte(fmt.Sprintf("key%04d", i)),
			Value: []byte(fmt.Sprintf("val%04d", i)),
			Hlc:   42,
		}
	}

	encoded := slottedPageEncode(kvs)
	decoded, _, err := slottedPageDecode(encoded)
	if err != nil {
		t.Fatal(err)
	}

	for i := range kvs {
		if decoded[i].Hlc != 42 {
			t.Errorf("kv[%d] HLC: got %d, want 42", i, decoded[i].Hlc)
		}
	}

	payload := totalPayload(kvs)
	overhead := len(encoded) - payload
	t.Logf("100 KVs same HLC: %d bytes total, %d payload, %d overhead (%.1f%%)",
		len(encoded), payload, overhead, 100*float64(overhead)/float64(len(encoded)))
}

func TestSlottedPage_RoundTrip_Large(t *testing.T) {
	// Simulate a realistic interval with ~2000 small KVs.
	const n = 2000
	kvs := make([]*KV, n)
	for i := range kvs {
		kvs[i] = &KV{
			Key:   []byte(fmt.Sprintf("key%06d", i)),
			Value: []byte(fmt.Sprintf("v%06d-padding", i)),
			Hlc:   HLC(1000 + i),
		}
	}

	encoded := slottedPageEncode(kvs)
	decoded, consumedBytes, err := slottedPageDecode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if consumedBytes != len(encoded) {
		t.Fatalf("consumed %d, want %d", consumedBytes, len(encoded))
	}
	if len(decoded) != n {
		t.Fatalf("decoded %d, want %d", len(decoded), n)
	}

	for i := range kvs {
		if !bytes.Equal(decoded[i].Key, kvs[i].Key) {
			t.Fatalf("kv[%d] key mismatch", i)
		}
		if !bytes.Equal(decoded[i].Value, kvs[i].Value) {
			t.Fatalf("kv[%d] value mismatch", i)
		}
		if decoded[i].Hlc != kvs[i].Hlc {
			t.Fatalf("kv[%d] HLC mismatch: %d vs %d", i, decoded[i].Hlc, kvs[i].Hlc)
		}
	}

	payload := totalPayload(kvs)
	overhead := len(encoded) - payload
	// Compare to old kv128 encoding.
	oldSize := 0
	for _, kv := range kvs {
		oldSize += kv128EncodedSize(kv)
	}

	t.Logf("%d KVs: slotted=%d bytes (overhead %.1f%%), kv128=%d bytes (overhead %.1f%%), savings=%.1f%%",
		n, len(encoded),
		100*float64(overhead)/float64(len(encoded)),
		oldSize,
		100*float64(oldSize-payload)/float64(oldSize),
		100*float64(oldSize-len(encoded))/float64(oldSize))
}

func TestSlottedPage_CRCCorruption(t *testing.T) {
	kvs := []*KV{
		&KV{Key: []byte("key1"), Value: []byte("val1"), Hlc: 1},
	}
	encoded := slottedPageEncode(kvs)

	// Corrupt a byte in the middle.
	corrupted := make([]byte, len(encoded))
	copy(corrupted, encoded)
	corrupted[len(corrupted)/2] ^= 0x42

	_, _, err := slottedPageDecode(corrupted)
	if err == nil {
		t.Error("expected CRC error on corrupted page")
	}
}

func TestSlottedPage_IsSlotted(t *testing.T) {
	// Slotted page starts with magic byte 0x00.
	kvs := []*KV{&KV{Key: []byte("k"), Value: []byte("v"), Hlc: 1}}
	page := slottedPageEncode(kvs)
	if !slottedPageIsSlotted(page) {
		t.Error("slotted page not detected")
	}
	if page[0] != slottedPageMagic {
		t.Errorf("first byte: got 0x%02x, want 0x%02x", page[0], slottedPageMagic)
	}

	// kv128 entry starts with varint(keyLen >= 1), so first byte >= 0x01.
	// It must NEVER be mistaken for a slotted page.
	kv128data := kv128Encode(nil, kvs[0])
	if slottedPageIsSlotted(kv128data) {
		t.Errorf("kv128 falsely detected as slotted page (first byte 0x%02x)", kv128data[0])
	}

	// Verify the invariant: any kv128-encoded entry with keyLen >= 1
	// has first byte >= 0x01.
	for keyLen := 1; keyLen <= 300; keyLen++ {
		kv := &KV{Key: make([]byte, keyLen), Value: []byte("v"), Hlc: 1}
		enc := kv128Encode(nil, kv)
		if enc[0] == 0x00 {
			t.Fatalf("kv128 with keyLen=%d starts with 0x00 — ambiguous!", keyLen)
		}
	}
}

func TestSlottedPage_Empty(t *testing.T) {
	encoded := slottedPageEncode(nil)
	if encoded != nil {
		t.Errorf("empty encode should return nil, got %d bytes", len(encoded))
	}
}

func TestSlottedPage_OverheadComparison(t *testing.T) {
	// The user's workload: avg 13-byte key + 13-byte value.
	sizes := []struct {
		name    string
		keySize int
		valSize int
		count   int
	}{
		{"26B-avg-16", 13, 13, 16},
		{"26B-avg-100", 13, 13, 100},
		{"26B-avg-1000", 13, 13, 1000},
		{"26B-avg-2000", 13, 13, 2000},
		{"100B-avg-500", 50, 50, 500},
		{"1KB-avg-60", 32, 992, 60},
	}

	for _, s := range sizes {
		kvs := make([]*KV, s.count)
		for i := range kvs {
			kvs[i] = &KV{
				Key:   bytes.Repeat([]byte("k"), s.keySize),
				Value: bytes.Repeat([]byte("v"), s.valSize),
				Hlc:   HLC(1000 + i),
			}
		}

		slottedSize := len(slottedPageEncode(kvs))
		kv128Size := 0
		for _, kv := range kvs {
			kv128Size += kv128EncodedSize(kv)
		}
		payload := s.count * (s.keySize + s.valSize)

		t.Logf("%-20s: slotted=%6d (oh %.1f%%) kv128=%6d (oh %.1f%%) savings=%.1f%%",
			s.name,
			slottedSize, 100*float64(slottedSize-payload)/float64(slottedSize),
			kv128Size, 100*float64(kv128Size-payload)/float64(kv128Size),
			100*float64(kv128Size-slottedSize)/float64(kv128Size))
	}
}

func TestSlottedPage_Dump(t *testing.T) {
	kvs := []*KV{
		&KV{Key: []byte("alpha"), Value: []byte("value-alpha"), Hlc: 1000},
		&KV{Key: []byte("bravo"), Value: []byte("value-bravo"), Hlc: 1002},
		&KV{Key: []byte("charlie"), Value: nil, Hlc: 1005}, // tombstone
	}

	encoded := slottedPageEncode(kvs)
	out := slottedPageDump(encoded)
	t.Logf("slottedPageDump output:\n%s", out)

	// Header line checks.
	if !strings.Contains(out, "SlottedPage [") {
		t.Error("missing SlottedPage header")
	}
	if !strings.Contains(out, "count=3") {
		t.Error("missing count=3")
	}
	if !strings.Contains(out, "baseHLC=1000") {
		t.Error("missing baseHLC=1000")
	}
	if !strings.Contains(out, "CRC=OK") {
		t.Error("missing CRC=OK")
	}

	// Entry lines.
	if !strings.Contains(out, `"alpha"`) {
		t.Error("missing alpha key")
	}
	if !strings.Contains(out, `"bravo"`) {
		t.Error("missing bravo key")
	}
	if !strings.Contains(out, `"charlie"`) {
		t.Error("missing charlie key")
	}
	if !strings.Contains(out, "tombstone") {
		t.Error("missing tombstone marker for charlie")
	}
	if !strings.Contains(out, "val=11B") {
		t.Error("missing val=11B for alpha/bravo")
	}
	// HLC deltas.
	if !strings.Contains(out, "(+0)") {
		t.Error("missing (+0) delta for alpha")
	}
	if !strings.Contains(out, "(+2)") {
		t.Error("missing (+2) delta for bravo")
	}
	if !strings.Contains(out, "(+5)") {
		t.Error("missing (+5) delta for charlie")
	}

	// Space summary.
	if !strings.Contains(out, "Free:") {
		t.Error("missing Free summary line")
	}
	if !strings.Contains(out, "utilization") {
		t.Error("missing utilization in summary")
	}
}

func TestSlottedPage_Dump_Empty(t *testing.T) {
	// Empty padded page.
	encoded := slottedPageEncodePadded(nil, 64)
	out := slottedPageDump(encoded)
	t.Logf("empty dump:\n%s", out)

	if !strings.Contains(out, "count=0") {
		t.Error("missing count=0")
	}
	if !strings.Contains(out, "CRC=OK") {
		t.Error("missing CRC=OK for empty page")
	}
}

func TestSlottedPage_Dump_CorruptedCRC(t *testing.T) {
	kvs := []*KV{
		&KV{Key: []byte("key1"), Value: []byte("val1"), Hlc: 1},
	}
	encoded := slottedPageEncode(kvs)

	corrupted := make([]byte, len(encoded))
	copy(corrupted, encoded)
	corrupted[len(corrupted)/2] ^= 0x42

	out := slottedPageDump(corrupted)
	t.Logf("corrupted dump:\n%s", out)

	if !strings.Contains(out, "CRC=INVALID") {
		t.Error("expected CRC=INVALID for corrupted page")
	}
}

func TestSlottedPage_Dump_VPtrWithoutVLog(t *testing.T) {
	kvs := []*KV{
		&KV{Key: []byte("small"), Value: []byte("inline"), Hlc: 1},
		&KV{Key: []byte("big"), HasVPtr: true, Vptr: VPtr{Offset: 12345, Length: 67890}, Hlc: 2},
	}

	encoded := slottedPageEncode(kvs)
	out := slottedPageDump(encoded)
	t.Logf("vptr dump (no vlog):\n%s", out)

	if !strings.Contains(out, "vptr(off=12345,len=67890)") {
		t.Errorf("missing vptr details in output:\n%s", out)
	}
	// Should NOT contain "=>" since no vlog provided.
	if strings.Contains(out, "=>") {
		t.Error("unexpected '=>' without vlog")
	}
}

func TestSlottedPage_DumpWithVLog(t *testing.T) {
	fs, dir := newTestFS(t)
	vlogPath := filepath.Join(dir, "LARGE.VLOG")

	vlog, err := openValueLog(vlogPath, fs)
	if err != nil {
		t.Fatal(err)
	}
	defer vlog.close()

	// Write two values to the VLOG.
	largeVal1 := []byte("this-is-a-large-value-stored-in-vlog-number-one")
	largeVal2 := []byte("second-large-value-for-vlog-testing-hello-world")

	vp1, err := vlog.append(largeVal1, 100)
	if err != nil {
		t.Fatal(err)
	}
	vp2, err := vlog.append(largeVal2, 200)
	if err != nil {
		t.Fatal(err)
	}

	// Build a slotted page with inline + vptr + tombstone entries.
	kvs := []*KV{
		&KV{Key: []byte("alpha"), Value: []byte("inline-val"), Hlc: 100},
		&KV{Key: []byte("bravo"), HasVPtr: true, Vptr: vp1, Hlc: 150},
		&KV{Key: []byte("charlie"), Value: nil, Hlc: 175}, // tombstone
		&KV{Key: []byte("delta"), HasVPtr: true, Vptr: vp2, Hlc: 200},
	}

	encoded := slottedPageEncode(kvs)

	// --- Without vlog ---
	outNoVLog := slottedPageDump(encoded)
	t.Logf("dump without vlog:\n%s", outNoVLog)

	if strings.Contains(outNoVLog, "=>") {
		t.Error("slottedPageDump (no vlog) should not resolve vptrs")
	}
	if !strings.Contains(outNoVLog, fmt.Sprintf("vptr(off=%d,len=%d)", vp1.Offset, vp1.Length)) {
		t.Error("missing vptr details for bravo")
	}

	// --- With vlog ---
	outWithVLog := slottedPageDumpWithVLog(encoded, vlog)
	t.Logf("dump with vlog:\n%s", outWithVLog)

	// Header.
	if !strings.Contains(outWithVLog, "count=4") {
		t.Error("missing count=4")
	}
	if !strings.Contains(outWithVLog, "CRC=OK") {
		t.Error("missing CRC=OK")
	}

	// Inline entry.
	if !strings.Contains(outWithVLog, "val=10B") {
		t.Error("missing val=10B for alpha")
	}

	// Tombstone.
	if !strings.Contains(outWithVLog, "tombstone") {
		t.Error("missing tombstone for charlie")
	}

	// VPtr entries should show resolved values.
	if !strings.Contains(outWithVLog, "=>") {
		t.Error("slottedPageDumpWithVLog should resolve vptrs with '=>'")
	}
	if !strings.Contains(outWithVLog, string(largeVal1)) {
		t.Errorf("resolved value 1 not in output:\n%s", outWithVLog)
	}
	if !strings.Contains(outWithVLog, string(largeVal2)) {
		t.Errorf("resolved value 2 not in output:\n%s", outWithVLog)
	}
	if !strings.Contains(outWithVLog, fmt.Sprintf("%dB", len(largeVal1))) {
		t.Error("missing byte count for resolved value 1")
	}
}

func TestSlottedPage_DumpWithVLog_LargeValueTruncated(t *testing.T) {
	fs, dir := newTestFS(t)
	vlogPath := filepath.Join(dir, "LARGE.VLOG")

	vlog, err := openValueLog(vlogPath, fs)
	if err != nil {
		t.Fatal(err)
	}
	defer vlog.close()

	// Write a value longer than the 60-char preview limit.
	bigVal := bytes.Repeat([]byte("X"), 200)
	vp, err := vlog.append(bigVal, 1)
	if err != nil {
		t.Fatal(err)
	}

	kvs := []*KV{
		&KV{Key: []byte("key"), HasVPtr: true, Vptr: vp, Hlc: 1},
	}
	encoded := slottedPageEncode(kvs)
	out := slottedPageDumpWithVLog(encoded, vlog)
	t.Logf("truncated value dump:\n%s", out)

	// The preview should be truncated with "..." inside the quoted string.
	if !strings.Contains(out, "...") {
		t.Error("expected '...' truncation for large value preview")
	}
	if !strings.Contains(out, "200B") {
		t.Error("expected full byte count '200B' even when preview is truncated")
	}
}

func TestSlottedPage_DumpWithVLog_BadVPtr(t *testing.T) {
	fs, dir := newTestFS(t)
	vlogPath := filepath.Join(dir, "LARGE.VLOG")

	// Create an empty VLOG — any read will fail.
	vlog, err := openValueLog(vlogPath, fs)
	if err != nil {
		t.Fatal(err)
	}
	defer vlog.close()

	kvs := []*KV{
		&KV{Key: []byte("bad"), HasVPtr: true, Vptr: VPtr{Offset: 99999, Length: 100}, Hlc: 1},
	}
	encoded := slottedPageEncode(kvs)
	out := slottedPageDumpWithVLog(encoded, vlog)
	t.Logf("bad vptr dump:\n%s", out)

	if !strings.Contains(out, "ERR:") {
		t.Error("expected ERR: for bad vptr read")
	}
}

func TestSlottedPage_DumpWithVLog_NilVLog(t *testing.T) {
	// slottedPageDumpWithVLog with nil vlog should behave like slottedPageDump.
	kvs := []*KV{
		&KV{Key: []byte("key"), HasVPtr: true, Vptr: VPtr{Offset: 100, Length: 50}, Hlc: 1},
	}
	encoded := slottedPageEncode(kvs)

	out1 := slottedPageDump(encoded)
	out2 := slottedPageDumpWithVLog(encoded, nil)
	if out1 != out2 {
		t.Errorf("nil vlog should produce same output as slottedPageDump\ngot:\n%s\nwant:\n%s", out2, out1)
	}
}

func totalPayload(kvs []*KV) int {
	total := 0
	for _, kv := range kvs {
		total += len(kv.Key) + len(kv.Value)
		if kv.HasVPtr {
			total += vptrSize
		}
	}
	return total
}
