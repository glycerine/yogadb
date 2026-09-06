package yogadb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	//"github.com/glycerine/vfs"
)

func TestSlottedPage_RoundTrip_Basic(t *testing.T) {
	kvs := []KV{
		{Key: "alpha", Value: []byte("value-alpha"), Hlc: 100},
		{Key: "bravo", Value: []byte("value-bravo"), Hlc: 105},
		{Key: "charlie", Value: []byte("value-charlie"), Hlc: 110},
	}

	encoded := slottedPageEncode(kvs)
	if encoded == nil {
		t.Fatal("encoded is nil")
	}
	if !slottedPageHasMagic(encoded) {
		t.Fatalf("magic mismatch: first 16 bytes: %x", encoded[:min(len(encoded), 16)])
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
		if decoded[i].Key != kvs[i].Key {
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
	kvs := []KV{
		{Key: "alive", Value: []byte("val"), Hlc: 1},
		{Key: "dead", Vptr: VPtr{Length: tombstoneVPtrLength}, Hlc: 2}, // tombstone
		{Key: "zombie", Value: []byte("z"), Hlc: 3},
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
	kvs := []KV{
		{Key: "small", Value: []byte("inline"), Hlc: 1},
		{Key: "big", Vptr: VPtr{Offset: 12345, Length: 67890}, Hlc: 2},
	}

	encoded := slottedPageEncode(kvs)
	decoded, _, err := slottedPageDecode(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if !decoded[1].HasVPtr() {
		t.Error("VPtr not set on decoded entry")
	}
	if decoded[1].Vptr.Offset != 12345 || decoded[1].Vptr.Length != 67890 {
		t.Errorf("VPtr: got %+v, want {12345, 67890}", decoded[1].Vptr)
	}
}

func TestSlottedPage_RoundTrip_VtypMetadata(t *testing.T) {
	largeVtyp := make([]byte, 8)
	putUint64(largeVtyp, 0x8877665544332211)

	kvs := []KV{
		{Key: "empty", Vptr: VPtr{Offset: 0x11, Length: 0}, Hlc: 1},
		{Key: "small", Value: []byte("inline"), Vptr: VPtr{Offset: 0x22334455, Length: 6}, Hlc: 2},
		{Key: "big", Value: largeVtyp, Vptr: VPtr{Offset: 12345, Length: vlogInlineThreshold + 1}, Hlc: 3},
		{Key: "small-plain", Value: []byte("plain"), Vptr: VPtr{Length: 5}, Hlc: 4},
		{Key: "big-plain", Vptr: VPtr{Offset: 23456, Length: vlogInlineThreshold + 2}, Hlc: 5},
	}

	encoded := slottedPageEncode(kvs)
	decoded, _, err := slottedPageDecode(encoded)
	if err != nil {
		t.Fatal(err)
	}

	for i := range kvs {
		if decoded[i].Key != kvs[i].Key {
			t.Fatalf("decoded[%d].Key = %q, want %q", i, decoded[i].Key, kvs[i].Key)
		}
		if decoded[i].Vptr != kvs[i].Vptr {
			t.Fatalf("decoded[%d].Vptr = %+v, want %+v", i, decoded[i].Vptr, kvs[i].Vptr)
		}
		if !bytes.Equal(decoded[i].Value, kvs[i].Value) {
			t.Fatalf("decoded[%d].Value = %q, want %q", i, decoded[i].Value, kvs[i].Value)
		}
		if decoded[i].Vtyp() != kvs[i].Vtyp() {
			t.Fatalf("decoded[%d].Vtyp() = %#x, want %#x", i, decoded[i].Vtyp(), kvs[i].Vtyp())
		}
	}
}

func TestSlottedPage_InlineVtypOnlyCostsSpaceWhenNonzero(t *testing.T) {
	plain := []KV{{Key: "plain", Value: []byte("x"), Vptr: VPtr{Length: 1}, Hlc: 1}}
	typed := []KV{{Key: "plain", Value: []byte("x"), Vptr: VPtr{Offset: 7, Length: 1}, Hlc: 1}}

	plainSize := len(slottedPageEncode(plain))
	typedSize := len(slottedPageEncode(typed))

	if typedSize != plainSize+8 {
		t.Fatalf("nonzero inline Vtyp size = %d, want plain size %d + 8", typedSize, plainSize)
	}
}

func TestSlottedPage_VPtrVtypStoredOnlyInEntry(t *testing.T) {
	const vtyp uint64 = 0x8877665544332211
	vtypBytes := make([]byte, 8)
	putUint64(vtypBytes, vtyp)
	kv := KV{
		Key:   "big",
		Value: vtypBytes,
		Vptr:  VPtr{Offset: 0x0102030405060708, Length: vlogInlineThreshold + 1},
		Hlc:   1,
	}

	encoded := slottedPageEncode([]KV{kv})
	entryOff := slottedPageHeaderSize
	if keyLen := int(binary.LittleEndian.Uint16(encoded[entryOff:])); keyLen != len(kv.Key) {
		t.Fatalf("keyLen = %d, want %d", keyLen, len(kv.Key))
	}
	valInfo := binary.LittleEndian.Uint16(encoded[entryOff+2:])
	if valInfo != slottedValInfoVPtrWithVtyp {
		t.Fatalf("valInfo = %#x, want %#x", valInfo, slottedValInfoVPtrWithVtyp)
	}
	if got := binary.LittleEndian.Uint64(encoded[entryOff+4:]); got != vtyp {
		t.Fatalf("entry Vtyp = %#x, want %#x; Vtyp must be stored in the entry record", got, vtyp)
	}
	if got := slottedValInfoToLen(valInfo); got != vptrSize {
		t.Fatalf("value bytes for VPtr+Vtyp = %d, want only the %d-byte VPtr", got, vptrSize)
	}

	decoded, _, err := slottedPageDecode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got := decoded[0].Vtyp(); got != vtyp {
		t.Fatalf("decoded Vtyp = %#x, want %#x", got, vtyp)
	}
	if decoded[0].Vptr != kv.Vptr {
		t.Fatalf("decoded Vptr = %+v, want %+v", decoded[0].Vptr, kv.Vptr)
	}
}

func TestSlottedPage_RoundTrip_SameHLC(t *testing.T) {
	// All same HLC -> all deltas are 0 -> minimal HLC region.
	kvs := make([]KV, 100)
	for i := range kvs {
		kvs[i] = KV{
			Key:   fmt.Sprintf("key%04d", i),
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
	kvs := make([]KV, n)
	for i := range kvs {
		kvs[i] = KV{
			Key:   fmt.Sprintf("key%06d", i),
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
		if decoded[i].Key != kvs[i].Key {
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
	kvs := []KV{
		{Key: "key1", Value: []byte("val1"), Hlc: 1},
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
	kvs := []KV{{Key: "k", Value: []byte("v"), Hlc: 1}}
	page := slottedPageEncode(kvs)
	if !slottedPageIsSlotted(page) {
		t.Error("slotted page not detected")
	}
	if !slottedPageHasMagic(page) {
		t.Errorf("magic mismatch: first 16 bytes: %x", page[:min(len(page), 16)])
	}

	// Raw kv128 data must NEVER be mistaken for a slotted page.
	kv128data := kv128Encode(nil, kvs[0])
	if slottedPageIsSlotted(kv128data) {
		t.Errorf("kv128 falsely detected as slotted page")
	}

	// kv128 data with its magic prefix must not be mistaken for a slotted page.
	kv128withMagic := append(kv128ExtentMagic[:], kv128data...)
	if slottedPageIsSlotted(kv128withMagic) {
		t.Errorf("kv128 with magic prefix falsely detected as slotted page")
	}
	if !kv128HasMagic(kv128withMagic) {
		t.Errorf("kv128 with magic prefix not detected by kv128HasMagic")
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
		kvs := make([]KV, s.count)
		for i := range kvs {
			kvs[i] = KV{
				Key:   string(bytes.Repeat([]byte("k"), s.keySize)),
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
	kvs := []KV{
		{Key: "alpha", Value: []byte("value-alpha"), Hlc: 1000},
		{Key: "bravo", Value: []byte("value-bravo"), Hlc: 1002},
		{Key: "charlie", Vptr: VPtr{Length: tombstoneVPtrLength}, Hlc: 1005}, // tombstone
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
	kvs := []KV{
		{Key: "key1", Value: []byte("val1"), Hlc: 1},
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
	kvs := []KV{
		{Key: "small", Value: []byte("inline"), Hlc: 1},
		{Key: "big", Vptr: VPtr{Offset: 12345, Length: 67890}, Hlc: 2},
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
	// hmm.. these are not over 64 bytes!
	//largeVal1 := []byte("this-is-a-large-value-stored-in-vlog-number-one")
	//largeVal2 := []byte("second-large-value-for-vlog-testing-hello-world")
	largeVal1 := []byte("this-is-a-large-value-stored-in-vlog-number-one901234567890123456")
	largeVal2 := []byte("second-large-value-for-vlog-testing-hello-world901234567890123456")

	vp1, err := vlog.append(largeVal1, 100)
	if err != nil {
		t.Fatal(err)
	}
	vp2, err := vlog.append(largeVal2, 200)
	if err != nil {
		t.Fatal(err)
	}

	// Build a slotted page with inline + vptr + tombstone entries.
	kvs := []KV{
		{Key: "alpha", Value: []byte("inline-val"), Hlc: 100},
		{Key: "bravo", Vptr: vp1, Hlc: 150},
		{Key: "charlie", Vptr: VPtr{Length: tombstoneVPtrLength}, Hlc: 175}, // tombstone
		{Key: "delta", Vptr: vp2, Hlc: 200},
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
	needle := fmt.Sprintf("%dB", len(largeVal1))
	if !strings.Contains(outWithVLog, needle) {
		t.Errorf("missing byte count for resolved value 1 (needle == '%v'; haystack='%v'", needle, outWithVLog)
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

	kvs := []KV{
		{Key: "key", Vptr: vp, Hlc: 1},
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

	// Create an empty VLOG - any read will fail.
	vlog, err := openValueLog(vlogPath, fs)
	if err != nil {
		t.Fatal(err)
	}
	defer vlog.close()

	kvs := []KV{
		{Key: "bad", Vptr: VPtr{Offset: 99999, Length: 100}, Hlc: 1},
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
	kvs := []KV{
		{Key: "key", Vptr: VPtr{Offset: 100, Length: 50}, Hlc: 1},
	}
	encoded := slottedPageEncode(kvs)

	out1 := slottedPageDump(encoded)
	out2 := slottedPageDumpWithVLog(encoded, nil)
	if out1 != out2 {
		t.Errorf("nil vlog should produce same output as slottedPageDump\ngot:\n%s\nwant:\n%s", out2, out1)
	}
}

func totalPayload(kvs []KV) int {
	total := 0
	for _, kv := range kvs {
		total += len(kv.Key) + len(kv.Value)
		if kv.HasVPtr() {
			total += vptrSize
		}
	}
	return total
}
