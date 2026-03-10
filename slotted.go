package yogadb

// slotted.go: Slotted page interval encoding for FlexSpace.
//
// A slotted page stores multiple KV entries in a bidirectional layout:
// entry records packed forward, values packed backward, with free space
// between them for O(1) appends. Entry records include the key and
// its length, the HLC timestamp delta from the baseHLC, the length
// of the value, and the type of the value (local (<=64 bytes) or
// VPtr into LARGE.VLOG).
//
// note:
//
// THIS IS THE ONLY OVERWRITTEN IN PLACE ON-DISK DATA STRUCTURE
// IN YOGA DB. It was not in the original paper. We added it
// for update efficiency, after seeing very large Write
// Amplification with the original FlexDB (C) design.
//
// Here is the design strategy:
//
// "Hybrid In-Place Updates"
// FlexSpace mandates out-of-place updates to avoid random
// write penalties. However, on modern NVMe SSDs, random
// writes are much faster than they were on HDDs.
//
// How it works: If YogaDB detects an update where the new
// value's byte size is exactly the same as (or smaller than)
// the old value, we bypass the append-only log and overwrite
// the data in place at the existing physical offset.
//
// Trade-off: This completely eliminates space amplification
// for fixed-size updates, but we must ensure our write-ahead
// log (WAL) is rock solid to maintain crash consistency,
// as we are breaking the pure append-only paradigm.
//
//
//
// On-disk layout:
//
// ┌────────┬──────────────────────────────────────────┬──────┬──────────┬────────┐
// │ Header │ Entry Records →→→                        │free  │ ←← Values│ CRC32C │
// │  12B   │ N × [keyLen + valInfo + hlc delta + key] │space │          │  4B    │
// └────────┴──────────────────────────────────────────┴──────┴──────────┴────────┘
//
// Header (12 bytes):
//   [1] magic       slottedPageMagic (0x00)
//   [1] unsorted    uint8 — number of unsorted appended entries at end
//   [2] count       uint16 LE — total number of KV entries
//   [8] baseHLC     int64 BE — minimum HLC among all entries
//
// Entry Records (packed forward, one per entry):
//   [2] keyLen      uint16 LE — key length in bytes
//   [2] valInfo     uint16 LE — value descriptor:
//     0x0000       = tombstone (0 value bytes)
//     0xFFFF       = VPtr (16 value bytes: 8B offset + 8B length)
//     1..0xFFFE   = inline value length (valInfo - 1)
//   [varint] HLC delta from baseHLC (uvarint, 1-10 bytes)
//   [keyLen] key bytes
//
// Values region: values packed contiguously from the back (before CRC).
//   Value[0] is closest to CRC, value[N-1] is closest to the entries.
//   Value[i] offset = totalSize - 4 - sum(valBytes[0..i])
//
// CRC32C (4 bytes): covers bytes [0 .. totalSize-4).
//
// The interleaved entry record layout (slot+HLC+key together) enables
// O(1) appends: extend forward with a new entry record, extend backward
// with a new value. No data movement for existing entries.

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"
)

const (
	// SLOTTED_PAGE_KB is the target page size for slotted page intervals.
	// Tune this for different workloads. Larger pages amortize overhead
	// better but increase rewrite cost.
	SLOTTED_PAGE_KB = 10 // 10:5.214 8:5.84 12:7.874 // up from 2 to 10 seems to help. 10: (9.984, 10.01, 9.817 ns/key); 2: (23.72 ns/key); 20:14.84

	slottedPageMaxSize = SLOTTED_PAGE_KB * 1024 // 65536 bytes

	// MAX_KEY_BYTES is the maximum key size in bytes that a slotted page
	// can hold. Derived from SLOTTED_PAGE_KB to ensure at least one key
	// always fits in a page.
	MAX_KEY_BYTES = SLOTTED_PAGE_KB * 512 // 32768 bytes

	// slottedPageMagic is 0x00. This is unambiguous because kv128 entries
	// start with varint(keyLen) where keyLen >= 1, so their first byte is
	// always >= 0x01. A leading 0x00 byte can therefore ONLY be a slotted page.
	slottedPageMagic = 0x00

	slottedPageHeaderSize = 12 // 1 magic + 1 unsorted + 2 count + 8 baseHLC
	slottedPageCRCSize    = 4

	// valInfo sentinels
	slottedValInfoTombstone = 0x0000
	slottedValInfoVPtr      = 0xFFFF
)

// slottedPageEncode encodes a sorted slice of KVs into a slotted page.
// Returns the encoded page bytes. The input kvs must be sorted by key.
func slottedPageEncode(kvs []KV) []byte {
	return slottedPageEncodeInto(kvs, 0, 0)
}

// slottedPageEncodeWithUnsorted encodes KVs with the given unsorted count in the header.
func slottedPageEncodeWithUnsorted(kvs []KV, unsorted uint8) []byte {
	return slottedPageEncodeInto(kvs, unsorted, 0)
}

// slottedPageEncodePadded encodes KVs into a fixed-size page of targetSize bytes.
// The page is zero-padded between the entry records and the values region.
// Panics if the encoded content exceeds targetSize.
func slottedPageEncodePadded(kvs []KV, targetSize int) []byte {
	return slottedPageEncodeInto(kvs, 0, targetSize)
}

// slottedPageEncodeInto is the common encoder. If targetSize > 0, the output is
// padded to exactly targetSize bytes. Otherwise, output is tight (no padding).
func slottedPageEncodeInto(kvs []KV, unsorted uint8, targetSize int) []byte {
	count := len(kvs)
	if count == 0 {
		if targetSize > 0 {
			// Empty padded page: header + CRC, zero-padded.
			buf := make([]byte, targetSize)
			buf[0] = slottedPageMagic
			// count=0, unsorted=0, baseHLC=0 — all zero is fine
			crcOff := targetSize - slottedPageCRCSize
			checksum := crc32.Checksum(buf[:crcOff], crc32cTable)
			binary.LittleEndian.PutUint32(buf[crcOff:], checksum)
			return buf
		}
		return nil
	}
	if count > 0xFFFF {
		panic(fmt.Sprintf("slottedPageEncode: too many KVs: %d", count))
	}

	// Find baseHLC (minimum).
	baseHLC := kvs[0].Hlc
	for i := 1; i < count; i++ {
		if kvs[i].Hlc < baseHLC {
			baseHLC = kvs[i].Hlc
		}
	}

	// Compute entry record sizes and total values size.
	// Each entry record = 2B keyLen + 2B valInfo + varint(HLC delta) + keyLen bytes
	var hlcBuf [binary.MaxVarintLen64]byte
	totalEntriesSize := 0
	totalValsSize := 0
	for i := 0; i < count; i++ {
		delta := uint64(kvs[i].Hlc - baseHLC)
		varintLen := binary.PutUvarint(hlcBuf[:], delta)
		totalEntriesSize += 4 + varintLen + len(kvs[i].Key) // 4 = 2B keyLen + 2B valInfo
		totalValsSize += slottedValBytes(kvs[i])
	}

	contentSize := slottedPageHeaderSize +
		totalEntriesSize +
		totalValsSize +
		slottedPageCRCSize

	totalSize := contentSize
	if targetSize > 0 {
		if contentSize > targetSize {
			// Content exceeds target — can happen transiently during flush
			// when HLC deltas are mixed (some entries updated, others not).
			// Encode tight (no padding); caller must handle the size mismatch.
			targetSize = 0
		} else {
			totalSize = targetSize
		}
	}

	buf := make([]byte, totalSize)

	// --- Header ---
	buf[0] = slottedPageMagic
	buf[1] = unsorted
	binary.LittleEndian.PutUint16(buf[2:4], uint16(count))
	binary.BigEndian.PutUint64(buf[4:12], uint64(baseHLC))

	// --- Entry Records (forward) ---
	entryOff := slottedPageHeaderSize
	for i := 0; i < count; i++ {
		binary.LittleEndian.PutUint16(buf[entryOff:entryOff+2], uint16(len(kvs[i].Key)))
		binary.LittleEndian.PutUint16(buf[entryOff+2:entryOff+4], slottedValInfo(kvs[i]))
		entryOff += 4
		delta := uint64(kvs[i].Hlc - baseHLC)
		n := binary.PutUvarint(buf[entryOff:], delta)
		entryOff += n
		copy(buf[entryOff:], kvs[i].Key)
		entryOff += len(kvs[i].Key)
	}

	// --- Values Region (packed backward from CRC) ---
	// Value[0] is closest to CRC end, value[N-1] is farthest.
	valEnd := totalSize - slottedPageCRCSize // points past last value byte
	for i := 0; i < count; i++ {
		vb := slottedValBytesOf(kvs[i])
		valStart := valEnd - len(vb)
		copy(buf[valStart:], vb)
		valEnd = valStart
	}

	// --- CRC32C ---
	crcOff := totalSize - slottedPageCRCSize
	checksum := crc32.Checksum(buf[:crcOff], crc32cTable)
	binary.LittleEndian.PutUint32(buf[crcOff:], checksum)

	return buf
}

// slottedPageDecode decodes a slotted page into a slice of KVs.
// Returns the decoded KVs and the total bytes consumed, or an error.
func slottedPageDecode(src []byte) ([]KV, int, error) {
	if len(src) < slottedPageHeaderSize+slottedPageCRCSize {
		return nil, 0, fmt.Errorf("slotted page: too short (%d bytes)", len(src))
	}
	if src[0] != slottedPageMagic {
		return nil, 0, fmt.Errorf("slotted page: bad magic 0x%02x", src[0])
	}

	count := int(binary.LittleEndian.Uint16(src[2:4]))
	baseHLC := HLC(binary.BigEndian.Uint64(src[4:12]))

	if count == 0 {
		// For padded pages, consume the entire buffer.
		consumed := slottedPageHeaderSize + slottedPageCRCSize
		if len(src) > consumed {
			consumed = len(src)
		}
		return nil, consumed, nil
	}

	// Scan entry records to extract keys, keyLens, valInfos, and HLC deltas.
	type entryMeta struct {
		keyLen  uint16
		valInfo uint16
		hlc     HLC
		keyOff  int // offset of key bytes within src
	}
	entries := make([]entryMeta, count)

	off := slottedPageHeaderSize
	totalValsSize := 0
	for i := 0; i < count; i++ {
		if off+4 > len(src) {
			return nil, 0, fmt.Errorf("slotted page: truncated entry record %d at offset %d", i, off)
		}
		entries[i].keyLen = binary.LittleEndian.Uint16(src[off : off+2])
		entries[i].valInfo = binary.LittleEndian.Uint16(src[off+2 : off+4])
		off += 4

		delta, n := binary.Uvarint(src[off:])
		if n <= 0 {
			return nil, 0, fmt.Errorf("slotted page: bad HLC delta at entry %d", i)
		}
		entries[i].hlc = baseHLC + HLC(delta)
		off += n

		kl := int(entries[i].keyLen)
		if off+kl > len(src) {
			return nil, 0, fmt.Errorf("slotted page: truncated key at entry %d", i)
		}
		entries[i].keyOff = off
		off += kl

		totalValsSize += slottedValInfoToLen(entries[i].valInfo)
	}

	// Total content size = entries region end + values + CRC (no padding).
	entriesEnd := off
	tightSize := entriesEnd + totalValsSize + slottedPageCRCSize

	if len(src) < tightSize {
		return nil, 0, fmt.Errorf("slotted page: truncated (%d < %d)", len(src), tightSize)
	}

	// Verify CRC. Try tight layout first; if that fails and the buffer is
	// larger (padded page), try CRC at the end of the full buffer.
	totalSize := tightSize
	crcOff := totalSize - slottedPageCRCSize
	storedCRC := binary.LittleEndian.Uint32(src[crcOff : crcOff+4])
	computedCRC := crc32.Checksum(src[:crcOff], crc32cTable)
	if storedCRC != computedCRC {
		// Try padded layout: CRC and values at end of full buffer.
		if len(src) > tightSize {
			paddedSize := len(src)
			paddedCRCOff := paddedSize - slottedPageCRCSize
			paddedStoredCRC := binary.LittleEndian.Uint32(src[paddedCRCOff : paddedCRCOff+4])
			paddedComputedCRC := crc32.Checksum(src[:paddedCRCOff], crc32cTable)
			if paddedStoredCRC == paddedComputedCRC {
				totalSize = paddedSize
				crcOff = paddedCRCOff
			} else {
				return nil, 0, fmt.Errorf("slotted page: CRC mismatch (stored %08x, computed %08x)", storedCRC, computedCRC)
			}
		} else {
			return nil, 0, fmt.Errorf("slotted page: CRC mismatch (stored %08x, computed %08x)", storedCRC, computedCRC)
		}
	}

	// Build KV slice.
	kvs := make([]KV, count)
	for i := 0; i < count; i++ {
		kl := int(entries[i].keyLen)
		kvs[i].Key = string(src[entries[i].keyOff : entries[i].keyOff+kl])
		kvs[i].Hlc = entries[i].hlc
	}

	// Decode values (packed backward from CRC).
	valEnd := crcOff
	for i := 0; i < count; i++ {
		vl := slottedValInfoToLen(entries[i].valInfo)
		valStart := valEnd - vl
		if entries[i].valInfo == slottedValInfoTombstone {
			// tombstone: no value bytes, Value stays nil
		} else if entries[i].valInfo == slottedValInfoVPtr {
			kvs[i].Vptr = decodeVPtr(src[valStart:valEnd])
		} else {
			kvs[i].Value = make([]byte, vl)
			copy(kvs[i].Value, src[valStart:valEnd])
		}
		valEnd = valStart
	}

	return kvs, totalSize, nil
}

// slottedPageIsSlotted checks if the data at src starts with a slotted page.
func slottedPageIsSlotted(src []byte) bool {
	return len(src) >= 1 && src[0] == slottedPageMagic
}

// slottedValInfo returns the valInfo uint16 for a KV entry.
func slottedValInfo(kv KV) uint16 {
	if kv.isTombstone() {
		return slottedValInfoTombstone
	}
	if kv.HasVPtr() {
		return slottedValInfoVPtr
	}
	return uint16(len(kv.Value) + 1)
}

// slottedValBytes returns the number of value bytes stored for a KV.
func slottedValBytes(kv KV) int {
	if kv.isTombstone() {
		return 0
	}
	if kv.HasVPtr() {
		return vptrSize
	}
	return len(kv.Value)
}

// slottedValBytesOf returns the raw value bytes to store for a KV.
func slottedValBytesOf(kv KV) []byte {
	if kv.isTombstone() {
		return nil
	}
	if kv.HasVPtr() {
		var buf [vptrSize]byte
		kv.Vptr.encode(buf[:])
		return buf[:]
	}
	return kv.Value
}

// slottedPageFirstKey extracts the first key from a slotted page without
// fully decoding it. Used by recovery to reconstruct anchor keys.
func slottedPageFirstKey(src []byte) (string, bool) {
	// Need at least header + first entry's 4B slot header + 1B varint
	if len(src) < slottedPageHeaderSize+5 {
		return "", false
	}
	if src[0] != slottedPageMagic {
		return "", false
	}
	count := int(binary.LittleEndian.Uint16(src[2:4]))
	if count == 0 {
		return "", false
	}
	// First entry record starts at offset 12 (header size).
	off := slottedPageHeaderSize
	keyLen := int(binary.LittleEndian.Uint16(src[off : off+2]))
	off += 4 // skip keyLen + valInfo
	// Skip HLC delta varint.
	_, n := binary.Uvarint(src[off:])
	if n <= 0 {
		return "", false
	}
	off += n
	if off+keyLen > len(src) {
		return "", false
	}
	return string(src[off : off+keyLen]), true
}

// slottedValInfoToLen returns the number of value bytes for a valInfo.
func slottedValInfoToLen(vi uint16) int {
	if vi == slottedValInfoTombstone {
		return 0
	}
	if vi == slottedValInfoVPtr {
		return vptrSize
	}
	return int(vi - 1)
}

// slottedPageUnsorted returns the unsorted count from a slotted page header.
func slottedPageUnsorted(src []byte) uint8 {
	if len(src) < slottedPageHeaderSize {
		return 0
	}
	return src[1]
}

// slottedPageComputeSize returns the encoded size of a slotted page for the given KVs.
func slottedPageComputeSize(kvs []KV) int {
	if len(kvs) == 0 {
		return 0
	}
	var hlcBuf [binary.MaxVarintLen64]byte
	baseHLC := kvs[0].Hlc
	for i := 1; i < len(kvs); i++ {
		if kvs[i].Hlc < baseHLC {
			baseHLC = kvs[i].Hlc
		}
	}
	totalEntriesSize := 0
	totalValsSize := 0
	for i := 0; i < len(kvs); i++ {
		delta := uint64(kvs[i].Hlc - baseHLC)
		varintLen := binary.PutUvarint(hlcBuf[:], delta)
		totalEntriesSize += 4 + varintLen + len(kvs[i].Key)
		totalValsSize += slottedValBytes(kvs[i])
	}
	return slottedPageHeaderSize + totalEntriesSize + totalValsSize + slottedPageCRCSize
}

// slottedPageWouldFit returns true if adding newKV to the existing kvs
// (with one entry possibly replaced at replaceIdx, or -1 for insert) would
// still fit within targetSize bytes.
func slottedPageWouldFit(kvs []KV, count int, newKV KV, replaceIdx int, targetSize int) bool {
	var hlcBuf [binary.MaxVarintLen64]byte

	// Compute baseHLC considering all entries including newKV.
	baseHLC := newKV.Hlc
	for i := 0; i < count; i++ {
		if i == replaceIdx {
			continue // will be replaced by newKV
		}
		if kvs[i].Hlc < baseHLC {
			baseHLC = kvs[i].Hlc
		}
	}

	totalEntriesSize := 0
	totalValsSize := 0

	for i := 0; i < count; i++ {
		kv := kvs[i]
		if i == replaceIdx {
			kv = newKV // use replacement
		}
		delta := uint64(kv.Hlc - baseHLC)
		varintLen := binary.PutUvarint(hlcBuf[:], delta)
		totalEntriesSize += 4 + varintLen + len(kv.Key)
		totalValsSize += slottedValBytes(kv)
	}
	if replaceIdx < 0 {
		// inserting new entry (not replacing)
		delta := uint64(newKV.Hlc - baseHLC)
		varintLen := binary.PutUvarint(hlcBuf[:], delta)
		totalEntriesSize += 4 + varintLen + len(newKV.Key)
		totalValsSize += slottedValBytes(newKV)
	}

	contentSize := slottedPageHeaderSize + totalEntriesSize + totalValsSize + slottedPageCRCSize
	return contentSize <= targetSize
}

// slottedPageWouldFitUniformHLC checks whether the page would fit if all
// entries had the same HLC (i.e., all deltas are 0, using 1-byte varints).
// This detects the case where overflow is caused solely by transient mixed
// HLC deltas during a flush, not by genuine key/value size growth.
func slottedPageWouldFitUniformHLC(kvs []KV, count int, newKV KV, replaceIdx int, targetSize int) bool {
	totalEntriesSize := 0
	totalValsSize := 0
	for i := 0; i < count; i++ {
		kv := kvs[i]
		if i == replaceIdx {
			kv = newKV
		}
		totalEntriesSize += 4 + 1 + len(kv.Key) // 4=header, 1=varint(0)
		totalValsSize += slottedValBytes(kv)
	}
	contentSize := slottedPageHeaderSize + totalEntriesSize + totalValsSize + slottedPageCRCSize
	return contentSize <= targetSize
}

// slottedPageDump returns a human-readable multi-line string describing the
// structure of a raw slotted page buffer. Useful for debugging.
func slottedPageDump(src []byte) string {
	return slottedPageDumpImpl(src, nil)
}

// slottedPageDumpWithVLog is like slottedPageDump but also follows VPtr entries
// and prints the resolved value bytes from the given valueLog (VLOG file).
// If vlog is nil, behaves identically to slottedPageDump.
func slottedPageDumpWithVLog(src []byte, vlog *valueLog) string {
	return slottedPageDumpImpl(src, vlog)
}

func slottedPageDumpImpl(src []byte, vlog *valueLog) string {
	var b strings.Builder
	totalSize := len(src)

	if totalSize < slottedPageHeaderSize+slottedPageCRCSize {
		fmt.Fprintf(&b, "SlottedPage [%dB] ERROR: too short\n", totalSize)
		return b.String()
	}
	if src[0] != slottedPageMagic {
		fmt.Fprintf(&b, "SlottedPage [%dB] ERROR: bad magic 0x%02x\n", totalSize, src[0])
		return b.String()
	}

	unsorted := src[1]
	count := int(binary.LittleEndian.Uint16(src[2:4]))
	baseHLC := HLC(binary.BigEndian.Uint64(src[4:12]))

	// Verify CRC (try tight first, then padded).
	crcStatus := "OK"
	crcOff := -1
	// We need to scan entries to find tight size, but also want CRC status in the header.
	// Do a quick CRC check at the end of the full buffer (works for both padded and tight).
	{
		off := totalSize - slottedPageCRCSize
		stored := binary.LittleEndian.Uint32(src[off : off+4])
		computed := crc32.Checksum(src[:off], crc32cTable)
		if stored == computed {
			crcOff = off
		}
	}
	// If that failed and buffer might be tight, we'll update after scanning entries.

	fmt.Fprintf(&b, "SlottedPage [%dB] count=%d unsorted=%d baseHLC=%d", totalSize, count, unsorted, baseHLC)

	if count == 0 {
		if crcOff >= 0 {
			fmt.Fprintf(&b, " CRC=OK\n")
		} else {
			fmt.Fprintf(&b, " CRC=INVALID\n")
		}
		return b.String()
	}

	// Scan entry records.
	type entryInfo struct {
		keyLen  uint16
		valInfo uint16
		hlc     HLC
		keyOff  int
	}
	entries := make([]entryInfo, 0, count)
	off := slottedPageHeaderSize
	totalValsSize := 0
	scanOK := true
	for i := 0; i < count; i++ {
		if off+4 > totalSize {
			scanOK = false
			break
		}
		var e entryInfo
		e.keyLen = binary.LittleEndian.Uint16(src[off : off+2])
		e.valInfo = binary.LittleEndian.Uint16(src[off+2 : off+4])
		off += 4
		delta, n := binary.Uvarint(src[off:])
		if n <= 0 {
			scanOK = false
			break
		}
		e.hlc = baseHLC + HLC(delta)
		off += n
		if off+int(e.keyLen) > totalSize {
			scanOK = false
			break
		}
		e.keyOff = off
		off += int(e.keyLen)
		totalValsSize += slottedValInfoToLen(e.valInfo)
		entries = append(entries, e)
	}
	entriesEnd := off

	// Now try tight CRC if full-buffer CRC failed.
	if crcOff < 0 && scanOK {
		tightSize := entriesEnd + totalValsSize + slottedPageCRCSize
		if tightSize <= totalSize {
			tOff := tightSize - slottedPageCRCSize
			stored := binary.LittleEndian.Uint32(src[tOff : tOff+4])
			computed := crc32.Checksum(src[:tOff], crc32cTable)
			if stored == computed {
				crcOff = tOff
				crcStatus = "OK"
			}
		}
	}
	if crcOff < 0 {
		crcStatus = "INVALID"
	}

	fmt.Fprintf(&b, " CRC=%s\n", crcStatus)

	// Determine values region start.
	valRegionEnd := totalSize - slottedPageCRCSize
	if crcOff >= 0 {
		valRegionEnd = crcOff
	}
	valRegionStart := valRegionEnd - totalValsSize

	// Entry table.
	entriesSize := entriesEnd - slottedPageHeaderSize
	fmt.Fprintf(&b, "  Entries (%dB from offset %d):\n", entriesSize, slottedPageHeaderSize)
	for i, e := range entries {
		key := string(src[e.keyOff : e.keyOff+int(e.keyLen)])
		if len(key) > 40 {
			key = key[:37] + "..."
		}
		delta := e.hlc - baseHLC

		var valDesc string
		switch {
		case e.valInfo == slottedValInfoTombstone:
			valDesc = "tombstone"
		case e.valInfo == slottedValInfoVPtr:
			// Try to decode the VPtr if we have the values region.
			valDesc = "vptr"
			// We'd need to compute this entry's value offset to show details,
			// so do a forward scan of value sizes.
		default:
			valDesc = fmt.Sprintf("val=%dB", slottedValInfoToLen(e.valInfo))
		}

		// For vptr entries, try to show offset/length and optionally resolve value.
		if e.valInfo == slottedValInfoVPtr && scanOK {
			// Compute this entry's value position by summing preceding value sizes.
			vEnd := valRegionEnd
			for j := 0; j <= i; j++ {
				vEnd -= slottedValInfoToLen(entries[j].valInfo)
			}
			vStart := vEnd // vEnd is now start of entry i's value
			vLen := slottedValInfoToLen(e.valInfo)
			if vStart >= 0 && vStart+vLen <= totalSize {
				vp := decodeVPtr(src[vStart : vStart+vLen])
				valDesc = fmt.Sprintf("vptr(off=%d,len=%d)", vp.Offset, vp.Length)
				if vlog != nil {
					data, err := vlog.read(vp)
					if err != nil {
						valDesc += fmt.Sprintf(" ERR:%s", err)
					} else {
						preview := string(data)
						if len(preview) > 60 {
							preview = preview[:57] + "..."
						}
						valDesc += fmt.Sprintf(" => %dB %q", len(data), preview)
					}
				}
			}
		}

		fmt.Fprintf(&b, "    [%02d] key=%-42q %s  hlc=%d (+%d)\n", i, key, valDesc, e.hlc, delta)
	}
	if !scanOK {
		fmt.Fprintf(&b, "    ... (truncated, scan error)\n")
	}

	// Space summary.
	freeSpace := valRegionStart - entriesEnd
	if freeSpace < 0 {
		freeSpace = 0
	}
	utilPct := 100.0 * float64(totalSize-freeSpace) / float64(totalSize)
	fmt.Fprintf(&b, "  Values (%dB before CRC at offset %d):\n", totalValsSize, valRegionEnd)
	fmt.Fprintf(&b, "  Free: %dB (%.1f%% utilization of %dB)\n", freeSpace, utilPct, totalSize)

	return b.String()
}
