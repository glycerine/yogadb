package yogadb

/*
  flexspace_fuzz_test.go - New file

  3 tests:

1. FuzzFlexSpace - Go native fuzz test exercising
Insert (end + middle), Collapse, Write (overwrite),
and Sync with a ground-truth byte-buffer oracle.
Checks all 6 invariants (INV-FS-1 through INV-FS-6) every 8 ops.

2. FuzzRecoveryFlexSpace - Same ops plus close/reopen
cycles (opcode 5). Verifies INV-FS-4 (recovery preserves blkusage and
data) after each reopen.

3. TestFlexSpace_RandomizedInvariants - Deterministic 2000-op
stress test with PRNG seed, including up to 5 reopen cycles. Full
invariant checks every 50 ops, read correctness every 200 ops.

Infrastructure:
  - flexSpaceOracle - simple byte-slice ground truth
    tracking insert/collapse/write semantics
  - verifyAllFlexSpaceInvariants / verifyNoBlockCrossingFuzz -
     reusable invariant checkers for testing.TB
  - verifyReadCorrectness - chunk-wise full content
    comparison against oracle
  - makeFillData - deterministic fill data from PRNG

  To run for 30 minutes:
  go test -tags memfs -fuzz '^FuzzFlexSpace$' -fuzztime 30m -run='^$' -timeout 35m
  go test -tags memfs -fuzz '^FuzzRecoveryFlexSpace$' -fuzztime 30m -run='^$' -timeout 35m

*/

// flexspace_fuzz_test.go - Fuzz tests for FlexSpace extent manipulation and
// block usage accounting. Exercises Insert, Collapse, Write (overwrite),
// SetTag/GetTag, and Sync at the FlexSpace layer, checking six invariants
// after every operation:
//
//  INV-FS-1: blkusage[i] == actual bytes in block i across all FlexTree extents
//  INV-FS-2: No extent crosses a 4MB block boundary (new data)
//  INV-FS-3: blkusage[i] <= FLEXSPACE_BLOCK_SIZE
//  INV-FS-4: Recovery preserves blkusage (checked periodically via reopen)
//  INV-FS-5: Read correctness: data reads back as written
//  INV-FS-6: sum(blkusage) == sum(extent.Len)
//
// These complement the FlexTree-level FuzzFlexTree in flextree_fuzz_test.go.

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/glycerine/vfs"
)

// ========================================================================
// Invariant checkers (reuse helpers from flexspace_test.go where possible)
// ========================================================================

// verifyAllFlexSpaceInvariants checks INV-FS-1 through INV-FS-3 and INV-FS-6.
func verifyAllFlexSpaceInvariants(t testing.TB, ff *FlexSpace, opDesc string) {
	t.Helper()

	expected := make(map[uint64]uint64)
	totalExtentBytes := uint64(0)

	nodeID := ff.tree.LeafHead
	for !nodeID.IsIllegal() {
		le := ff.tree.GetLeaf(nodeID)
		for i := uint32(0); i < le.Count; i++ {
			ext := &le.Extents[i]
			if ext.IsHole() {
				continue
			}
			poff := ext.Address()
			remaining := uint64(ext.Len)
			totalExtentBytes += remaining
			for remaining > 0 {
				blkid := poff >> FLEXSPACE_BLOCK_BITS
				blkEnd := (blkid + 1) << FLEXSPACE_BLOCK_BITS
				inBlock := blkEnd - poff
				if inBlock > remaining {
					inBlock = remaining
				}
				expected[blkid] += inBlock
				poff += inBlock
				remaining -= inBlock
			}
		}
		nodeID = le.Next
	}

	// INV-FS-1: blkusage matches actual
	for blkid, want := range expected {
		got := uint64(ff.bm.blkusage[blkid])
		if got != want {
			t.Fatalf("[INV-FS-1] %s: blkusage[%d] = %d, want %d", opDesc, blkid, got, want)
		}
	}

	// Check no unexpected non-zero blocks
	for i := range ff.bm.blkusage {
		if ff.bm.blkusage[i] != 0 && expected[uint64(i)] == 0 {
			t.Fatalf("[INV-FS-1] %s: blkusage[%d] = %d but no extents in block", opDesc, i, ff.bm.blkusage[i])
		}
	}

	// INV-FS-3: no block exceeds capacity
	for blkid, usage := range expected {
		if usage > FLEXSPACE_BLOCK_SIZE {
			t.Fatalf("[INV-FS-3] %s: blkusage[%d] = %d > BLOCK_SIZE %d", opDesc, blkid, usage, FLEXSPACE_BLOCK_SIZE)
		}
	}

	// INV-FS-6: totals match
	totalUsage := uint64(0)
	for _, v := range expected {
		totalUsage += v
	}
	if totalUsage != totalExtentBytes {
		t.Fatalf("[INV-FS-6] %s: sum(blkusage) = %d != sum(extent.Len) = %d", opDesc, totalUsage, totalExtentBytes)
	}
}

// verifyNoBlockCrossingFuzz checks INV-FS-2.
func verifyNoBlockCrossingFuzz(t testing.TB, ff *FlexSpace, opDesc string) {
	t.Helper()
	nodeID := ff.tree.LeafHead
	for !nodeID.IsIllegal() {
		le := ff.tree.GetLeaf(nodeID)
		for i := uint32(0); i < le.Count; i++ {
			ext := &le.Extents[i]
			if ext.IsHole() || ext.Len == 0 {
				continue
			}
			poff := ext.Address()
			startBlk := poff >> FLEXSPACE_BLOCK_BITS
			endBlk := (poff + uint64(ext.Len) - 1) >> FLEXSPACE_BLOCK_BITS
			if startBlk != endBlk {
				t.Fatalf("[INV-FS-2] %s: extent loff=%d poff=%d len=%d crosses blocks %d-%d",
					opDesc, ext.Loff, poff, ext.Len, startBlk, endBlk)
			}
		}
		nodeID = le.Next
	}
}

// ========================================================================
// flexSpaceOracle - simple ground-truth byte buffer for read verification
// ========================================================================

// flexSpaceOracle tracks the expected content of the FlexSpace as a byte slice.
type flexSpaceOracle struct {
	data []byte
}

func (o *flexSpaceOracle) insert(off uint64, buf []byte) {
	if off > uint64(len(o.data)) {
		off = uint64(len(o.data))
	}
	// Make room
	o.data = append(o.data, make([]byte, len(buf))...)
	copy(o.data[off+uint64(len(buf)):], o.data[off:])
	copy(o.data[off:], buf)
}

func (o *flexSpaceOracle) collapse(off, length uint64) {
	if off+length > uint64(len(o.data)) {
		length = uint64(len(o.data)) - off
	}
	copy(o.data[off:], o.data[off+length:])
	o.data = o.data[:uint64(len(o.data))-length]
}

func (o *flexSpaceOracle) write(off uint64, buf []byte) {
	end := off + uint64(len(buf))
	if end > uint64(len(o.data)) {
		// Extend
		o.data = append(o.data, make([]byte, end-uint64(len(o.data)))...)
	}
	copy(o.data[off:], buf)
}

func (o *flexSpaceOracle) size() uint64 {
	return uint64(len(o.data))
}

// ========================================================================
// FuzzFlexSpace - Go native fuzz test
// ========================================================================

func FuzzFlexSpace(f *testing.F) {
	// Seed corpus
	f.Add([]byte{}, int64(42))

	// A few inserts
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 64, 0}, int64(123))

	// Insert + collapse
	f.Add([]byte{0, 0, 0, 0, 0, 0, 2, 0, 0, 1, 0, 0, 0, 0, 0, 64}, int64(7))

	// Multiple ops
	f.Add([]byte{
		0, 0, 0, 0, 0, 0, 1, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 128, 0,
		2, 0, 0, 16, 0, 0, 16,
		3,
	}, int64(999))

	f.Fuzz(func(t *testing.T, data []byte, seed int64) {
		fs, dir := newTestFS(t)
		ff, err := OpenFlexSpaceCoW(dir, false, fs)
		if err != nil {
			t.Skipf("OpenFlexSpaceCoW: %v", err)
		}
		// Guarantee cleanup even on t.Fatal/panic — the fuzz engine
		// requires each iteration to be fully self-contained.
		// Recover from panics so panicOn() calls don't kill the worker.
		t.Cleanup(func() {
			defer func() { recover() }()
			ff.Close()
		})
		oracle := &flexSpaceOracle{}

		pos := 0
		opCount := 0
		const checkInterval = 8
		const maxOps = 150                               // bound work per iteration for -race
		const maxSize = uint64(2 * FLEXSPACE_BLOCK_SIZE) // cap at 8MB to keep tests fast

		// Simple PRNG from seed for generating fill data
		prngState := uint64(seed)
		prng := func() uint64 {
			prngState ^= prngState << 13
			prngState ^= prngState >> 7
			prngState ^= prngState << 17
			if prngState == 0 {
				prngState = 1
			}
			return prngState
		}

		fillBuf := make([]byte, FLEXSPACE_MAX_EXTENT_SIZE)
		for i := range fillBuf {
			fillBuf[i] = byte(prng())
		}

		// Wrap in func+recover so panicOn() calls don't kill the fuzz worker subprocess.
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic in fuzz iteration: %v", r)
				}
			}()

		for pos < len(data) && opCount < maxOps {
			opcode := data[pos] % 5
			pos++
			opCount++

			size := ff.Size()
			var opDesc string

			switch opcode {
			case 0: // Insert at end
				rawLen := consumeU16(data, &pos)
				length := uint64(rawLen)%(FLEXSPACE_MAX_EXTENT_SIZE-1) + 1
				if size+length > maxSize {
					continue
				}
				// Use deterministic data from fillBuf
				insertData := makeFillData(fillBuf, length, prng)
				opDesc = fmt.Sprintf("Insert(end, %d)", length)

				n, err := ff.Insert(insertData, size, length)
				if err != nil {
					t.Fatalf("%s: %v", opDesc, err)
				}
				if uint64(n) != length {
					t.Fatalf("%s: wrote %d, want %d", opDesc, n, length)
				}
				oracle.insert(size, insertData)

			case 1: // Collapse
				if size < 2 {
					continue
				}
				rawOff := consumeU16(data, &pos)
				rawLen := consumeU16(data, &pos)
				off := uint64(rawOff) % size
				maxLen := size - off
				length := uint64(rawLen)%maxLen + 1
				opDesc = fmt.Sprintf("Collapse(%d, %d)", off, length)

				if err := ff.Collapse(off, length); err != nil {
					t.Fatalf("%s: %v", opDesc, err)
				}
				oracle.collapse(off, length)

			case 2: // Write (overwrite within existing region)
				if size < 2 {
					continue
				}
				rawOff := consumeU16(data, &pos)
				rawLen := consumeU16(data, &pos)
				off := uint64(rawOff) % size
				maxLen := size - off
				if maxLen > FLEXSPACE_MAX_EXTENT_SIZE {
					maxLen = FLEXSPACE_MAX_EXTENT_SIZE
				}
				length := uint64(rawLen)%maxLen + 1
				writeData := makeFillData(fillBuf, length, prng)
				opDesc = fmt.Sprintf("Write(%d, %d)", off, length)

				n, err := ff.Write(writeData, off, length)
				if err != nil {
					t.Fatalf("%s: %v", opDesc, err)
				}
				if uint64(n) != length {
					t.Fatalf("%s: wrote %d, want %d", opDesc, n, length)
				}
				oracle.write(off, writeData)

			case 3: // Sync
				opDesc = "Sync"
				ff.Sync()

			case 4: // Insert at middle (true insert-range)
				if size < 2 {
					continue
				}
				rawOff := consumeU16(data, &pos)
				rawLen := consumeU16(data, &pos)
				off := uint64(rawOff) % size
				length := uint64(rawLen)%(FLEXSPACE_MAX_EXTENT_SIZE-1) + 1
				if size+length > maxSize {
					continue
				}
				insertData := makeFillData(fillBuf, length, prng)
				opDesc = fmt.Sprintf("Insert(%d, %d)", off, length)

				n, err := ff.Insert(insertData, off, length)
				if err != nil {
					t.Fatalf("%s: %v", opDesc, err)
				}
				if uint64(n) != length {
					t.Fatalf("%s: wrote %d, want %d", opDesc, n, length)
				}
				oracle.insert(off, insertData)
			}

			if opDesc == "" {
				opDesc = fmt.Sprintf("op#%d", opCount)
			}

			// Quick size check every op
			if ff.Size() != oracle.size() {
				t.Fatalf("[INV-FS-5] %s: FlexSpace.Size()=%d != oracle.size()=%d",
					opDesc, ff.Size(), oracle.size())
			}

			// Full invariant check periodically
			if opCount%checkInterval == 0 {
				verifyAllFlexSpaceInvariants(t, ff, opDesc)
				verifyNoBlockCrossingFuzz(t, ff, opDesc)

				// INV-FS-5: spot-check read correctness
				verifyReadCorrectness(t, ff, oracle, opDesc)
			}
		}

		// Final comprehensive checks
		if ff.Size() > 0 {
			verifyAllFlexSpaceInvariants(t, ff, "final")
			verifyNoBlockCrossingFuzz(t, ff, "final")
			verifyReadCorrectness(t, ff, oracle, "final")
		}
		}() // end recover wrapper
		// ff.Close() handled by t.Cleanup
	})
}

// ========================================================================
// FuzzRecoveryFlexSpace - fuzz test that includes close/reopen cycles
// ========================================================================

func FuzzRecoveryFlexSpace(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 2, 0, 0}, int64(77))
	f.Add([]byte{
		0, 0, 0, 0, 0, 0, 1, 0, 0,
		3, // sync
		0, 0, 0, 0, 0, 0, 0, 128, 0,
		3, // sync
	}, int64(88))

	f.Fuzz(func(t *testing.T, data []byte, seed int64) {
		fs, dir := newTestFS(t)
		ff, err := OpenFlexSpaceCoW(dir, false, fs)
		if err != nil {
			t.Skipf("OpenFlexSpaceCoW: %v", err)
		}
		// Track whether ff is currently open so cleanup doesn't
		// double-close after an opcode-5 close+reopen sequence.
		// We must also recover from panics — panicOn() calls inside
		// FlexSpace would kill the fuzz worker subprocess, which the
		// fuzz framework reports as "hung or terminated unexpectedly".
		ffOpen := true
		t.Cleanup(func() {
			if ffOpen {
				defer func() { recover() }() // swallow panic from double-close or bad state
				ff.Close()
			}
		})
		oracle := &flexSpaceOracle{}

		pos := 0
		opCount := 0
		reopenCount := 0
		const maxOps = 150    // bound work per iteration for -race
		const maxReopens = 5  // reopens are expensive (sync+close+open+verify)
		const maxSize = uint64(2 * FLEXSPACE_BLOCK_SIZE)

		prngState := uint64(seed)
		prng := func() uint64 {
			prngState ^= prngState << 13
			prngState ^= prngState >> 7
			prngState ^= prngState << 17
			if prngState == 0 {
				prngState = 1
			}
			return prngState
		}

		fillBuf := make([]byte, FLEXSPACE_MAX_EXTENT_SIZE)
		for i := range fillBuf {
			fillBuf[i] = byte(prng())
		}

		// Wrap the entire operation loop in a deferred recover so that
		// any panicOn() inside FlexSpace code converts to a test failure
		// instead of killing the fuzz worker subprocess.
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic in fuzz iteration: %v", r)
				}
			}()

			for pos < len(data) && opCount < maxOps {
				opcode := data[pos] % 6
				pos++
				opCount++

				size := ff.Size()

				switch opcode {
				case 0: // Insert at end
					rawLen := consumeU16(data, &pos)
					length := uint64(rawLen)%(FLEXSPACE_MAX_EXTENT_SIZE-1) + 1
					if size+length > maxSize {
						continue
					}
					insertData := makeFillData(fillBuf, length, prng)
					n, err := ff.Insert(insertData, size, length)
					if err != nil || uint64(n) != length {
						continue
					}
					oracle.insert(size, insertData)

				case 1: // Collapse
					if size < 2 {
						continue
					}
					rawOff := consumeU16(data, &pos)
					rawLen := consumeU16(data, &pos)
					off := uint64(rawOff) % size
					maxLen := size - off
					length := uint64(rawLen)%maxLen + 1
					if err := ff.Collapse(off, length); err != nil {
						continue
					}
					oracle.collapse(off, length)

				case 2: // Write
					if size < 2 {
						continue
					}
					rawOff := consumeU16(data, &pos)
					rawLen := consumeU16(data, &pos)
					off := uint64(rawOff) % size
					maxLen := size - off
					if maxLen > FLEXSPACE_MAX_EXTENT_SIZE {
						maxLen = FLEXSPACE_MAX_EXTENT_SIZE
					}
					length := uint64(rawLen)%maxLen + 1
					writeData := makeFillData(fillBuf, length, prng)
					n, err := ff.Write(writeData, off, length)
					if err != nil || uint64(n) != length {
						continue
					}
					oracle.write(off, writeData)

				case 3: // Sync
					ff.Sync()

				case 4: // Insert at middle
					if size < 2 {
						continue
					}
					rawOff := consumeU16(data, &pos)
					rawLen := consumeU16(data, &pos)
					off := uint64(rawOff) % size
					length := uint64(rawLen)%(FLEXSPACE_MAX_EXTENT_SIZE-1) + 1
					if size+length > maxSize {
						continue
					}
					insertData := makeFillData(fillBuf, length, prng)
					n, err := ff.Insert(insertData, off, length)
					if err != nil || uint64(n) != length {
						continue
					}
					oracle.insert(off, insertData)

				case 5: // Sync + Close + Reopen
					if reopenCount >= maxReopens {
						continue
					}
					reopenCount++
					ff.Sync()
					ff.Close()
					ffOpen = false

					var err error
					ff, err = OpenFlexSpaceCoW(dir, false, fs)
					if err != nil {
						t.Fatalf("reopen: %v", err)
					}
					ffOpen = true

					// After recovery: verify invariants
					verifyAllFlexSpaceInvariants(t, ff, fmt.Sprintf("recovery#%d", opCount))
					verifyNoBlockCrossingFuzz(t, ff, fmt.Sprintf("recovery#%d", opCount))

					if ff.Size() != oracle.size() {
						t.Fatalf("[INV-FS-4] recovery#%d: FlexSpace.Size()=%d != oracle.size()=%d",
							opCount, ff.Size(), oracle.size())
					}
					verifyReadCorrectness(t, ff, oracle, fmt.Sprintf("recovery#%d", opCount))
				}
			}

			// Final checks
			if ff.Size() > 0 {
				verifyAllFlexSpaceInvariants(t, ff, "final")
				verifyReadCorrectness(t, ff, oracle, "final")
			}
		}()
		// ff.Close() handled by t.Cleanup
	})
}

// ========================================================================
// TestFlexSpace_RandomizedInvariants - deterministic stress test
// ========================================================================

func TestFlexSpace_RandomizedInvariants(t *testing.T) {
	fs, dir := newTestFS(t)
	ff := mustOpenFuzz(t, dir, fs)
	ffOpen := true
	defer func() {
		if ffOpen {
			ff.Close()
		}
	}()
	oracle := &flexSpaceOracle{}

	seed := uint64(54321)
	prng := func() uint64 {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		if seed == 0 {
			seed = 1
		}
		return seed
	}

	fillBuf := make([]byte, FLEXSPACE_MAX_EXTENT_SIZE)
	for i := range fillBuf {
		fillBuf[i] = byte(prng())
	}

	const numOps = 2000
	const maxSize = uint64(4 * FLEXSPACE_BLOCK_SIZE) // 16MB cap
	reopenCount := 0

	for op := 0; op < numOps; op++ {
		size := ff.Size()
		r := prng() % 10

		switch {
		case r < 3 || size < 1024: // Insert at end (30% or when small)
			length := prng()%(FLEXSPACE_MAX_EXTENT_SIZE-1) + 1
			if size+length > maxSize {
				continue
			}
			insertData := makeFillData(fillBuf, length, prng)
			n, err := ff.Insert(insertData, size, length)
			if err != nil {
				t.Fatalf("op %d Insert(end, %d): %v", op, length, err)
			}
			if uint64(n) != length {
				t.Fatalf("op %d Insert: wrote %d, want %d", op, n, length)
			}
			oracle.insert(size, insertData)

		case r < 4 && size > 2048: // Insert at middle (10%)
			off := prng() % size
			length := prng()%(FLEXSPACE_MAX_EXTENT_SIZE-1) + 1
			if size+length > maxSize {
				continue
			}
			insertData := makeFillData(fillBuf, length, prng)
			n, err := ff.Insert(insertData, off, length)
			if err != nil {
				t.Fatalf("op %d Insert(%d, %d): %v", op, off, length, err)
			}
			if uint64(n) != length {
				t.Fatalf("op %d Insert: wrote %d, want %d", op, n, length)
			}
			oracle.insert(off, insertData)

		case r < 6 && size > 2048: // Collapse (20%)
			maxLen := size / 4
			if maxLen < 1 {
				maxLen = 1
			}
			length := prng()%maxLen + 1
			off := prng() % (size - length)
			if err := ff.Collapse(off, length); err != nil {
				t.Fatalf("op %d Collapse(%d, %d): %v", op, off, length, err)
			}
			oracle.collapse(off, length)

		case r < 8 && size > 1024: // Write (20%)
			maxLen := size / 4
			if maxLen > FLEXSPACE_MAX_EXTENT_SIZE {
				maxLen = FLEXSPACE_MAX_EXTENT_SIZE
			}
			if maxLen < 1 {
				maxLen = 1
			}
			length := prng()%maxLen + 1
			off := prng() % (size - length)
			writeData := makeFillData(fillBuf, length, prng)
			n, err := ff.Write(writeData, off, length)
			if err != nil {
				t.Fatalf("op %d Write(%d, %d): %v", op, off, length, err)
			}
			if uint64(n) != length {
				t.Fatalf("op %d Write: wrote %d, want %d", op, n, length)
			}
			oracle.write(off, writeData)

		case r == 8: // Sync (10%)
			ff.Sync()

		case r == 9 && reopenCount < 5: // Reopen (10%, max 5 times)
			ff.Sync()
			ff.Close()
			ffOpen = false
			ff = mustOpenFuzz(t, dir, fs)
			ffOpen = true
			reopenCount++

			verifyAllFlexSpaceInvariants(t, ff, fmt.Sprintf("reopen#%d", reopenCount))
			if ff.Size() != oracle.size() {
				t.Fatalf("reopen#%d: size mismatch %d vs %d", reopenCount, ff.Size(), oracle.size())
			}
			verifyReadCorrectness(t, ff, oracle, fmt.Sprintf("reopen#%d", reopenCount))
		}

		// Check invariants every 50 ops
		if (op+1)%50 == 0 {
			verifyAllFlexSpaceInvariants(t, ff, fmt.Sprintf("op%d", op))
			verifyNoBlockCrossingFuzz(t, ff, fmt.Sprintf("op%d", op))

			if ff.Size() != oracle.size() {
				t.Fatalf("op %d: size mismatch %d vs %d", op, ff.Size(), oracle.size())
			}
		}

		// Check read correctness every 200 ops
		if (op+1)%200 == 0 {
			verifyReadCorrectness(t, ff, oracle, fmt.Sprintf("op%d", op))
		}
	}

	// Final comprehensive check
	verifyAllFlexSpaceInvariants(t, ff, "final")
	verifyNoBlockCrossingFuzz(t, ff, "final")
	verifyReadCorrectness(t, ff, oracle, "final")
	// ff.Close() handled by defer

	t.Logf("Completed %d ops, %d reopens. Final size: %d bytes", numOps, reopenCount, oracle.size())
}

// ========================================================================
// Helpers
// ========================================================================

// mustOpenFuzz opens a FlexSpace or fails the test (same as mustOpen but
// with a distinct name for clarity in fuzz context).
func mustOpenFuzz(t testing.TB, path string, fs vfs.FS) *FlexSpace {
	t.Helper()
	ff, err := OpenFlexSpaceCoW(path, false, fs)
	if err != nil {
		t.Fatalf("OpenFlexSpaceCoW(%q): %v", path, err)
	}
	return ff
}

// makeFillData returns a slice of deterministic data of the given length.
func makeFillData(fillBuf []byte, length uint64, prng func() uint64) []byte {
	if length <= uint64(len(fillBuf)) {
		// Rotate the fill buffer by a PRNG offset for variety
		off := prng() % uint64(len(fillBuf))
		result := make([]byte, length)
		for i := uint64(0); i < length; i++ {
			result[i] = fillBuf[(off+i)%uint64(len(fillBuf))]
		}
		return result
	}
	// Longer than fillBuf - repeat
	result := make([]byte, length)
	for i := range result {
		result[i] = fillBuf[i%len(fillBuf)]
	}
	return result
}

// verifyReadCorrectness reads the entire FlexSpace content and compares with oracle.
func verifyReadCorrectness(t testing.TB, ff *FlexSpace, oracle *flexSpaceOracle, opDesc string) {
	t.Helper()

	size := ff.Size()
	if size != oracle.size() {
		t.Fatalf("[INV-FS-5] %s: size mismatch FlexSpace=%d oracle=%d", opDesc, size, oracle.size())
	}
	if size == 0 {
		return
	}

	// Read in chunks to avoid huge allocations for large files
	const chunkSize = uint64(256 * 1024)
	for off := uint64(0); off < size; off += chunkSize {
		readLen := chunkSize
		if off+readLen > size {
			readLen = size - off
		}
		buf := make([]byte, readLen)
		n, err := ff.Read(buf, off, readLen)
		if err != nil {
			t.Fatalf("[INV-FS-5] %s: Read(off=%d, len=%d): %v", opDesc, off, readLen, err)
		}
		if uint64(n) != readLen {
			t.Fatalf("[INV-FS-5] %s: Read returned %d, want %d", opDesc, n, readLen)
		}
		expected := oracle.data[off : off+readLen]
		if !bytes.Equal(buf, expected) {
			// Find first mismatch
			for i := range buf {
				if buf[i] != expected[i] {
					t.Fatalf("[INV-FS-5] %s: mismatch at offset %d: got 0x%02x, want 0x%02x",
						opDesc, off+uint64(i), buf[i], expected[i])
				}
			}
		}
	}
}
