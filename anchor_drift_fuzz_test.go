//go:build memfs

package yogadb

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/glycerine/vfs"
)

// FuzzAnchorTreeDrift exercises sequences of Put, Delete, Sync, VacuumKV,
// and Close+Reopen to detect cases where the memSparseIndexTree (anchor tree)
// drifts out of sync with the FlexTree's logical offsets and tags.
//
// The bug this catches: when flushDirtyPages resizes a tight page via
// ff.Update, the FlexTree shifts subsequent loffs but the anchor tree's
// shifts were not updated, causing verifyAnchorTags to fail.
func FuzzAnchorTreeDrift(f *testing.F) {
	// Seeds covering key scenarios:
	// 1. Basic put + sync
	f.Add([]byte{0, 0, 0, 0, 30, 0, 1, 0, 30, 4})
	// 2. Put + vacuum + put + sync (the original failing pattern)
	f.Add([]byte{0, 0, 0, 0, 30, 0, 0, 1, 0, 30, 4, 5, 0, 0, 2, 0, 30, 4})
	// 3. Many puts filling multiple anchors, then sync
	f.Add([]byte{
		0, 0, 0, 0, 40, 0, 0, 1, 0, 40, 0, 0, 2, 0, 40, 0, 0, 3, 0, 40,
		0, 0, 4, 0, 40, 0, 0, 5, 0, 40, 0, 0, 6, 0, 40, 0, 0, 7, 0, 40,
		4, 4,
	})
	// 4. Put + sync + vacuum + reopen + put + sync
	f.Add([]byte{0, 0, 0, 0, 30, 4, 5, 6, 0, 0, 0, 0, 35, 4})
	// 5. Interleaved deletes
	f.Add([]byte{0, 0, 0, 0, 30, 0, 0, 1, 0, 30, 1, 0, 0, 4, 0, 0, 2, 0, 30, 4})

	f.Fuzz(func(t *testing.T, data []byte) {
		fs := vfs.NewMem()
		dir := "fuzz_anchor_drift"
		if err := fs.MkdirAll(dir, 0755); err != nil {
			return
		}

		db := openFuzzDB(fs, dir)
		if db == nil {
			return
		}
		defer func() {
			// Always close DB to avoid leaking goroutines.
			db.Close()
		}()

		// Track what keys we've inserted and their expected values,
		// so we can verify correctness after reopen.
		kv := newOmap[string, string]() // key -> value
		synced := false

		// Wrap in func+recover so panicOn() calls don't kill the fuzz worker subprocess.
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic in fuzz iteration: %v", r)
				}
			}()

			i := 0
			for i < len(data) {
				op := data[i] % 7
				i++

				switch op {
				case 0: // Put with key index and value size from fuzz data
					if i+4 > len(data) {
						return
					}
					keyIdx := int(binary.BigEndian.Uint16(data[i : i+2]))
					valSize := int(data[i+2])
					valSeed := int(data[i+3])
					i += 4

					keyIdx = keyIdx % 500 // cap key space
					if valSize < 5 {
						valSize = 5
					}
					if valSize > 55 { // keep values inline (< 64)
						valSize = 55
					}

					key := fmt.Sprintf("k%04d", keyIdx)
					val := makeFuzzValue(valSize, valSeed)
					err := db.Put(key, []byte(val))
					if err != nil {
						t.Fatalf("Put(%q): %v", key, err)
					}
					kv.set(key, val)
					synced = false

				case 1: // Delete
					if i+2 > len(data) || kv.Len() == 0 {
						continue
					}
					keyIdx := int(binary.BigEndian.Uint16(data[i : i+2]))
					i += 2
					keyIdx = keyIdx % 500
					key := fmt.Sprintf("k%04d", keyIdx)
					if _, exists := kv.get2(key); exists {
						err := db.Delete(key)
						if err != nil {
							t.Fatalf("Delete(%q): %v", key, err)
						}
						kv.delkey(key)
						synced = false
					}

				case 2: // Put batch - insert several sequential keys
					if i+2 > len(data) {
						continue
					}
					startIdx := int(data[i]) * 2
					count := int(data[i+1])%20 + 1
					i += 2
					for j := 0; j < count; j++ {
						key := fmt.Sprintf("k%04d", (startIdx+j)%500)
						val := makeFuzzValue(30, startIdx+j)
						err := db.Put(key, []byte(val))
						if err != nil {
							t.Fatalf("Put(%q): %v", key, err)
						}
						kv.set(key, val)
					}
					synced = false

				case 3: // Overwrite existing keys with different-sized values
					if i+2 > len(data) || kv.Len() == 0 {
						continue
					}
					newSize := int(data[i])%50 + 5
					count := int(data[i+1])%10 + 1
					i += 2
					j := 0
					for key := range kv.all() {
						if j >= count {
							break
						}
						val := makeFuzzValue(newSize, j+newSize)
						err := db.Put(key, []byte(val))
						if err != nil {
							t.Fatalf("Put(%q): %v", key, err)
						}
						kv.set(key, val)
						j++
					}
					synced = false

				case 4: // Sync - triggers flushDirtyPages (the original bug site)
					if kv.Len() == 0 {
						continue
					}
					db.Sync()
					synced = true
					// verifyAnchorTags is called inside Sync, so if we get
					// here without panic, the anchor tree is in sync.

				case 5: // VacuumKV - creates tight pages, rebuilds FlexTree
					if !synced || kv.Len() == 0 {
						continue
					}
					_, err := db.VacuumKV()
					if err != nil {
						t.Fatalf("VacuumKV: %v", err)
					}

				case 6: // Close + Reopen - tests recovery path
					if kv.Len() == 0 {
						continue
					}
					if !synced {
						db.Sync()
						synced = true
					}
					anchorsBefore := db.tree.countAnchors()
					t.Logf("before Close: %d anchors, %d keys", anchorsBefore, kv.Len())
					db.Close()
					db = openFuzzDB(fs, dir)
					if db == nil {
						t.Fatal("reopen failed")
					}
					anchorsAfter := db.tree.countAnchors()
					t.Logf("after Reopen: %d anchors (was %d)", anchorsAfter, anchorsBefore)
					// Verify all synced keys survived recovery.
					for key, wantVal := range kv.all() {
						gotVal, ok, gerr := db.Get(key)
						panicOn(gerr)
						if !ok {
							t.Fatalf("after reopen: Get(%q) not found (anchors before=%d after=%d)", key, anchorsBefore, anchorsAfter)
						}
						if string(gotVal) != wantVal {
							t.Fatalf("after reopen: Get(%q) = %q, want %q", key, gotVal, wantVal)
						}
					}
				}
			}

			// Final sync + integrity check.
			if kv.Len() > 0 {
				db.Sync()
				errs := db.CheckIntegrity()
				if len(errs) > 0 {
					for _, e := range errs {
						t.Errorf("integrity: %v", e)
					}
					t.Fatalf("CheckIntegrity found %d errors", len(errs))
				}
			}
		}() // end recover wrapper
	})
}

// openFuzzDB opens a FlexDB for fuzz testing. Returns nil on error.
func openFuzzDB(fs vfs.FS, dir string) *FlexDB {
	cfg := &Config{FS: fs}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		return nil
	}
	return db
}

// makeFuzzValue creates a deterministic test value of the given size.
func makeFuzzValue(size, seed int) string {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte('A' + ((seed + i) % 26))
	}
	return string(b)
}

// TestAnchorTreeDrift_VacuumThenWrite is a deterministic regression test
// for the anchor tree shift drift bug. After VacuumKV creates tight pages,
// writing new data and syncing must propagate page-growth shifts correctly
// in the anchor tree.
func TestAnchorTreeDrift_VacuumThenWrite(t *testing.T) {
	fs, dir := newTestFS(t)

	// Populate with enough keys to create multiple anchors.
	db := openTestDBAt(fs, t, dir, nil)
	for i := 0; i < 200; i++ {
		mustPut(t, db, fmt.Sprintf("drift_%04d", i), makeTestValue(40))
	}
	db.Sync()
	db.Close()

	// Reopen, overwrite, vacuum (creates tight pages).
	db = openTestDBAt(fs, t, dir, nil)
	for i := 0; i < 200; i++ {
		mustPut(t, db, fmt.Sprintf("drift_%04d", i), makeTestValue(41))
	}
	db.Sync()
	_, err := db.VacuumKV()
	if err != nil {
		t.Fatalf("VacuumKV: %v", err)
	}
	db.Close()

	// Reopen after vacuum. Write same keys with different values.
	// The dirty flush must resize tight pages and propagate shifts.
	db = openTestDBAt(fs, t, dir, nil)
	for i := 0; i < 200; i++ {
		mustPut(t, db, fmt.Sprintf("drift_%04d", i), makeTestValue(42))
	}
	// This Sync triggers flushDirtyPages which resizes tight-padded pages.
	// Without the shift propagation fix, verifyAnchorTags panics here.
	db.Sync()

	// Verify data correctness.
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("drift_%04d", i)
		mustGet(t, db, key, makeTestValue(42))
	}

	mustCheckIntegrity(t, db)
	db.Close()
}

// TestAnchorTreeDrift_MultipleResizeCycles tests that multiple rounds of
// tight-padded page resizing don't accumulate shift errors.
func TestAnchorTreeDrift_MultipleResizeCycles(t *testing.T) {
	fs, dir := newTestFS(t)

	db := openTestDBAt(fs, t, dir, nil)

	for cycle := 0; cycle < 3; cycle++ {
		// Write keys.
		for i := 0; i < 150; i++ {
			key := fmt.Sprintf("mc_%04d", i)
			mustPut(t, db, key, makeTestValue(35+cycle))
		}
		db.Sync()

		if cycle > 0 {
			// Vacuum compacts to tight pages.
			_, err := db.VacuumKV()
			if err != nil {
				t.Fatalf("cycle %d VacuumKV: %v", cycle, err)
			}
		}

		// Close + reopen (recovery rebuilds anchors from tags).
		db.Close()
		db = openTestDBAt(fs, t, dir, nil)

		// Verify data.
		for i := 0; i < 150; i++ {
			key := fmt.Sprintf("mc_%04d", i)
			mustGet(t, db, key, makeTestValue(35+cycle))
		}
	}

	mustCheckIntegrity(t, db)
	db.Close()
}

// TestAnchorTreeDrift_NewKeysAfterVacuum tests adding NEW keys (not just
// overwriting) after vacuum. New keys may trigger anchor splits via
// treeInsertAnchor, which must handle tight page resizing correctly.
func TestAnchorTreeDrift_NewKeysAfterVacuum(t *testing.T) {
	fs, dir := newTestFS(t)

	db := openTestDBAt(fs, t, dir, nil)
	for i := 0; i < 100; i++ {
		mustPut(t, db, fmt.Sprintf("nk_%04d", i), makeTestValue(40))
	}
	db.Sync()

	// Overwrite + vacuum - tight pages.
	for i := 0; i < 100; i++ {
		mustPut(t, db, fmt.Sprintf("nk_%04d", i), makeTestValue(41))
	}
	db.Sync()
	_, err := db.VacuumKV()
	if err != nil {
		t.Fatalf("VacuumKV: %v", err)
	}

	// Add NEW keys that interleave with existing anchors.
	// These trigger anchor splits and page resizing.
	for i := 100; i < 250; i++ {
		mustPut(t, db, fmt.Sprintf("nk_%04d", i), makeTestValue(42))
	}
	db.Sync()

	// Verify all data.
	for i := 0; i < 100; i++ {
		mustGet(t, db, fmt.Sprintf("nk_%04d", i), makeTestValue(41))
	}
	for i := 100; i < 250; i++ {
		mustGet(t, db, fmt.Sprintf("nk_%04d", i), makeTestValue(42))
	}

	mustCheckIntegrity(t, db)
	db.Close()
}

// TestAnchorTreeDrift_DeleteAndReinsert tests that deleting keys, vacuuming,
// then reinserting them doesn't cause drift.
func TestAnchorTreeDrift_DeleteAndReinsert(t *testing.T) {
	fs, dir := newTestFS(t)

	db := openTestDBAt(fs, t, dir, nil)
	for i := 0; i < 150; i++ {
		mustPut(t, db, fmt.Sprintf("dr_%04d", i), makeTestValue(38))
	}
	db.Sync()

	// Delete half the keys.
	for i := 0; i < 150; i += 2 {
		mustDelete(t, db, fmt.Sprintf("dr_%04d", i))
	}
	db.Sync()

	// Vacuum.
	_, err := db.VacuumKV()
	if err != nil {
		t.Fatalf("VacuumKV: %v", err)
	}

	// Reinsert deleted keys with different values.
	for i := 0; i < 150; i += 2 {
		mustPut(t, db, fmt.Sprintf("dr_%04d", i), makeTestValue(39))
	}
	db.Sync()

	// Verify all keys.
	for i := 0; i < 150; i++ {
		key := fmt.Sprintf("dr_%04d", i)
		if i%2 == 0 {
			mustGet(t, db, key, makeTestValue(39))
		} else {
			mustGet(t, db, key, makeTestValue(38))
		}
	}

	mustCheckIntegrity(t, db)
	db.Close()
}
