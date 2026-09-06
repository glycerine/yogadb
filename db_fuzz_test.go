package yogadb

import (
	"bytes"
	"testing"
)

type dbVtypFuzzRecord struct {
	key      string
	value    []byte
	vtyp     uint64
	viaBatch bool
}

func FuzzFlexDBVtypRoundTrip(f *testing.F) {
	f.Add(uint64(0), uint64(1), uint64(2), uint64(3))
	f.Add(uint64(0x1122334455667788), uint64(0x8877665544332211), uint64(0), uint64(42))
	f.Add(^uint64(0), uint64(64), uint64(65), uint64(0x8000000000000000))

	f.Fuzz(func(t *testing.T, put64Vtyp, put65Vtyp, batch64Vtyp, batch65Vtyp uint64) {
		db, _ := openTestDB(t, &Config{DisableBackgroundFlush: true})

		records := []dbVtypFuzzRecord{
			{
				key:   "put/64",
				value: bytes.Repeat([]byte{'p'}, 64),
				vtyp:  put64Vtyp,
			},
			{
				key:   "put/65",
				value: bytes.Repeat([]byte{'q'}, 65),
				vtyp:  put65Vtyp,
			},
			{
				key:      "batch/64",
				value:    bytes.Repeat([]byte{'b'}, 64),
				vtyp:     batch64Vtyp,
				viaBatch: true,
			},
			{
				key:      "batch/65",
				value:    bytes.Repeat([]byte{'c'}, 65),
				vtyp:     batch65Vtyp,
				viaBatch: true,
			},
		}

		for _, rec := range records {
			if rec.viaBatch {
				continue
			}
			if err := db.Put(rec.key, rec.value, rec.vtyp); err != nil {
				t.Fatalf("Put(%q, len=%d, vtyp=%#x): %v", rec.key, len(rec.value), rec.vtyp, err)
			}
		}

		batch := db.NewBatch()
		for _, rec := range records {
			if !rec.viaBatch {
				continue
			}
			if err := batch.Set(rec.key, rec.value, rec.vtyp); err != nil {
				t.Fatalf("Batch.Set(%q, len=%d, vtyp=%#x): %v", rec.key, len(rec.value), rec.vtyp, err)
			}
		}
		if _, err := batch.Commit(false); err != nil {
			t.Fatalf("Batch.Commit: %v", err)
		}

		assertDBVtypFuzzRecords(t, db, records, "before Sync")
		if err := db.Sync(); err != nil {
			t.Fatalf("Sync: %v", err)
		}
		assertDBVtypFuzzRecords(t, db, records, "after Sync")
	})
}

func assertDBVtypFuzzRecords(t *testing.T, db *FlexDB, records []dbVtypFuzzRecord, phase string) {
	t.Helper()

	for _, rec := range records {
		got, found, gotVtyp, err := db.Get(rec.key)
		if err != nil {
			t.Fatalf("%s Get(%q): %v", phase, rec.key, err)
		}
		if !found {
			t.Fatalf("%s Get(%q): not found", phase, rec.key)
		}
		if !bytes.Equal(got, rec.value) {
			t.Fatalf("%s Get(%q) value=%q, want %q", phase, rec.key, got, rec.value)
		}
		if gotVtyp != rec.vtyp {
			t.Fatalf("%s Get(%q) vtyp=%#x, want %#x", phase, rec.key, gotVtyp, rec.vtyp)
		}
	}

	wantByKey := make(map[string]dbVtypFuzzRecord, len(records))
	for _, rec := range records {
		wantByKey[rec.key] = rec
	}
	seen := make(map[string]bool, len(records))

	if err := db.View(func(roDB *ReadOnlyTx) error {
		it := roDB.NewIter()
		defer it.Close()

		for it.SeekFirst(); it.Valid(); it.Next() {
			key, got, gotVtyp, found, err := it.GetAnySize()
			if err != nil {
				t.Fatalf("%s iterator GetAnySize: %v", phase, err)
			}
			if !found {
				t.Fatalf("%s iterator at valid position reported found=false", phase)
			}
			want, ok := wantByKey[key]
			if !ok {
				continue
			}
			if !bytes.Equal(got, want.value) {
				t.Fatalf("%s iterator key %q value=%q, want %q", phase, key, got, want.value)
			}
			if gotVtyp != want.vtyp {
				t.Fatalf("%s iterator key %q vtyp=%#x, want %#x", phase, key, gotVtyp, want.vtyp)
			}
			seen[key] = true
		}
		return nil
	}); err != nil {
		t.Fatalf("%s View: %v", phase, err)
	}

	for _, rec := range records {
		if !seen[rec.key] {
			t.Fatalf("%s iterator did not visit key %q", phase, rec.key)
		}
	}
}
