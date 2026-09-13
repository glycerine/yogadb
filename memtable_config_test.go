package yogadb

import (
	"reflect"
	"strings"
	"testing"
)

func TestConfigMemtableKindSelectsBackingStore(t *testing.T) {
	for _, kind := range []MemtableKind{MemtableWormhole, MemtableKeyStable} {
		t.Run(kind.String(), func(t *testing.T) {
			db, _ := openTestDB(t, &Config{
				DisableBackgroundFlush: true,
				MemtableKind:           kind,
			})
			if db.mt.kind != kind {
				t.Fatalf("memtable kind = %v, want %v", db.mt.kind, kind)
			}

			for _, kv := range []struct {
				key string
				val string
			}{
				{"b", "bee"},
				{"a", "aye"},
				{"c", "see"},
			} {
				mustPut(t, db, kv.key, kv.val)
			}

			got, found, _, _, err := db.Get("b")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if !found || string(got) != "bee" {
				t.Fatalf("Get(%q) = %q, %v; want %q, true", "b", got, found, "bee")
			}

			kvc, exact, err := db.Find(GTE, "bb")
			if err != nil {
				t.Fatalf("Find(GTE): %v", err)
			}
			defer kvc.Close()
			if kvc == nil || exact || kvc.Key != "c" {
				t.Fatalf("Find(GTE, %q) = %#v, exact=%v; want key c, exact false", "bb", kvc, exact)
			}

			kvc, exact, err = db.Find(LTE, "bb")
			if err != nil {
				t.Fatalf("Find(LTE): %v", err)
			}
			defer kvc.Close()
			if kvc == nil || exact || kvc.Key != "b" {
				t.Fatalf("Find(LTE, %q) = %#v, exact=%v; want key b, exact false", "bb", kvc, exact)
			}

			var asc []string
			if err := db.View(func(ro *ReadOnlyTx) error {
				ro.Ascend("", func(key string, value []byte, vtyp uint64, hlc HLC) bool {
					asc = append(asc, key+"="+string(value))
					return true
				})
				return nil
			}); err != nil {
				t.Fatalf("View Ascend: %v", err)
			}
			if want := []string{"a=aye", "b=bee", "c=see"}; !reflect.DeepEqual(asc, want) {
				t.Fatalf("Ascend = %v, want %v", asc, want)
			}

			var desc []string
			if err := db.View(func(ro *ReadOnlyTx) error {
				ro.Descend("", func(key string, value []byte, vtyp uint64, hlc HLC) bool {
					desc = append(desc, key)
					return true
				})
				return nil
			}); err != nil {
				t.Fatalf("View Descend: %v", err)
			}
			if got, want := strings.Join(desc, ","), "c,b,a"; got != want {
				t.Fatalf("Descend keys = %q, want %q", got, want)
			}

			var ranged []string
			if err := db.View(func(ro *ReadOnlyTx) error {
				ro.AscendRange("b", "d", func(key string, value []byte, vtyp uint64, hlc HLC) bool {
					ranged = append(ranged, key)
					return true
				})
				return nil
			}); err != nil {
				t.Fatalf("View AscendRange: %v", err)
			}
			if want := []string{"b", "c"}; !reflect.DeepEqual(ranged, want) {
				t.Fatalf("AscendRange keys = %v, want %v", ranged, want)
			}
		})
	}
}

func TestConfigMemtableKindRejectsInvalidValue(t *testing.T) {
	fs, dir := newTestFS(t)
	_, err := OpenFlexDB(dir, &Config{
		FS:           fs,
		MemtableKind: MemtableKind(99),
	})
	if err == nil {
		t.Fatal("OpenFlexDB accepted invalid Config.MemtableKind")
	}
	if !strings.Contains(err.Error(), "invalid Config.MemtableKind") {
		t.Fatalf("OpenFlexDB error = %v, want invalid Config.MemtableKind", err)
	}
}
