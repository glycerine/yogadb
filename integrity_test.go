package yogadb

import (
	"strings"
	"testing"
)

func TestCheckIntegrityReportsMissingAnchorTag(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{
		FS:                     fs,
		DisableBackgroundFlush: true,
	}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	mustPut(t, db, "anchor-tag-key", "anchor-tag-value")
	if err := db.Sync(); err != nil {
		db.Close()
		t.Fatalf("Sync: %v", err)
	}
	if err := db.ff.SetTag(0, 0); err != nil {
		db.Close()
		t.Fatalf("clear anchor tag: %v", err)
	}

	db.AllowReads()
	errs := db.CheckIntegrity()
	restoreErr := db.ff.SetTag(0, flexdbTagGenerate(true, 0))
	db.Close()
	if restoreErr != nil {
		t.Fatalf("restore anchor tag: %v", restoreErr)
	}

	for _, err := range errs {
		if err.Check == "anchor_tag" {
			return
		}
	}
	t.Fatalf("CheckIntegrity did not report missing anchor tag; got %v", errs)
}

func TestAnchorPageResizeRestampsMissingAnchorTag(t *testing.T) {
	fs, dir := newTestFS(t)
	cfg := &Config{
		FS:                     fs,
		DisableBackgroundFlush: true,
	}
	db, err := OpenFlexDB(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	mustPut(t, db, "anchor-tag-key", "small")
	if err := db.Sync(); err != nil {
		db.Close()
		t.Fatalf("initial Sync: %v", err)
	}
	if err := db.ff.SetTag(0, 0); err != nil {
		db.Close()
		t.Fatalf("clear anchor tag: %v", err)
	}

	mustPut(t, db, "anchor-tag-key", strings.Repeat("x", 48))
	func() {
		defer func() {
			if r := recover(); r != nil {
				_ = db.ff.SetTag(0, flexdbTagGenerate(true, 0))
				db.Close()
				t.Fatalf("Sync panicked before restamping the rewritten anchor page: %v", r)
			}
		}()
		if err := db.Sync(); err != nil {
			db.Close()
			t.Fatalf("rewrite Sync: %v", err)
		}
	}()

	tag, tagErr := db.ff.GetTag(0)
	db.Close()
	if tagErr != nil || !flexdbTagIsAnchor(tag) {
		t.Fatalf("rewritten anchor page was not restamped: tag=0x%04x err=%v", tag, tagErr)
	}
}
