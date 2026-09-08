package yogadb

import "testing"

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
