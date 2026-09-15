package yogabluge

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"testing"

	bluge "github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/glycerine/yogadb"
)

type bytesWriterTo []byte

func (b bytesWriterTo) WriteTo(w io.Writer, closeCh chan struct{}) (int64, error) {
	if closeCh != nil {
		select {
		case <-closeCh:
			return 0, context.Canceled
		default:
		}
	}
	n, err := w.Write(b)
	return int64(n), err
}

func openFlexDBForBlugeTest(t *testing.T, path string) *yogadb.FlexDB {
	t.Helper()
	db, err := yogadb.OpenFlexDB(path, &yogadb.Config{DisableBackgroundFlush: true})
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}
	db.AllowReads()
	return db
}

func TestDirectoryPersistLoadListRemoveStats(t *testing.T) {
	db := openFlexDBForBlugeTest(t, t.TempDir())
	t.Cleanup(func() { db.Close() })

	dir := NewDirectory(db, "idx")
	if err := dir.Setup(false); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if err := dir.Persist(index.ItemKindSegment, 2, bytesWriterTo("segment-two"), nil); err != nil {
		t.Fatalf("Persist segment 2: %v", err)
	}
	if err := dir.Persist(index.ItemKindSegment, 1, bytesWriterTo("segment-one"), nil); err != nil {
		t.Fatalf("Persist segment 1: %v", err)
	}
	if err := dir.Persist(index.ItemKindSnapshot, 7, bytesWriterTo("snapshot-seven"), nil); err != nil {
		t.Fatalf("Persist snapshot 7: %v", err)
	}

	ids, err := dir.List(index.ItemKindSegment)
	if err != nil {
		t.Fatalf("List segments: %v", err)
	}
	if want := []uint64{2, 1}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("List segments = %v, want %v", ids, want)
	}

	data, closer, err := dir.Load(index.ItemKindSegment, 2)
	if err != nil {
		t.Fatalf("Load segment 2: %v", err)
	}
	if closer != nil {
		t.Fatalf("Load returned unexpected closer")
	}
	got, err := data.Read(0, data.Len())
	if err != nil {
		t.Fatalf("Read loaded segment: %v", err)
	}
	if string(got) != "segment-two" {
		t.Fatalf("loaded segment = %q, want %q", got, "segment-two")
	}

	numItems, numBytes := dir.Stats()
	if numItems != 3 || numBytes != uint64(len("segment-two")+len("segment-one")+len("snapshot-seven")) {
		t.Fatalf("Stats = (%d, %d), want 3 items with expected bytes", numItems, numBytes)
	}

	if err := dir.Remove(index.ItemKindSegment, 1); err != nil {
		t.Fatalf("Remove segment 1: %v", err)
	}
	ids, err = dir.List(index.ItemKindSegment)
	if err != nil {
		t.Fatalf("List after remove: %v", err)
	}
	if want := []uint64{2}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("List after remove = %v, want %v", ids, want)
	}
	if _, _, err := dir.Load(index.ItemKindSegment, 1); err == nil {
		t.Fatalf("Load removed segment succeeded, want error")
	}
}

func TestDirectorySurvivesFlexDBReopen(t *testing.T) {
	path := t.TempDir()
	db := openFlexDBForBlugeTest(t, path)
	dir := NewDirectory(db, "persisted")
	if err := dir.Setup(false); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if err := dir.Persist(index.ItemKindSnapshot, 42, bytesWriterTo("snapshot"), nil); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if err := dir.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	db.Close()

	db = openFlexDBForBlugeTest(t, path)
	t.Cleanup(func() { db.Close() })
	dir = NewDirectory(db, "persisted")
	if err := dir.Setup(true); err != nil {
		t.Fatalf("Setup after reopen: %v", err)
	}
	data, _, err := dir.Load(index.ItemKindSnapshot, 42)
	if err != nil {
		t.Fatalf("Load after reopen: %v", err)
	}
	got, err := data.Read(0, data.Len())
	if err != nil {
		t.Fatalf("Read after reopen: %v", err)
	}
	if string(got) != "snapshot" {
		t.Fatalf("snapshot after reopen = %q, want %q", got, "snapshot")
	}
}

func TestBlugeSearchRoundTripBackedByYogaDB(t *testing.T) {
	db := openFlexDBForBlugeTest(t, t.TempDir())
	t.Cleanup(func() { db.Close() })

	config := NewConfig(db, "search")
	writer, err := bluge.OpenWriter(config)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	docs := []*bluge.Document{
		bluge.NewDocument("doc-1").
			AddField(bluge.NewTextField("body", "yoga backed full text search").StoreValue()).
			AddField(bluge.NewKeywordField("kind", "note").StoreValue().Aggregatable()),
		bluge.NewDocument("doc-2").
			AddField(bluge.NewTextField("body", "plain storage engine notes").StoreValue()).
			AddField(bluge.NewKeywordField("kind", "note").StoreValue().Aggregatable()),
	}
	for _, doc := range docs {
		if err := writer.Update(doc.ID(), doc); err != nil {
			t.Fatalf("Update: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	reader, err := bluge.OpenReader(config)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer reader.Close()

	query := bluge.NewMatchQuery("yoga").SetField("body")
	req := bluge.NewTopNSearch(10, query).WithStandardAggregations()
	matches, err := reader.Search(context.Background(), req)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	match, err := matches.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if match == nil {
		t.Fatalf("Search returned no matches")
	}

	var stored bytes.Buffer
	if err := match.VisitStoredFields(func(field string, value []byte) bool {
		if field == "_id" {
			stored.Write(value)
		}
		return true
	}); err != nil {
		t.Fatalf("VisitStoredFields: %v", err)
	}
	if stored.String() != "doc-1" {
		t.Fatalf("matched _id = %q, want doc-1", stored.String())
	}
}
