package yogableve

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/index/upsidedown"
	"github.com/blevesearch/bleve/v2/mapping"
	store "github.com/blevesearch/upsidedown_store_api"
	storetest "github.com/blevesearch/upsidedown_store_api/test"
	"github.com/glycerine/yogadb"
)

func openTestStore(t *testing.T, mo store.MergeOperator) (*Store, *yogadb.FlexDB) {
	t.Helper()
	db, err := yogadb.OpenFlexDB(t.TempDir(), &yogadb.Config{DisableBackgroundFlush: true})
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}
	db.AllowReads()
	return NewStore(db, t.Name(), mo), db
}

func runStoreTest(t *testing.T, mo store.MergeOperator, fn func(*testing.T, store.KVStore)) {
	t.Helper()
	s, db := openTestStore(t, mo)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Fatalf("store Close: %v", err)
		}
		db.Close()
	})
	fn(t, s)
}

func TestKVCrud(t *testing.T) {
	runStoreTest(t, nil, storetest.CommonTestKVCrud)
}

func TestReaderIsolation(t *testing.T) {
	runStoreTest(t, nil, storetest.CommonTestReaderIsolation)
}

func TestReaderOwnsGetBytes(t *testing.T) {
	runStoreTest(t, nil, storetest.CommonTestReaderOwnsGetBytes)
}

func TestWriterOwnsBytes(t *testing.T) {
	runStoreTest(t, nil, storetest.CommonTestWriterOwnsBytes)
}

func TestPrefixIterator(t *testing.T) {
	runStoreTest(t, nil, storetest.CommonTestPrefixIterator)
}

func TestPrefixIteratorSeek(t *testing.T) {
	runStoreTest(t, nil, storetest.CommonTestPrefixIteratorSeek)
}

func TestRangeIterator(t *testing.T) {
	runStoreTest(t, nil, storetest.CommonTestRangeIterator)
}

func TestRangeIteratorSeek(t *testing.T) {
	runStoreTest(t, nil, storetest.CommonTestRangeIteratorSeek)
}

func TestMerge(t *testing.T) {
	runStoreTest(t, &storetest.TestMergeCounter{}, storetest.CommonTestMerge)
}

func TestBinaryKeysPreserveOrder(t *testing.T) {
	s, db := openTestStore(t, nil)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Fatalf("store Close: %v", err)
		}
		db.Close()
	})

	w, err := s.Writer()
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	b := w.NewBatch()
	keys := [][]byte{
		{0x00},
		{0x00, 0xff},
		{0x01},
		{'t', 0xff, 'a'},
		{'t', 0xff, 'b'},
		{0xff},
	}
	for _, key := range keys {
		b.Set(key, []byte("v"))
	}
	if err := w.ExecuteBatch(b); err != nil {
		t.Fatalf("ExecuteBatch: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Writer Close: %v", err)
	}

	r, err := s.Reader()
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer r.Close()

	it := r.RangeIterator(nil, nil)
	defer it.Close()
	var got [][]byte
	for it.Valid() {
		got = append(got, append([]byte(nil), it.Key()...))
		it.Next()
	}
	if !reflect.DeepEqual(got, keys) {
		t.Fatalf("range order = %#v, want %#v", got, keys)
	}

	it = r.PrefixIterator([]byte{'t', 0xff})
	defer it.Close()
	var prefixed [][]byte
	for it.Valid() {
		prefixed = append(prefixed, append([]byte(nil), it.Key()...))
		it.Next()
	}
	if want := keys[3:5]; !reflect.DeepEqual(prefixed, want) {
		t.Fatalf("prefix keys = %#v, want %#v", prefixed, want)
	}
}

func TestRegistryConstructorStats(t *testing.T) {
	kv, err := New(nil, map[string]interface{}{
		"path":                   t.TempDir(),
		"namespace":              "stats",
		"disableBackgroundFlush": true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	w, err := kv.Writer()
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	b := w.NewBatch()
	b.Set([]byte("a"), []byte("alpha"))
	b.Set([]byte("b"), []byte("beta"))
	if err := w.ExecuteBatch(b); err != nil {
		t.Fatalf("ExecuteBatch: %v", err)
	}

	stats, ok := kv.(store.KVStoreStats)
	if !ok {
		t.Fatalf("store does not implement KVStoreStats")
	}
	buf, err := stats.Stats().MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(buf, &decoded); err != nil {
		t.Fatalf("stats JSON: %v", err)
	}
	if decoded["items"].(float64) != 2 {
		t.Fatalf("stats items = %v, want 2", decoded["items"])
	}
}

func TestBleveUpsideDownSearchBackedByYogaDB(t *testing.T) {
	index, err := bleve.NewUsing(
		t.TempDir(),
		testMapping(),
		upsidedown.Name,
		Name,
		map[string]interface{}{"disableBackgroundFlush": true},
	)
	if err != nil {
		t.Fatalf("NewUsing: %v", err)
	}
	t.Cleanup(func() {
		if err := index.Close(); err != nil {
			t.Fatalf("index Close: %v", err)
		}
	})

	docs := map[string]interface{}{
		"doc-1": map[string]interface{}{"body": "yoga backed full text search", "kind": "note"},
		"doc-2": map[string]interface{}{"body": "plain storage engine notes", "kind": "note"},
		"doc-3": map[string]interface{}{"body": "yogadb indexing experiment", "kind": "lab"},
	}
	for id, doc := range docs {
		if err := index.Index(id, doc); err != nil {
			t.Fatalf("Index %s: %v", id, err)
		}
	}

	query := bleve.NewMatchQuery("yoga")
	query.SetField("body")
	req := bleve.NewSearchRequest(query)
	req.Fields = []string{"kind"}
	req.AddFacet("kinds", bleve.NewFacetRequest("kind", 10))

	res, err := index.Search(req)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if res.Total != 1 {
		t.Fatalf("Search total = %d, want 1; hits=%#v", res.Total, res.Hits)
	}
	if got := res.Hits[0].ID; got != "doc-1" {
		t.Fatalf("Search hit = %q, want doc-1", got)
	}
	facet := res.Facets["kinds"]
	if facet == nil || len(facet.Terms.Terms()) == 0 {
		t.Fatalf("expected kind facet terms, got %#v", res.Facets)
	}
}

func TestBleveUpsideDownDeleteBackedByYogaDB(t *testing.T) {
	index, err := bleve.NewUsing(
		t.TempDir(),
		testMapping(),
		upsidedown.Name,
		Name,
		map[string]interface{}{"disableBackgroundFlush": true},
	)
	if err != nil {
		t.Fatalf("NewUsing: %v", err)
	}
	t.Cleanup(func() {
		if err := index.Close(); err != nil {
			t.Fatalf("index Close: %v", err)
		}
	})

	if err := index.Index("doc-1", map[string]interface{}{"body": "delete me", "kind": "note"}); err != nil {
		t.Fatalf("Index doc-1: %v", err)
	}
	if err := index.Index("doc-2", map[string]interface{}{"body": "keep me", "kind": "note"}); err != nil {
		t.Fatalf("Index doc-2: %v", err)
	}
	if err := index.Delete("doc-1"); err != nil {
		t.Fatalf("Delete doc-1: %v", err)
	}

	req := bleve.NewSearchRequest(bleve.NewMatchQuery("keep"))
	res, err := index.Search(req)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if res.Total != 1 || res.Hits[0].ID != "doc-2" {
		t.Fatalf("Search after delete = total %d hits %#v, want doc-2 only", res.Total, res.Hits)
	}
}

func testMapping() mapping.IndexMapping {
	body := bleve.NewTextFieldMapping()
	body.Store = true
	body.IncludeTermVectors = true

	kind := bleve.NewKeywordFieldMapping()
	kind.Store = true

	doc := bleve.NewDocumentMapping()
	doc.AddFieldMappingsAt("body", body)
	doc.AddFieldMappingsAt("kind", kind)

	mapping := bleve.NewIndexMapping()
	mapping.DefaultMapping = doc
	return mapping
}
