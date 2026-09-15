package kvstore

import (
	"testing"

	store "github.com/blevesearch/upsidedown_store_api"
	"github.com/glycerine/yogadb"
	"github.com/glycerine/yogadb/lib/yogableve"
	storetest "github.com/glycerine/yogadb/lib/yogableve/blevetests/kvstoretest"
)

func open(t *testing.T, mo store.MergeOperator) (*yogableve.Store, *yogadb.FlexDB) {
	t.Helper()
	db, err := yogadb.OpenFlexDB(t.TempDir(), &yogadb.Config{DisableBackgroundFlush: true})
	if err != nil {
		t.Fatalf("OpenFlexDB: %v", err)
	}
	db.AllowReads()
	return yogableve.NewStore(db, t.Name(), mo), db
}

func cleanup(t *testing.T, s *yogableve.Store, db *yogadb.FlexDB) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("store Close: %v", err)
	}
	db.Close()
}

func run(t *testing.T, mo store.MergeOperator, fn func(*testing.T, store.KVStore)) {
	t.Helper()
	s, db := open(t, mo)
	defer cleanup(t, s, db)
	fn(t, s)
}

func TestYogaDBKVCrud(t *testing.T) {
	run(t, nil, storetest.CommonTestKVCrud)
}

func TestYogaDBReaderIsolation(t *testing.T) {
	run(t, nil, storetest.CommonTestReaderIsolation)
}

func TestYogaDBReaderOwnsGetBytes(t *testing.T) {
	run(t, nil, storetest.CommonTestReaderOwnsGetBytes)
}

func TestYogaDBWriterOwnsBytes(t *testing.T) {
	run(t, nil, storetest.CommonTestWriterOwnsBytes)
}

func TestYogaDBPrefixIterator(t *testing.T) {
	run(t, nil, storetest.CommonTestPrefixIterator)
}

func TestYogaDBPrefixIteratorSeek(t *testing.T) {
	run(t, nil, storetest.CommonTestPrefixIteratorSeek)
}

func TestYogaDBRangeIterator(t *testing.T) {
	run(t, nil, storetest.CommonTestRangeIterator)
}

func TestYogaDBRangeIteratorSeek(t *testing.T) {
	run(t, nil, storetest.CommonTestRangeIteratorSeek)
}

func TestYogaDBMerge(t *testing.T) {
	run(t, &storetest.TestMergeCounter{}, storetest.CommonTestMerge)
}
