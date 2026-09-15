//  Copyright (c) 2018 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package versus

import (
	"strconv"
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/index/upsidedown"
	"github.com/blevesearch/bleve/v2/index/upsidedown/store/boltdb"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/glycerine/yogadb/lib/yogableve"
)

func TestDisjunctionSearchScoreYogaDBMatchesBoltDB(t *testing.T) {
	boltHits := disjunctionQueryiOnIndexWithCompositeFields(upsidedown.Name, boltdb.Name, t)
	yogaHits := disjunctionQueryiOnIndexWithCompositeFields(upsidedown.Name, yogableve.Name, t)

	if boltHits[0].ID != yogaHits[0].ID || boltHits[1].ID != yogaHits[1].ID {
		t.Errorf("boltdb, yogadb returned different docs;\n"+
			"boltdb: (%s, %s), yogadb: (%s, %s)\n",
			boltHits[0].ID, boltHits[1].ID, yogaHits[0].ID, yogaHits[1].ID)
	}

	if boltHits[0].Score != yogaHits[0].Score || boltHits[1].Score != yogaHits[1].Score {
		t.Errorf("boltdb, yogadb showing different scores;\n"+
			"boltdb: (%+v, %+v), yogadb: (%+v, %+v)\n",
			*boltHits[0].Expl, *boltHits[1].Expl, *yogaHits[0].Expl, *yogaHits[1].Expl)
	}
}

func disjunctionQueryiOnIndexWithCompositeFields(indexName, kvStore string,
	t *testing.T,
) []*search.DocumentMatch {
	tmpIndexPath := t.TempDir()

	// create an index
	idxMapping := mapping.NewIndexMapping()
	idx, err := bleve.NewUsing(tmpIndexPath, idxMapping, indexName,
		kvStore, kvConfig(kvStore))
	if err != nil {
		t.Fatalf("NewUsing(%s, %s): %v", indexName, kvStore, err)
	}

	defer func() {
		err = idx.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	// create and insert documents as a batch
	batch := idx.NewBatch()
	docs := []struct {
		field1 string
		field2 int
	}{
		{
			field1: "one",
			field2: 1,
		},
		{
			field1: "two",
			field2: 2,
		},
	}

	for i := 0; i < len(docs); i++ {
		doc := document.NewDocument(strconv.Itoa(docs[i].field2))
		doc.Fields = []document.Field{
			document.NewTextField("field1", []uint64{}, []byte(docs[i].field1)),
			document.NewNumericField("field2", []uint64{}, float64(docs[i].field2)),
		}
		doc.CompositeFields = []*document.CompositeField{
			document.NewCompositeFieldWithIndexingOptions(
				"_all", true, []string{"field1"}, []string{},
				index.IndexField|index.IncludeTermVectors),
		}
		if err = batch.IndexAdvanced(doc); err != nil {
			t.Error(err)
		}
	}
	if err = idx.Batch(batch); err != nil {
		t.Error(err)
	}

	/*
		Query:
				 DISJ
			        /    \
			     CONJ    TERM(two)
			     /
		          TERM(one)
	*/

	tq1 := bleve.NewTermQuery("one")
	tq1.SetBoost(2)
	tq2 := bleve.NewTermQuery("two")
	tq2.SetBoost(3)

	cq := bleve.NewConjunctionQuery(tq1)
	cq.SetBoost(4)

	q := bleve.NewDisjunctionQuery(tq1, tq2)
	sr := bleve.NewSearchRequestOptions(q, 2, 0, true)
	res, err := idx.Search(sr)
	if err != nil {
		t.Error(err)
	}

	if len(res.Hits) != 2 {
		t.Errorf("indexType: %s Expected 2 hits, but got: %v", indexName, len(res.Hits))
	}

	return res.Hits
}
