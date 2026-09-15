// Package yogableve adapts YogaDB's FlexDB to Bleve's upside_down KV store API.
//
// The package registers itself with Bleve as the "yogadb" KV store, so it can
// be selected through bleve.NewUsing:
//
//	index, err := bleve.NewUsing(path, mapping, upsidedown.Name, yogableve.Name, map[string]interface{}{
//		"disableBackgroundFlush": true,
//	})
//
// It can also wrap a caller-owned FlexDB directly:
//
//	db, err := yogadb.OpenFlexDB(path, nil)
//	if err != nil {
//		return err
//	}
//	db.AllowReads()
//	defer db.Close()
//
//	store := yogableve.NewStore(db, "articles", mergeOperator)
//
// Bleve keys are stored under a namespace-specific prefix. The raw Bleve key
// bytes are appended directly after that prefix, preserving bytewise ordering
// for prefix and range iterators.
package yogableve
