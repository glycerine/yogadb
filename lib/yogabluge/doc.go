// Package yogabluge adapts YogaDB's FlexDB to Bluge's index.Directory.
//
// The caller owns the FlexDB lifecycle:
//
//	db, err := yogadb.OpenFlexDB(path, nil)
//	if err != nil {
//		return err
//	}
//	db.AllowReads()
//	defer db.Close()
//
//	config := yogabluge.NewConfig(db, "articles")
//	writer, err := bluge.OpenWriter(config)
//
// Bluge segment and snapshot objects are stored as YogaDB keys under a
// namespace-specific prefix. The initial implementation stores each Bluge item
// as one YogaDB value.
package yogabluge
