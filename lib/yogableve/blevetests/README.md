# Vendored Bleve Tests

This directory vendors focused Bleve test suites used to exercise yogableve as a
YogaDB-backed Bleve `upsidedown` KV store.

- `kvstore` runs Bleve's generic `upsidedown_store_api/test` KVStore contract
  suite against `yogableve`.
- `integration` vendors Bleve's JSON fixture integration suite and defaults it
  to `upsidedown` plus `yogableve`.
- `versus` vendors Bleve's randomized versus suite and compares
  `upsidedown/boltdb` against `upsidedown/yogableve`.

The upstream Bleve and upsidedown_store_api licenses are copied under
`licenses/`. Source files retain their original Apache 2.0 headers where the
upstream files had them. Local changes are intentionally limited to package
names, temp-directory handling, backend selection, and one default skip for the
`geoshapes` fixture when running against `upsidedown`, because Bleve's
`upsidedown` indexer does not implement geoshape search.
