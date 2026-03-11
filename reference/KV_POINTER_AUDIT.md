# KV Pointer Refactor: Correctness Audit

## What Changed
All `KV` pass-by-value changed to `*KV` pass-by-pointer throughout yogadb:
- `BTreeG[KV]` -> `BTreeG[*KV]` (memtable, transaction write buffer, snapshots)
- `[]KV` -> `[]*KV` (interval cache, slotted pages, decode results)
- All function signatures: `kvLess`, `kvSizeApprox`, `isTombstone`, `kv128Encode/Decode`,
  `putPassthrough`, `resolveVPtr`, `btreeSeekGE/LE`, `flexSpaceSeekGE/LE`, etc.

## The Core Risk: Aliasing
With pass-by-value, every recipient gets an independent copy. With `*KV`, multiple
data structures can hold pointers to the same KV object. If one mutates the object,
all others see corrupted data. This is the class of bugs we audited for.

## Audit Process
Three independent analysis agents examined:
1. Btree aliasing (memtable, transactions, merged seeks)
2. Interval cache aliasing (cache entries, prefetch spans, VacuumVLOG)
3. Encode/decode paths (nil safety, split operations, Batch.Commit)

I then manually verified each finding against the actual code, especially where
agents disagreed.

---

## Initial Agent Findings (Some Were Wrong)

Agent 1 flagged **5 "CRITICAL" bugs**. After manual verification:

| Agent Finding | Verdict | Why |
|---|---|---|
| "Batch.Commit corrupts btree by mutating KV after storage" | **FALSE ALARM** | VLOG mutation (line 175) happens BEFORE btree insert (line 227). Btree gets the already-modified pointer. |
| "flushMemtable passes btree pointers to cache = use-after-free" | **FALSE ALARM** | Go GC keeps KV alive. Neither btree nor cache modifies the KV. bt.Clear() just drops the btree's reference. |
| "btreeSeekGE returns stale btree pointer" | **FALSE ALARM** | Callers read fields under topMutRW.RLock(), copy key/value via dupBytes(). No mutation. |
| "bt.Copy() shares *KV = snapshot corruption" | **FALSE ALARM** | COW copies node structure, shares pointers. But all writes create fresh &KV{}, never mutate stored ones. |
| "Prefetch spans hold stale pointers after cache eviction" | **Semantic change** (actually more correct, see below) |

Agent 2 flagged **4 issues**. After verification:

| Agent Finding | Verdict | Why |
|---|---|---|
| "VacuumVLOG mutates cache KV" | **Safe** | Runs under exclusive topMutRW.Lock(). Memtables already cleared. No concurrent readers. |
| "cacheEntryReplace leaves dangling old *KV" | **Safe** | Old KV stays alive via GC. Iterator's prefetch span keeps its own copy of the pointer. |
| "Prefetch spans = CRITICAL unsynchronized access" | **Safe** | Same as old behavior - spans always reference cache memory without locks. HLC check detects mutations. |
| "flushMemtable → cache = CRITICAL shared reference" | **Safe** | KVs are immutable once stored. No mutation window. |

Agent 3 found **no bugs** (all paths safe). Its analysis was the most accurate.

---

## Final Verdict: NO BUGS FOUND

### The Key Invariant
**KV objects are effectively immutable once stored in any data structure.**

Every code path that "modifies" a KV actually creates a fresh `&KV{...}`:
- `writeLockHeldPut`: `kv := &KV{Key: key, Value: value, Hlc: hlcVal}`
- `Batch.Set`: `&KV{Key: append([]byte{}, key...), Value: append([]byte{}, value...)}`
- `kv128Decode`: `kv = &KV{}`
- `slottedPageDecode`: `kvs[i] = &KV{}`
- `cacheEntryReplace`: stores a new `*KV`, doesn't modify the old one

The only exception is `VacuumVLOG` which mutates `fce.kvs[i].Vptr`, but it holds
an exclusive lock with no other references to those KVs.

### One Semantic Change (Improvement)
**Prefetch spans** (iter.go): Previously `[]KV` meant spans had a window into
the cache's contiguous memory. A `cacheEntryReplace` would modify that memory
and the iterator could see the new value mid-iteration. Now `[]*KV` means the
span holds its own copy of the pointer - iterator sees a frozen snapshot.
This is **more correct** behavior.

### Optional Hardening (Not Required)
- Could add nil-element assertions in slottedPageEncode for defense-in-depth
- Could add a defensive copy in flushMemtable to prevent future bugs if someone
  adds KV mutation code in that window

---

## Verification Commands
```bash
go test -count=1 -timeout=300s -race ./...   # race detector
go test -count=5 -timeout=600s ./...          # repeated runs for flaky bugs
```
