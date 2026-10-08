`/app/tinykv` is a small embedded multi-version log-structured key-value store (pure Python, standard library only) with snapshot reads. Its on-disk format, including how versions and sequence numbers work, is specified in `/app/FORMAT.md`. All of its file I/O goes through `/app/tinykv/fs.py`, which also implements a simulated power-failure model. Read the docstring of `fs.py` for the exact durability and crash semantics. You are graded against that model.

The engine is unreliable in production, and it cannot cope with large databases: memory use and I/O grow with the size of the data. Change it so that it meets the contract below. The verifier uses only the public API, the durability model and accounting of `tinykv.fs`, Python's `tracemalloc`, and its own independent reader for `FORMAT.md`.

## Public API (keep it unchanged)

- `tinykv.Options(memtable_limit=..., max_ssts=..., merge_width=...)`
- `tinykv.DB(path, options=None)` opens a database directory, creating it if it does not exist.
- `put(key, value)`, `delete(key)`, `flush()`, `compact()`, `close()`.
- `write_batch(ops)`, where `ops` is a list of `("put", key, value)` / `("del", key)` tuples applied in order.
- `get(key, snapshot=None)` returns the value, or `None` if the key is absent. It reads the latest state, or the state as of `snapshot`.
- `snapshot()` returns a `Snapshot` whose `seq` attribute is the sequence number of the last write acknowledged before it was taken (0 if there is none). `Snapshot.release()` and `DB.release(snapshot)` release it.
  - A snapshot is live from `snapshot()` until it is released or the database is closed.
  - Snapshots are not persisted.
- Keys are non-empty `bytes`. Values are `bytes` and may be empty or several MiB.

Use is single-process and single-threaded. `close()` does not need to flush.

## Required behaviour

Here, a "crash" means a power failure before any mutating file operation, with any outcome that the durability model in the `fs.py` docstring allows. A "restart" means calling `fs.reset()` and constructing a new `DB` on the same directory.

The crash simulator in the shipped `fs.py` implements only part of that model: it never loses or reorders directory changes. The verifier installs an implementation of the complete model, with the same API, and checks recovery from every crash outcome it permits.

1. **Sequence numbers.** Writes are numbered exactly as described in `FORMAT.md`, starting from 1 in a new database. After a restart, numbering continues from the largest sequence number present in the directory.
2. **Snapshot reads.** While a snapshot is live, `get(key, snapshot=s)` returns the key's state as of `s.seq`. This holds through any later writes, flushes and compactions.
3. **Durability.** Once `put`, `delete` or `write_batch` returns, its effect survives any later crash.
4. **Atomicity.** A `write_batch` is visible after a restart either completely or not at all.
5. **Prefix consistency.** Suppose a crash happens at any point: while opening, during a write, or during a flush, compaction or recovery. A restart must then succeed without raising. Afterwards, `get` must show exactly the effects of every acknowledged operation, with or without the single operation that was in progress.
   - The database must then keep working: new writes, snapshots, flushes and compactions must behave correctly across further restarts.
   - Crashing again during recovery must not change which state is recovered.
6. **No resurrection.** A deleted key never becomes visible again unless it is written again.
7. **Version retention.** Every table that a flush or merge writes holds exactly the following versions:
   - It considers the versions it consumes, key by key. It keeps a version if and only if that version is the newest one with `seq <= S` for at least one `S`, where `S` ranges over the seqs of the live snapshots plus "infinity" (the latest state).
   - If no live table older than the output remains, it then repeatedly drops each key's oldest kept version while that version is a tombstone.
8. **Format.** Directories must follow `FORMAT.md`.
   - Any directory that conforms to it must open correctly. That includes directories written by other implementations and directories containing the leftovers a crash can produce.
   - After `close()`, an independent reader of `FORMAT.md` must see exactly the database's latest contents in the directory, and no live log may contain an invalid tail.
9. **Shape.**
   - The memtable's size is the sum of `len(key) + len(value)` over every write it holds, with a delete counting as its key length. When that size reaches `memtable_limit`, the memtable is flushed to a new table. A flush that keeps no versions writes no table.
   - After each flush, while more than `max_ssts` tables are live, the `merge_width` newest tables are merged into at most one table.
   - `compact()` flushes and then merges all live tables into at most one.
10. **Resource bounds.** Reads are counted by `fs.stats()["read_bytes"]`.
    - **Open:** constructing a `DB` reads at most the `MANIFEST`, the live logs, and the footer and index of each live table.
    - **Point reads:** `get` reads only data blocks from each table it consults. From each one it reads at most:
      - the last block whose first key is less than the key;
      - the first block whose first key is greater than or equal to the key;
      - any further blocks whose first key equals the key.
    - **Memory:** `tracemalloc` measures peak memory allocated during the call, relative to the start of the call, not counting the memtable's own data. During `compact()`, and during any call that runs an automatic merge, that peak stays below `2 MiB + (n + 2) × B + 1 KiB × L`:
      - `n` is the number of tables the call reads or writes;
      - `B` is the size of the largest data block among them;
      - `L` is the number of live snapshots.

      This must hold however large the tables are and however many versions a key has.
11. **I/O discipline.**
    - Every creation, write, rename, truncation and deletion of a file in a database directory must go through `tinykv.fs`.
    - Do not modify `fs.py`. The verifier replaces it with its complete implementation of the same model before testing.
    - Use only the Python standard library.
