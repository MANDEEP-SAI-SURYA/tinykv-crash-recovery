# tinykv on-disk format (version 3)

Other tools (backup, inspection, replication) read and write this format, so
it must not change.  All fixed-width integers are little-endian.  `varint` is
unsigned LEB128 (7 bits per byte, least-significant group first, high bit set
on every byte except the last).  `crc32` is zlib's CRC-32.

## Versions and sequence numbers

Every write of a key creates a new *version* of it.  Each put or delete
consumes the next sequence number (`seq`); a batch of n ops consumes n
consecutive numbers in op order.  The first write to a new database has
seq 1.  The latest state of a key, and its state as seen at sequence number
S, is given by its version with the largest seq (respectively the largest seq
<= S); a tombstone version means "absent".  No two versions of one key share a
seq.

A database is a directory containing:

| file           | purpose                                             |
|----------------|-----------------------------------------------------|
| `MANIFEST`     | which tables are live, which logs are live          |
| `MANIFEST.tmp` | scratch file used while replacing `MANIFEST`        |
| `wal-<N>.log`  | write-ahead logs                                    |
| `sst-<N>.sst`  | sorted string tables                                |

`<N>` is a positive decimal integer without padding (`wal-7.log`,
`sst-12.sst`).  WAL and SST numbers are allocated from one shared counter
and a number is never reused for a different file.

## MANIFEST

Two lines, each terminated by `\n`:

1. compact JSON (`sort_keys`, separators `,` and `:`) of
   `{"last_seq": Q, "log_number": L, "next_file": F, "ssts": [ids...], "version": 3}`
2. the crc32 of line 1 (without its newline) as 8 lowercase hex digits.

* `ssts` lists the live table numbers.  A table with a larger number is
  newer: every version it holds has a larger seq than any version of the
  same key in a table with a smaller number.
* Every `wal-<N>.log` with `N >= log_number` is live and must be replayed on
  open, in increasing numeric order.  Logs with `N < log_number` are
  obsolete.  Versions in live logs are newer than every version in a table.
* `next_file` is a lower bound for the next number to allocate.  Files with
  numbers `>= next_file` may exist after a crash.
* `last_seq` is a lower bound for the largest seq ever used.  Larger seqs may
  exist in live tables and logs.
* Any `sst-<N>.sst` not listed in `ssts` is garbage left by an interrupted
  operation and must be ignored.

## WAL

A sequence of records:

```
record := crc32(body) : u32 | len(body) : u32 | body
body   := 0x01 varint(seq) varint(len k) k varint(len v) v       PUT
        | 0x02 varint(seq) varint(len k) k                       DELETE
        | 0x03 varint(count)                                     BATCH_BEGIN
        | 0x04                                                   BATCH_COMMIT
```

A single `put`/`delete` is one PUT/DELETE record.  `write_batch` with more
than one op is written as `BATCH_BEGIN(count)`, `count` PUT/DELETE records,
`BATCH_COMMIT`; the ops of a batch take effect together or not at all.

Replay reads records in order and stops at the first record that is
incomplete, fails its checksum, or does not parse.  A batch whose
`BATCH_COMMIT` does not appear before that point is not applied.  Everything
after the last applied record is the *tail* of the log; a tail may only be
the result of a crash, and new records must never be appended after one.

## SST

```
data    := block*
block   := entry{1..16}            entries 0-15, 16-31, ... of the table
entry   := 0x00 varint(len k) k varint(seq) varint(len v) v     value
         | 0x01 varint(len k) k varint(seq)                     tombstone
index   := ientry*                 one per block, in order
ientry  := varint(len k) k  offset : u64  crc32(block) : u32
                                   k = first key of the block,
                                   offset = position of the block in the file
footer  := index_offset : u64 | index_count : u32 | entry_count : u32
           | max_seq : u64 | crc32(index) : u32 | "TKV3"
file    := data index footer
```

Entries are sorted by key ascending, then seq descending.  A block runs from
its offset to the next block's offset (the last block ends at
`index_offset`).  `max_seq` is the largest seq of any entry in the table.  A
table may hold several versions of the same key, and those versions may be
split across blocks.  A table always holds at least one entry.
