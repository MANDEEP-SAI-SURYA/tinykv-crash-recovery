#!/bin/bash
# Reference solution: crash-consistent (including directory durability),
# snapshot-correct engine with streaming, bounded-memory table reads, writes
# and merges. fs.py is left untouched.
set -euo pipefail
cd /app/tinykv

cat > __init__.py <<'TINYKV_EOF'
"""tinykv - a tiny embedded multi-version log-structured key-value store."""

from .db import DB, Options, Snapshot
from .encoding import CorruptionError

__all__ = ["DB", "Options", "Snapshot", "CorruptionError"]
TINYKV_EOF

cat > encoding.py <<'TINYKV_EOF'
"""Low level encoding helpers shared by the WAL and the SSTable code."""

import zlib


class CorruptionError(Exception):
    """On-disk data is malformed."""


def put_varint(n):
    """Encode a non-negative int as unsigned LEB128."""
    if n < 0:
        raise ValueError("varint must be non-negative")
    out = bytearray()
    while n >= 0x80:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


def get_varint(buf, pos):
    """Decode an unsigned LEB128 value at ``pos``; returns (value, new_pos)."""
    result = 0
    shift = 0
    while True:
        if pos >= len(buf):
            raise CorruptionError("truncated varint")
        if shift > 63:
            raise CorruptionError("varint too long")
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if b < 0x80:
            return result, pos
        shift += 7


def get_bytes(buf, pos):
    """Decode a varint length followed by that many bytes."""
    n, pos = get_varint(buf, pos)
    end = pos + n
    if end > len(buf):
        raise CorruptionError("truncated byte string")
    return bytes(buf[pos:end]), end


def crc32(data):
    return zlib.crc32(data) & 0xFFFFFFFF
TINYKV_EOF

cat > wal.py <<'TINYKV_EOF'
"""Write-ahead log.  See FORMAT.md for the record layout."""

import struct

from . import fs
from .encoding import CorruptionError, crc32, get_bytes, get_varint, put_varint

PUT = 1
DEL = 2
BATCH_BEGIN = 3
BATCH_COMMIT = 4

HEADER = struct.Struct("<II")  # crc32(body), len(body)


def _record(body):
    return HEADER.pack(crc32(body), len(body)) + body


def _op_body(seq, op):
    kind = op[0]
    key = op[1]
    if kind == "put":
        val = op[2]
        return (bytes([PUT]) + put_varint(seq) + put_varint(len(key)) + key
                + put_varint(len(val)) + val)
    if kind == "del":
        return bytes([DEL]) + put_varint(seq) + put_varint(len(key)) + key
    raise ValueError("unknown op %r" % (kind,))


def encode_ops(ops, first_seq):
    """Encode ops numbered first_seq, first_seq+1, ...  More than one op is
    wrapped in a batch."""
    if len(ops) == 1:
        return _record(_op_body(first_seq, ops[0]))
    parts = [_record(bytes([BATCH_BEGIN]) + put_varint(len(ops)))]
    parts.extend(_record(_op_body(first_seq + i, op)) for i, op in enumerate(ops))
    parts.append(_record(bytes([BATCH_COMMIT])))
    return b"".join(parts)


def _parse_body(body):
    if not body:
        raise CorruptionError("empty record")
    typ = body[0]
    pos = 1
    if typ == PUT:
        seq, pos = get_varint(body, pos)
        key, pos = get_bytes(body, pos)
        val, pos = get_bytes(body, pos)
        rec = (PUT, (seq, ("put", key, val)))
    elif typ == DEL:
        seq, pos = get_varint(body, pos)
        key, pos = get_bytes(body, pos)
        rec = (DEL, (seq, ("del", key)))
    elif typ == BATCH_BEGIN:
        count, pos = get_varint(body, pos)
        rec = (BATCH_BEGIN, count)
    elif typ == BATCH_COMMIT:
        rec = (BATCH_COMMIT, None)
    else:
        raise CorruptionError("unknown record type %d" % typ)
    if pos != len(body):
        raise CorruptionError("trailing bytes in record")
    return rec


def replay(data):
    """Parse a WAL image.

    Returns ``(entries, valid_len)``: the (seq, op) pairs to apply, in order,
    and the length of the prefix that holds only complete, valid, committed
    records.  Any bytes after ``valid_len`` (a torn/corrupt tail or an
    uncommitted batch) must be discarded.
    """
    entries = []
    pos = 0
    valid = 0
    pending = None  # (expected_count, [entries]) while inside a batch
    n = len(data)
    while pos + HEADER.size <= n:
        crc, length = HEADER.unpack_from(data, pos)
        end = pos + HEADER.size + length
        if end > n:
            break
        body = data[pos + HEADER.size:end]
        if crc32(body) != crc:
            break
        try:
            typ, val = _parse_body(body)
        except CorruptionError:
            break
        if pending is None:
            if typ in (PUT, DEL):
                entries.append(val)
                valid = end
            elif typ == BATCH_BEGIN:
                pending = (val, [])
            else:
                break
        else:
            if typ in (PUT, DEL):
                pending[1].append(val)
            elif typ == BATCH_COMMIT and len(pending[1]) == pending[0]:
                entries.extend(pending[1])
                pending = None
                valid = end
            else:
                break
        pos = end
    return entries, valid


class WalWriter:
    def __init__(self, path, number):
        self.path = path
        self.number = number

    def add(self, ops, first_seq):
        fs.append(self.path, encode_ops(ops, first_seq))
        fs.fsync(self.path)
TINYKV_EOF

cat > sstable.py <<'TINYKV_EOF'
"""Immutable sorted tables.  See FORMAT.md for the file layout.

Tables are written and read incrementally: the writer streams data to the
file in chunks, and the reader keeps only the footer and the index in memory
and reads one data block at a time.
"""

import bisect
import os
import struct
import zlib

from . import fs
from .encoding import CorruptionError, crc32, get_bytes, get_varint, put_varint

MAGIC = b"TKV3"
# index_offset, index_count, entry_count, max_seq, crc32(index), magic
FOOTER = struct.Struct("<QIIQI4s")
IENTRY = struct.Struct("<QI")      # block offset, crc32(block)
INDEX_INTERVAL = 16
CHUNK = 64 * 1024

FLAG_VALUE = 0
FLAG_TOMBSTONE = 1

# Returned by SSTReader.get when the table has no visible entry for the key.
MISSING = object()


def encode_entry(key, seq, val):
    if val is None:
        return (bytes([FLAG_TOMBSTONE]) + put_varint(len(key)) + key
                + put_varint(seq))
    return (bytes([FLAG_VALUE]) + put_varint(len(key)) + key
            + put_varint(seq) + put_varint(len(val)) + val)


class SSTWriter:
    """Streams (key, seq, value-or-None) entries, in (key asc, seq desc)
    order, into a new table.  The file is created on the first entry, so a
    writer that receives no entries leaves nothing behind."""

    def __init__(self, path):
        self.path = path
        self.count = 0
        self.max_seq = 0
        self._prev = None
        self._buf = bytearray()
        self._data_len = 0
        self._index = bytearray()
        self._icount = 0
        self._block_key = None
        self._block_off = 0
        self._block_crc = 0

    def add(self, key, seq, val):
        cur = (key, -seq)
        if self._prev is not None and cur <= self._prev:
            raise ValueError("sstable entries out of order")
        self._prev = cur
        if self.count == 0:
            fs.create(self.path)
        if self.count % INDEX_INTERVAL == 0:
            self._end_block()
            self._block_key = key
            self._block_off = self._data_len
            self._block_crc = 0
        e = encode_entry(key, seq, val)
        self._block_crc = crc32_update(e, self._block_crc)
        self._buf += e
        self._data_len += len(e)
        self.count += 1
        if seq > self.max_seq:
            self.max_seq = seq
        if len(self._buf) >= CHUNK:
            self._drain()

    def _end_block(self):
        if self._block_key is not None:
            self._index += (put_varint(len(self._block_key)) + self._block_key
                            + IENTRY.pack(self._block_off, self._block_crc))
            self._icount += 1
            self._block_key = None

    def _drain(self):
        if self._buf:
            fs.append(self.path, bytes(self._buf))
            self._buf = bytearray()

    def finish(self):
        """Write index and footer and make the table durable.  Returns the
        number of entries written (0 means no file was created)."""
        if self.count == 0:
            return 0
        self._end_block()
        index = bytes(self._index)
        self._buf += index + FOOTER.pack(self._data_len, self._icount, self.count,
                                         self.max_seq, crc32(index), MAGIC)
        self._drain()
        fs.fsync(self.path)
        fs.fsync_dir(os.path.dirname(self.path))
        return self.count


def crc32_update(data, crc):
    return zlib.crc32(data, crc) & 0xFFFFFFFF


def _parse_entry(buf, pos):
    flag = buf[pos]
    key, pos = get_bytes(buf, pos + 1)
    seq, pos = get_varint(buf, pos)
    if flag == FLAG_TOMBSTONE:
        return key, seq, None, pos
    if flag != FLAG_VALUE:
        raise CorruptionError("bad entry flag")
    val, pos = get_bytes(buf, pos)
    return key, seq, val, pos


class SSTReader:
    def __init__(self, path):
        self.path = path
        size = fs.size(path)
        if size < FOOTER.size:
            raise CorruptionError("%s: too short" % path)
        footer = fs.read_at(path, size - FOOTER.size, FOOTER.size)
        index_off, icount, ecount, max_seq, icrc, magic = FOOTER.unpack(footer)
        index_end = size - FOOTER.size
        if magic != MAGIC or index_off > index_end:
            raise CorruptionError("%s: bad footer" % path)
        index = fs.read_at(path, index_off, index_end - index_off)
        if crc32(index) != icrc:
            raise CorruptionError("%s: index checksum mismatch" % path)
        self.entry_count = ecount
        self.max_seq = max_seq
        self.index_keys = []
        self.block_offs = []
        self.block_crcs = []
        pos = 0
        for _ in range(icount):
            key, pos = get_bytes(index, pos)
            off, bcrc = IENTRY.unpack_from(index, pos)
            pos += IENTRY.size
            self.index_keys.append(key)
            self.block_offs.append(off)
            self.block_crcs.append(bcrc)
        if pos != len(index):
            raise CorruptionError("%s: bad index" % path)
        self.block_ends = self.block_offs[1:] + [index_off]

    def _block(self, i):
        off = self.block_offs[i]
        data = fs.read_at(self.path, off, self.block_ends[i] - off)
        if crc32(data) != self.block_crcs[i]:
            raise CorruptionError("%s: block %d checksum mismatch" % (self.path, i))
        return data

    def _entries(self, i):
        data = self._block(i)
        pos = 0
        while pos < len(data):
            k, seq, v, pos = _parse_entry(data, pos)
            yield k, seq, v

    def get(self, key, max_seq):
        """Newest version of ``key`` with seq <= max_seq: its value, None for
        a tombstone, or MISSING if the table has no such version."""
        n = len(self.block_offs)
        # Versions of ``key`` may begin in the block before the first block
        # whose first key is >= key.
        i = max(bisect.bisect_left(self.index_keys, key) - 1, 0)
        for b in range(i, n):
            if b > i and self.index_keys[b] > key:
                break
            for k, seq, v in self._entries(b):
                if k < key:
                    continue
                if k > key:
                    return MISSING
                if seq <= max_seq:
                    return v
        return MISSING

    def items(self):
        """All entries, in order, reading one block at a time."""
        for b in range(len(self.block_offs)):
            yield from self._entries(b)
TINYKV_EOF

cat > manifest.py <<'TINYKV_EOF'
"""The MANIFEST names the live tables and the oldest live WAL."""

import json
import os

from . import fs
from .encoding import CorruptionError, crc32

NAME = "MANIFEST"
TMP_NAME = "MANIFEST.tmp"


def encode(state):
    line = json.dumps(state, sort_keys=True, separators=(",", ":"))
    raw = line.encode()
    return raw + b"\n" + (b"%08x\n" % crc32(raw))


def decode(data):
    parts = data.split(b"\n")
    if len(parts) != 3 or parts[2] != b"":
        raise CorruptionError("malformed MANIFEST")
    raw, crc_hex = parts[0], parts[1]
    try:
        crc = int(crc_hex, 16)
    except ValueError:
        raise CorruptionError("malformed MANIFEST checksum")
    if crc32(raw) != crc:
        raise CorruptionError("MANIFEST checksum mismatch")
    state = json.loads(raw)
    if state.get("version") != 3:
        raise CorruptionError("unsupported MANIFEST version")
    return state


def read(dirpath):
    path = os.path.join(dirpath, NAME)
    if not fs.exists(path):
        return None
    return decode(fs.read(path))


def write(dirpath, state):
    """Atomically replace the MANIFEST with ``state``."""
    tmp = os.path.join(dirpath, TMP_NAME)
    if fs.exists(tmp):
        fs.unlink(tmp)
    fs.create(tmp)
    fs.append(tmp, encode(state))
    fs.fsync(tmp)
    fs.rename(tmp, os.path.join(dirpath, NAME))
    fs.fsync_dir(dirpath)
TINYKV_EOF

cat > db.py <<'TINYKV_EOF'
"""tinykv: a tiny multi-version log-structured key-value store."""

import heapq
import itertools
import os
import re

from . import fs, manifest
from .encoding import CorruptionError
from .sstable import MISSING, SSTReader, SSTWriter
from .wal import WalWriter, replay

_WAL_RE = re.compile(r"^wal-(\d+)\.log$")
_SST_RE = re.compile(r"^sst-(\d+)\.sst$")


def wal_name(n):
    return "wal-%d.log" % n


def sst_name(n):
    return "sst-%d.sst" % n


class Options:
    def __init__(self, memtable_limit=64 * 1024, max_ssts=4, merge_width=3):
        if merge_width < 2:
            raise ValueError("merge_width must be >= 2")
        if max_ssts < 1:
            raise ValueError("max_ssts must be >= 1")
        self.memtable_limit = memtable_limit
        self.max_ssts = max_ssts
        self.merge_width = merge_width


class Snapshot:
    """A consistent read view as of sequence number ``seq``."""

    def __init__(self, db, seq):
        self._db = db
        self.seq = seq

    def release(self):
        self._db.release(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.release()


def _check_key(key):
    if not isinstance(key, bytes) or not key:
        raise TypeError("keys must be non-empty bytes")


def _check_value(val):
    if not isinstance(val, bytes):
        raise TypeError("values must be bytes")


class DB:
    def __init__(self, path, options=None):
        self.path = os.path.abspath(path)
        self.opts = options or Options()
        self.closed = False
        self._readers = {}
        self._snapshots = []
        self.memtable = {}       # key -> [(seq, value-or-None)], ascending seq
        self.mem_bytes = 0
        self.last_seq = 0
        self._open()

    # ------------------------------------------------------------ helpers
    def _p(self, name):
        return os.path.join(self.path, name)

    def _alloc(self):
        n = self.next_file
        self.next_file += 1
        return n

    def _state(self):
        return {
            "version": 3,
            "log_number": self.log_number,
            "next_file": self.next_file,
            "last_seq": self.last_seq,
            "ssts": list(self.ssts),
        }

    def _save_manifest(self):
        manifest.write(self.path, self._state())

    def _reader(self, sid):
        r = self._readers.get(sid)
        if r is None:
            r = self._readers[sid] = SSTReader(self._p(sst_name(sid)))
        return r

    def _apply(self, seq, op):
        key = op[1]
        val = op[2] if op[0] == "put" else None
        self.memtable.setdefault(key, []).append((seq, val))
        self.mem_bytes += len(key) + (len(val) if val is not None else 0)

    # ------------------------------------------------------------ open
    def _open(self):
        fs.ensure_dir(self.path)
        state = manifest.read(self.path)
        if state is None:
            # Fresh database.  Nothing can have been acknowledged before the
            # first MANIFEST became durable, so leftovers are garbage.
            for name in fs.listdir(self.path):
                if (_WAL_RE.match(name) or _SST_RE.match(name)
                        or name == manifest.TMP_NAME):
                    fs.unlink(self._p(name))
            state = {"version": 3, "log_number": 1, "next_file": 1,
                     "last_seq": 0, "ssts": []}
            manifest.write(self.path, state)

        self.log_number = state["log_number"]
        self.next_file = state["next_file"]
        self.ssts = sorted(int(x) for x in state["ssts"])
        live = set(self.ssts)

        wal_nums = []
        max_seen = 0
        for name in fs.listdir(self.path):
            m = _WAL_RE.match(name) or _SST_RE.match(name)
            if not m:
                continue
            num = int(m.group(1))
            max_seen = max(max_seen, num)
            if name.startswith("wal-"):
                if num >= self.log_number:
                    wal_nums.append(num)
                else:
                    fs.unlink(self._p(name))          # obsolete log
            elif num not in live:
                fs.unlink(self._p(name))              # orphaned table
        self.next_file = max(self.next_file, max_seen + 1)

        last = state["last_seq"]
        for sid in self.ssts:
            if not fs.exists(self._p(sst_name(sid))):
                raise CorruptionError("missing table %s" % sst_name(sid))
            last = max(last, self._reader(sid).max_seq)

        wal_nums.sort()
        for num in wal_nums:
            path = self._p(wal_name(num))
            data = fs.read(path)
            entries, valid = replay(data)
            for seq, op in entries:
                self._apply(seq, op)
                last = max(last, seq)
            if valid < len(data):
                fs.truncate(path, valid)
        self.last_seq = last
        self.live_wals = wal_nums
        if wal_nums:
            num = wal_nums[-1]
        else:
            num = self._alloc()
            fs.create(self._p(wal_name(num)))
            fs.fsync_dir(self.path)
            self.live_wals = [num]
        self.wal = WalWriter(self._p(wal_name(num)), num)

    # ------------------------------------------------------------ writes
    def _check_open(self):
        if self.closed:
            raise RuntimeError("database is closed")

    def _write(self, ops):
        self._check_open()
        first = self.last_seq + 1
        self.wal.add(ops, first)
        for i, op in enumerate(ops):
            self._apply(first + i, op)
        self.last_seq = first + len(ops) - 1
        if self.mem_bytes >= self.opts.memtable_limit:
            self._flush()

    def put(self, key, value):
        _check_key(key)
        _check_value(value)
        self._write([("put", key, value)])

    def delete(self, key):
        _check_key(key)
        self._write([("del", key)])

    def write_batch(self, ops):
        """Atomically apply a list of ("put", k, v) / ("del", k) tuples."""
        ops = [tuple(op) for op in ops]
        if not ops:
            return
        for op in ops:
            if op[0] == "put" and len(op) == 3:
                _check_key(op[1])
                _check_value(op[2])
            elif op[0] == "del" and len(op) == 2:
                _check_key(op[1])
            else:
                raise ValueError("bad batch op %r" % (op,))
        self._write(ops)

    # ------------------------------------------------------------ snapshots
    def snapshot(self):
        self._check_open()
        snap = Snapshot(self, self.last_seq)
        self._snapshots.append(snap)
        return snap

    def release(self, snap):
        for i, s in enumerate(self._snapshots):
            if s is snap:
                del self._snapshots[i]
                return

    def _snap_seq(self, snapshot):
        if snapshot is None:
            return self.last_seq
        if not any(s is snapshot for s in self._snapshots):
            raise ValueError("snapshot is not live in this database")
        return snapshot.seq

    # ------------------------------------------------------------ reads
    def get(self, key, snapshot=None):
        self._check_open()
        _check_key(key)
        limit = self._snap_seq(snapshot)
        for seq, val in reversed(self.memtable.get(key, ())):
            if seq <= limit:
                return val
        for sid in reversed(self.ssts):
            v = self._reader(sid).get(key, limit)
            if v is not MISSING:
                return v
        return None

    # ------------------------------------------------------------ retention
    def _gc(self, entries, bottommost):
        """Stream the retained versions out of ``entries`` -- (key, seq, val)
        in (key asc, seq desc) order.

        A version is kept iff it is the newest one with seq <= S for some S in
        {live snapshot seqs} + {infinity}.  At the bottom of the tree, each
        key's oldest kept versions are dropped while they are tombstones.
        Memory use is O(number of live snapshots), independent of how many
        versions a key has."""
        points = sorted({s.seq for s in self._snapshots}, reverse=True)
        for key, group in itertools.groupby(entries, key=lambda e: e[0]):
            pending = []          # kept tombstones not yet followed by a value
            prev_seq = None
            pi = 0
            for _, seq, val in group:
                if prev_seq is None:
                    keep = True
                else:
                    while pi < len(points) and points[pi] >= prev_seq:
                        pi += 1
                    keep = pi < len(points) and points[pi] >= seq
                prev_seq = seq
                if not keep:
                    continue
                if val is None and bottommost:
                    pending.append(seq)
                    continue
                for tseq in pending:
                    yield key, tseq, None
                pending = []
                yield key, seq, val

    def _write_table(self, entries):
        """Write a new table from a sorted entry stream; returns its number,
        or None if the stream was empty."""
        sid = self.next_file
        w = SSTWriter(self._p(sst_name(sid)))
        for k, seq, v in entries:
            if w.count == 0:
                self._alloc()
            w.add(k, seq, v)
        return sid if w.finish() else None

    # ------------------------------------------------------------ flush
    def flush(self):
        self._check_open()
        self._flush()

    def _flush(self):
        if not self.memtable:
            return
        new_wal = self._alloc()
        fs.create(self._p(wal_name(new_wal)))
        imm = self.memtable
        self.memtable = {}
        self.mem_bytes = 0
        old_wals = self.live_wals
        self.wal = WalWriter(self._p(wal_name(new_wal)), new_wal)
        self.live_wals = [new_wal]

        entries = ((k, seq, v) for k in sorted(imm) for seq, v in reversed(imm[k]))
        sid = self._write_table(self._gc(entries, bottommost=not self.ssts))
        if sid is not None:
            self.ssts.append(sid)
        self.log_number = new_wal
        self._save_manifest()
        for num in old_wals:
            fs.unlink(self._p(wal_name(num)))
        self._maybe_compact()

    # ------------------------------------------------------------ compaction
    def _maybe_compact(self):
        while len(self.ssts) > self.opts.max_ssts:
            self._merge(self.ssts[-self.opts.merge_width:])

    def compact(self):
        """Flush, then merge every table into one."""
        self._check_open()
        self._flush()
        if self.ssts:
            self._merge(list(self.ssts))

    def _merge(self, inputs):
        bottommost = inputs[0] == self.ssts[0]
        streams = [self._reader(sid).items() for sid in inputs]
        merged = heapq.merge(*streams, key=lambda e: (e[0], -e[1]))
        out = self._write_table(self._gc(merged, bottommost))
        keep = [s for s in self.ssts if s not in inputs]
        if out is not None:
            keep.append(out)
        self.ssts = keep
        self._save_manifest()
        for sid in inputs:
            self._readers.pop(sid, None)
            fs.unlink(self._p(sst_name(sid)))

    # ------------------------------------------------------------ misc
    def close(self):
        self.closed = True
        self._snapshots = []
        self._readers.clear()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
TINYKV_EOF

find /app -name "__pycache__" -type d -prune -exec rm -rf {} +
python -c "import sys; sys.path.insert(0, '/app'); import tinykv; print('tinykv patched')"
