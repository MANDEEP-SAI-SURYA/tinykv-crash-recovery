"""tinykv: a tiny multi-version log-structured key-value store."""

import os
import re

from . import fs, manifest
from .encoding import CorruptionError
from .sstable import MISSING, SSTReader, write_sst
from .wal import WalWriter, replay

_WAL_RE = re.compile(r"^wal-(\d+)\.log$")
_SST_RE = re.compile(r"^sst-(\d+)\.sst$")


def wal_name(n):
    return "wal-%d.log" % n


def sst_name(n):
    return "sst-%d.sst" % n


def _num(name):
    m = _WAL_RE.match(name) or _SST_RE.match(name)
    return int(m.group(1))


class Options:
    def __init__(self, memtable_limit=64 * 1024, max_ssts=4, merge_width=3,
                 wal_sync_every=8):
        if merge_width < 2:
            raise ValueError("merge_width must be >= 2")
        if max_ssts < 1:
            raise ValueError("max_ssts must be >= 1")
        self.memtable_limit = memtable_limit
        self.max_ssts = max_ssts
        self.merge_width = merge_width
        self.wal_sync_every = wal_sync_every


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
            "ssts": [_num(t) for t in self.tables],
        }

    def _save_manifest(self):
        manifest.write(self.path, self._state())

    def _reader(self, name):
        r = self._readers.get(name)
        if r is None:
            r = self._readers[name] = SSTReader(self._p(name))
        return r

    def _new_wal(self, num):
        return WalWriter(self._p(wal_name(num)), num,
                         sync_every=self.opts.wal_sync_every)

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
            state = {"version": 3, "log_number": 1, "next_file": 2,
                     "last_seq": 0, "ssts": []}
            manifest.write(self.path, state)

        self.log_number = state["log_number"]
        self.next_file = state["next_file"]
        self.last_seq = state["last_seq"]
        # Tables are kept oldest -> newest; the newest table is last.
        self.tables = sorted(sst_name(int(x)) for x in state["ssts"])
        for name in self.tables:
            if not fs.exists(self._p(name)):
                raise CorruptionError("missing table %s" % name)

        wals = sorted(name for name in fs.listdir(self.path)
                      if _WAL_RE.match(name) and _num(name) >= self.log_number)
        for name in wals:
            entries, _ = replay(fs.read(self._p(name)))
            for seq, op in entries:
                self._apply(seq, op)
        self.live_wals = wals
        if wals:
            self.wal = self._new_wal(_num(wals[-1]))
        else:
            num = self.log_number
            fs.create(self._p(wal_name(num)))
            self.live_wals = [wal_name(num)]
            self.wal = self._new_wal(num)

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
        for name in reversed(self.tables):
            v = self._reader(name).get(key, limit)
            if v is not MISSING:
                return v
        return None

    # ------------------------------------------------------------ retention
    def _retain(self, versions):
        """``versions``: one key's versions, newest first.  Keep the latest
        version, plus the version the oldest live snapshot reads; everything
        in between is shadowed for every reader."""
        keep = [versions[0]]
        if self._snapshots:
            oldest = min(s.seq for s in self._snapshots)
            for v in versions:
                if v[0] <= oldest:
                    if v is not versions[0]:
                        keep.append(v)
                    break
        return keep

    def _gc(self, by_key):
        items = []
        for key in sorted(by_key):
            versions = sorted(by_key[key], key=lambda e: -e[0])
            for seq, val in self._retain(versions):
                items.append((key, seq, val))
        return items

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
        self.wal = self._new_wal(new_wal)
        self.live_wals = [wal_name(new_wal)]

        items = self._gc(imm)
        if items:
            sid = self._alloc()
            write_sst(self._p(sst_name(sid)), items)
            self.tables.append(sst_name(sid))
        # the memtable now lives in the table; its logs are no longer needed
        for name in old_wals:
            fs.unlink(self._p(name))
        self.log_number = new_wal
        self._save_manifest()
        self._maybe_compact()

    # ------------------------------------------------------------ compaction
    def _maybe_compact(self):
        while len(self.tables) > self.opts.max_ssts:
            self._merge(self.tables[-self.opts.merge_width:])

    def compact(self):
        """Flush, then merge every table into one."""
        self._check_open()
        self._flush()
        if self.tables:
            self._merge(list(self.tables))

    def _merge(self, inputs):
        by_key = {}
        for name in inputs:
            for k, seq, v in self._reader(name).items():
                by_key.setdefault(k, []).append((seq, v))
        # merged output has no older data underneath it to hide
        items = [e for e in self._gc(by_key) if e[2] is not None]
        for name in inputs:
            self._readers.pop(name, None)
            fs.unlink(self._p(name))
        keep = [t for t in self.tables if t not in inputs]
        if items:
            out = self._alloc()
            write_sst(self._p(sst_name(out)), items)
            keep.append(sst_name(out))
        self.tables = keep
        self._save_manifest()

    # ------------------------------------------------------------ misc
    def close(self):
        self.closed = True
        self._snapshots = []
        self._readers.clear()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
