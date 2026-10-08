"""Verifier for the tinykv crash-consistency / MVCC task.

The engine is exercised through its public API (tinykv.DB / tinykv.Options)
and through the simulated durability model in tinykv/fs.py (restored from a
pristine copy by test.sh before these tests run).  On-disk results are
checked with an independent implementation of FORMAT.md (tkv_format.py).
"""

import builtins
import contextlib
import tracemalloc
import io
import os
import random
import shutil
import sys

import pytest

APP = os.environ.get("TINYKV_APP", "/app")
sys.path.insert(0, APP)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tinykv import DB, Options  # noqa: E402
from tinykv import fs  # noqa: E402
import tkv_format as F  # noqa: E402

_real_open = builtins.open
_real_os = {name: getattr(os, name) for name in
            ("open", "replace", "rename", "remove", "unlink", "truncate",
             "ftruncate", "rmdir")}

# ---------------------------------------------------------------- bypass guard
# While engine code runs, any attempt to modify files outside tinykv.fs is an
# error: it would escape the durability model the task is graded against.

_GUARD = {"on": False, "violations": []}


def _under_guard(path):
    if not _GUARD["on"] or isinstance(path, int):
        return False
    try:
        p = os.path.abspath(os.fspath(path))
    except TypeError:
        return False
    return any(p.startswith(r) for r in _GUARD.get("roots", ()))


def _guarded_open(file, mode="r", *a, **kw):
    if any(c in mode for c in "wax+") and _under_guard(file):
        _GUARD["violations"].append(("open", file, mode))
        raise PermissionError("file writes must go through tinykv.fs")
    return _real_open(file, mode, *a, **kw)


def _guarded_os_open(path, flags, *a, **kw):
    w = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_APPEND | os.O_TRUNC
    if flags & w and _under_guard(path):
        _GUARD["violations"].append(("os.open", path, flags))
        raise PermissionError("file writes must go through tinykv.fs")
    return _real_os["open"](path, flags, *a, **kw)


def _mk_guard(name):
    real = _real_os[name]

    def g(path, *a, **kw):
        if _under_guard(path):
            _GUARD["violations"].append((name, path))
            raise PermissionError("file changes must go through tinykv.fs")
        return real(path, *a, **kw)
    return g


@pytest.fixture(autouse=True)
def bypass_guard(tmp_path, monkeypatch):
    _GUARD["roots"] = (str(tmp_path),)
    _GUARD["violations"] = []
    monkeypatch.setattr(builtins, "open", _guarded_open)
    monkeypatch.setattr(io, "open", _guarded_open)
    monkeypatch.setattr(os, "open", _guarded_os_open)
    for name in ("replace", "rename", "remove", "unlink", "truncate", "rmdir"):
        monkeypatch.setattr(os, name, _mk_guard(name))
    fs.reset()
    yield
    _GUARD["on"] = False
    fs.reset()
    assert not _GUARD["violations"], (
        "engine modified files without tinykv.fs: %r" % _GUARD["violations"][:5])


@contextlib.contextmanager
def engine():
    _GUARD["on"] = True
    try:
        yield
    finally:
        _GUARD["on"] = False


def rmtree(path):
    _GUARD["on"] = False
    shutil.rmtree(path, ignore_errors=True)


# ---------------------------------------------------------------- workloads

def value_for(i, length):
    tag = b"<%d>" % i
    if length >= len(tag):
        return (tag * (length // len(tag) + 1))[:length]
    return bytes([(i * 7 + j) % 251 for j in range(length)])


SPECIAL_LENS = [0, 1, 127, 128, 129, 255, 256, 16383, 16384, 16385, 16400,
                16511, 16512]


def gen_workload(seed, n_ops, n_keys=24, big=False, snaps=True, hot=0.0):
    """Random ops: put / del / batch / flush / compact / snap / release."""
    rng = random.Random(seed)
    ops = []
    n_snap = 0

    def rand_len():
        r = rng.random()
        if r < 0.25:
            return rng.choice([0, 1, 127, 128, 129, 255, 256])
        if big and r < 0.30:
            return rng.choice(SPECIAL_LENS)
        return rng.randrange(1, 90)

    def key():
        if hot and rng.random() < hot:
            return b"hot"
        return b"key-%03d" % rng.randrange(n_keys)

    for i in range(n_ops):
        r = rng.random()
        if r < 0.44:
            ops.append(("put", key(), value_for(i, rand_len())))
        elif r < 0.60:
            ops.append(("del", key()))
        elif r < 0.78:
            batch = []
            for j in range(rng.randrange(2, 6)):
                if rng.random() < 0.7:
                    batch.append(("put", key(), value_for(i * 100 + j, rand_len())))
                else:
                    batch.append(("del", key()))
            ops.append(("batch", batch))
        elif r < 0.84:
            ops.append(("flush",))
        elif r < 0.87:
            ops.append(("compact",))
        elif snaps and r < 0.94:
            ops.append(("snap", n_snap))
            n_snap += 1
        elif snaps and n_snap:
            ops.append(("release", rng.randrange(n_snap)))
        else:
            ops.append(("put", key(), value_for(i, 7)))
    return ops


class Model:
    """Reference semantics: sequence numbers, versions and snapshots."""

    def __init__(self):
        self.seq = 0
        self.state = {}
        self.versions = {}      # key -> [(seq, value-or-None)] ascending
        self.snaps = {}         # snapshot id -> (seq, state copy)

    def _write(self, key, val):
        self.seq += 1
        self.versions.setdefault(key, []).append((self.seq, val))
        if val is None:
            self.state.pop(key, None)
        else:
            self.state[key] = val

    def apply(self, op):
        if op[0] == "put":
            self._write(op[1], op[2])
        elif op[0] == "del":
            self._write(op[1], None)
        elif op[0] == "batch":
            for sub in op[1]:
                self.apply(sub)
        elif op[0] == "snap":
            self.snaps[op[1]] = (self.seq, dict(self.state))
        elif op[0] == "release":
            self.snaps.pop(op[1], None)

    def drop_snapshots(self):
        self.snaps = {}

    def expected_tables_after_full_compaction(self):
        """Exact (key, seq, value) list the single table must hold."""
        points = sorted({s for s, _ in self.snaps.values()}, reverse=True)
        out = []
        for key in sorted(self.versions):
            vs = sorted(self.versions[key], key=lambda e: -e[0])
            keep = [vs[0]]
            for p in points:
                vis = next((v for v in vs if v[0] <= p), None)
                if vis is not None and vis not in keep:
                    keep.append(vis)
            while keep and keep[-1][1] is None:
                keep.pop()
            out.extend((key, s, v) for s, v in keep)
        return out


class Runner:
    """Drives a DB with workload ops, tracking snapshot handles by id."""

    def __init__(self, db):
        self.db = db
        self.handles = {}

    def run(self, op):
        db = self.db
        if op[0] == "put":
            db.put(op[1], op[2])
        elif op[0] == "del":
            db.delete(op[1])
        elif op[0] == "batch":
            db.write_batch([tuple(x) for x in op[1]])
        elif op[0] == "flush":
            db.flush()
        elif op[0] == "compact":
            db.compact()
        elif op[0] == "snap":
            self.handles[op[1]] = db.snapshot()
        elif op[0] == "release":
            h = self.handles.pop(op[1], None)
            if h is not None:
                h.release()
        else:
            raise AssertionError(op)


def prefix_states(ops):
    m = Model()
    out = [dict(m.state)]
    for op in ops:
        m.apply(op)
        out.append(dict(m.state))
    return out


def keys_of(ops):
    ks = set()
    for op in ops:
        if op[0] in ("put", "del"):
            ks.add(op[1])
        elif op[0] == "batch":
            for sub in op[1]:
                ks.add(sub[1])
    return ks


def read_all(db, universe, snapshot=None):
    if snapshot is None:
        return {k: db.get(k) for k in sorted(universe)}
    return {k: db.get(k, snapshot=snapshot) for k in sorted(universe)}


def as_view(state, universe):
    return {k: state.get(k) for k in sorted(universe)}


def describe_diff(got, want):
    diffs = [(k, got.get(k), want.get(k)) for k in sorted(set(got) | set(want))
             if got.get(k) != want.get(k)]
    short = [(k, (g[:24] if g else g), (w[:24] if w else w))
             for k, g, w in diffs[:6]]
    return "%d keys differ, e.g. (key, got, expected): %r" % (len(diffs), short)


def check_reads(runner, model, universe, ctx):
    got = read_all(runner.db, universe)
    want = as_view(model.state, universe)
    assert got == want, "%s: latest state: %s" % (ctx, describe_diff(got, want))
    for sid, (seq, st) in sorted(model.snaps.items()):
        got = read_all(runner.db, universe, runner.handles[sid])
        want = as_view(st, universe)
        assert got == want, "%s: snapshot #%d (seq %d): %s" % (
            ctx, sid, seq, describe_diff(got, want))


def check_on_disk(path, expected, opts=None):
    """Validate a closed DB directory with the independent format reader."""
    state, info = F.load_db(path)
    assert state == expected, "on-disk contents wrong: " + describe_diff(state, expected)
    for w, (size, valid) in info["wals"].items():
        assert size == valid, "wal-%d.log has %d bytes of invalid tail" % (w, size - valid)
    if opts is not None:
        assert len(info["manifest"]["ssts"]) <= opts.max_ssts, (
            "more than max_ssts live tables: %r" % info["manifest"]["ssts"])
        # Whatever is still only in the live logs is the memtable; after the
        # last write it must be below memtable_limit (otherwise it would
        # have been flushed).
        size = sum(len(k) + (len(v) if v is not None else 0)
                   for _, k, v in info["wal_ops"])
        assert size < opts.memtable_limit, (
            "live logs hold %d bytes of memtable data (memtable_limit=%d): "
            "memtable was not flushed" % (size, opts.memtable_limit))
    return info


def single_table(info):
    ssts = info["manifest"]["ssts"]
    assert len(ssts) <= 1, "compact() must leave at most one table, got %r" % ssts
    assert info["wal_ops"] == [], "compact() must flush the memtable"
    return info["tables"][int(ssts[0])] if ssts else []


# ================================================================ basic API

def test_basic_api_and_reopen(tmp_path):
    d = str(tmp_path / "db")
    with engine():
        db = DB(d, Options(memtable_limit=1 << 20))
        assert db.get(b"nope") is None
        db.put(b"a", b"1")
        db.put(b"b", b"")
        s1 = db.snapshot()
        db.put(b"c", b"3")
        db.delete(b"c")
        db.write_batch([("put", b"d", b"4"), ("del", b"a"), ("put", b"d", b"5")])
        assert db.get(b"a") is None and db.get(b"b") == b"" and db.get(b"c") is None
        assert db.get(b"d") == b"5"
        assert s1.seq == 2
        assert (db.get(b"a", snapshot=s1), db.get(b"b", snapshot=s1),
                db.get(b"d", snapshot=s1)) == (b"1", b"", None)
        db.flush()
        assert db.get(b"a", snapshot=s1) == b"1" and db.get(b"a") is None
        s1.release()
        db.close()
        fs.reset()
        db = DB(d, Options(memtable_limit=1 << 20))
        assert (db.get(b"a"), db.get(b"b"), db.get(b"c"), db.get(b"d")) == (None, b"", None, b"5")
        s2 = db.snapshot()
        assert s2.seq == 7
        db.put(b"e", b"6")
        s3 = db.snapshot()
        assert s3.seq == 8
        db.close()
    info = check_on_disk(d, {b"b": b"", b"d": b"5", b"e": b"6"})
    assert info["max_seq"] == 8


@pytest.mark.parametrize("where", ["wal", "sst"])
def test_length_boundaries(tmp_path, where):
    d = str(tmp_path / "db")
    expected = {}
    with engine():
        db = DB(d, Options(memtable_limit=1 << 24))
        for i, n in enumerate(SPECIAL_LENS + [70000, 2 ** 21 + 5]):
            k = b"k%02d-" % i + b"q" * (n % 300)
            v = value_for(i, n)
            db.put(k, v)
            expected[k] = v
        if where == "sst":
            db.flush()
        for k, v in expected.items():
            assert db.get(k) == v, "wrong value for len(key)=%d" % len(k)
        db.close()
        fs.reset()
        db = DB(d, Options(memtable_limit=1 << 24))
        for k, v in expected.items():
            got = db.get(k)
            assert got == v, "after reopen: len(value)=%d came back as %r" % (
                len(v), None if got is None else len(got))
        db.close()
    check_on_disk(d, expected)


# ================================================================ model checks

MODEL_CONFIGS = [
    # (seed, n_ops, Options kwargs, reopen_every, hot)
    (1, 900, dict(memtable_limit=300, max_ssts=4, merge_width=3), 97, 0.0),
    (2, 900, dict(memtable_limit=450, max_ssts=40, merge_width=2), 151, 0.0),
    (3, 900, dict(memtable_limit=200, max_ssts=2, merge_width=2), 0, 0.3),
    (4, 1500, dict(memtable_limit=600, max_ssts=5, merge_width=4), 333, 0.1),
    (5, 1200, dict(memtable_limit=2000, max_ssts=3, merge_width=2), 0, 0.5),
]


@pytest.mark.parametrize("seed,n_ops,okw,reopen_every,hot", MODEL_CONFIGS)
def test_random_workload_matches_model(tmp_path, seed, n_ops, okw, reopen_every, hot):
    d = str(tmp_path / "db")
    ops = gen_workload(seed, n_ops, big=(seed == 4), hot=hot)
    universe = keys_of(ops)
    opts = Options(**okw)
    model = Model()
    with engine():
        r = Runner(DB(d, opts))
        for i, op in enumerate(ops):
            r.run(op)
            model.apply(op)
            if i % 23 == 0:
                check_reads(r, model, universe, "after op %d %r" % (i, op[0]))
            if reopen_every and i % reopen_every == reopen_every - 1:
                r.db.close()
                model.drop_snapshots()
                fs.reset()
                r = Runner(DB(d, opts))
                check_reads(r, model, universe, "after reopen at op %d" % i)
        check_reads(r, model, universe, "end of workload")
        check_on_disk(d, model.state, opts)
        live = sorted(model.snaps)
        r.db.compact()
        check_reads(r, model, universe, "after compact() with %d live snapshots" % len(live))
        r.db.close()
    info = check_on_disk(d, model.state, opts)
    got = single_table(info)
    want = model.expected_tables_after_full_compaction()
    assert got == want, (
        "compact() with live snapshots at seqs %r must keep exactly the versions "
        "visible to them or to the latest state; got %d entries, expected %d; "
        "first difference: %r" % (
            sorted(s for s, _ in model.snaps.values()), len(got), len(want),
            next(((a, b) for a, b in zip(got, want) if a != b), None)))
    with engine():
        fs.reset()
        r = Runner(DB(d, opts))
        model.drop_snapshots()
        r.db.compact()
        check_reads(r, model, universe, "after reopen + compact()")
        r.db.close()
    info = check_on_disk(d, model.state, opts)
    assert single_table(info) == model.expected_tables_after_full_compaction()


def test_snapshots_pin_versions_through_merges(tmp_path):
    """Many snapshots of one hot key, held across flushes and merges."""
    d = str(tmp_path / "db")
    opts = Options(memtable_limit=700, max_ssts=3, merge_width=2)
    model = Model()
    universe = {b"hot", b"a", b"b", b"z"}
    rng = random.Random(42)
    sid = 0
    with engine():
        r = Runner(DB(d, opts))

        def do(op):
            r.run(op)
            model.apply(op)

        for i in range(400):
            k = b"hot" if rng.random() < 0.7 else rng.choice([b"a", b"b", b"z"])
            if rng.random() < 0.15:
                do(("del", k))
            else:
                do(("put", k, value_for(i, rng.randrange(1, 40))))
            if i % 3 == 0:
                do(("snap", sid))
                sid += 1
            if i % 7 == 0 and sid > 4:
                do(("release", rng.randrange(sid - 4)))
            if i % 50 == 0:
                check_reads(r, model, universe, "op %d" % i)
        check_reads(r, model, universe, "before compact")
        do(("compact",))
        check_reads(r, model, universe, "after compact")
        for j in list(model.snaps)[::2]:
            do(("release", j))
        do(("put", b"z", b"last"))
        do(("compact",))
        check_reads(r, model, universe, "after second compact")
        r.db.close()
    info = check_on_disk(d, model.state, opts)
    got = single_table(info)
    assert got == model.expected_tables_after_full_compaction()
    assert sum(1 for k, _, _ in got if k == b"hot") > 40, "test precondition"


def test_deletes_survive_partial_compaction(tmp_path):
    """Tombstones must keep hiding data that lives in tables not merged."""
    d = str(tmp_path / "db")
    opts = Options(memtable_limit=1 << 20, max_ssts=3, merge_width=2)
    keys = [b"user:%04d" % i for i in range(60)]
    model = Model()
    with engine():
        r = Runner(DB(d, opts))
        for k in keys:
            r.run(("put", k, b"original-" + k))
            model.apply(("put", k, b"original-" + k))
        r.run(("flush",))
        for round_ in range(14):
            for k in keys[round_ * 4:(round_ + 1) * 4]:
                r.run(("del", k))
                model.apply(("del", k))
            op = ("put", b"filler-%d" % round_, b"x" * 10)
            r.run(op)
            model.apply(op)
            r.run(("flush",))
            check_reads(r, model, set(keys) | set(model.state), "round %d" % round_)
        r.db.close()
        fs.reset()
        r = Runner(DB(d, opts))
        check_reads(r, model, set(keys) | set(model.state), "after reopen")
        r.db.close()
    check_on_disk(d, model.state, opts)


def test_many_tables_newest_wins(tmp_path):
    """More than ten live tables / logs: precedence is numeric, not lexical."""
    d = str(tmp_path / "db")
    opts = Options(memtable_limit=1 << 20, max_ssts=100, merge_width=2)
    model = Model()
    with engine():
        r = Runner(DB(d, opts))
        for gen in range(25):
            for k in range(8):
                op = ("put", b"k%d" % ((k + gen) % 12), b"gen%d" % gen)
                r.run(op)
                model.apply(op)
            r.run(("flush",))
        r.db.close()
        for _ in range(3):
            fs.reset()
            r = Runner(DB(d, opts))
            check_reads(r, model, set(model.state), "after reopen")
            r.run(("put", b"k0", b"latest"))
            model.apply(("put", b"k0", b"latest"))
            r.db.close()
    check_on_disk(d, model.state)


# ================================================================ compatibility

def _fixture_dir(root):
    """Fabricate a database written by another FORMAT.md v3 implementation
    that crashed while appending to its log."""
    d = os.path.join(root, "legacy")
    os.makedirs(d)

    def w(name, data):
        with _real_open(os.path.join(d, name), "wb") as f:
            f.write(data)

    seq = [0]

    def nxt():
        seq[0] += 1
        return seq[0]

    expected = {}

    def table(entries):
        """entries: [(key, value-or-None)] in write order -> sst bytes."""
        items = []
        for k, v in entries:
            s = nxt()
            items.append((k, s, v))
            if v is None:
                expected.pop(k, None)
            else:
                expected[k] = v
        items.sort(key=lambda e: (e[0], -e[1]))
        return F.sst_bytes(items)

    t2 = [(b"key-%02d" % i, b"t2-%d" % i) for i in range(40)]
    t2 += [(b"len128", value_for(128, 128)), (b"len16400", value_for(16400, 16400))]
    # 40 versions of one key kept by (former) snapshots: spans index blocks
    t2 += [(b"key-17", b"t2-17-v%d" % j) for j in range(40)]
    w("sst-2.sst", table(t2))
    w("sst-9.sst", table([(b"key-%02d" % i, b"t9-%d" % i) for i in range(0, 40, 2)]
                         + [(b"key-05", None), (b"key-07", None)]))
    w("sst-10.sst", table([(b"key-%02d" % i, b"t10-%d" % i) for i in range(0, 40, 3)]
                          + [(b"key-01", None), (b"key-05", b"t10-back")]
                          + [(b"key-16", b"t10-16-v%d" % j) for j in range(20)]))
    w("sst-11.sst", table([(b"key-%02d" % i, b"t11-%d" % i) for i in range(0, 40, 5)]
                          + [(b"key-02", None), (b"key-11", None)]))
    table_max = seq[0]
    # garbage left behind by an interrupted compaction / obsolete log
    w("sst-101.sst", F.sst_bytes([(b"key-00", 10 ** 6, b"ORPHAN"), (b"zzz", 10 ** 6, b"ORPHAN")]))
    w("sst-7.sst", F.sst_bytes([(b"key-03", 10 ** 6, b"STALE")]))
    w("wal-8.log", F.wal_put(10 ** 6, b"ghost", b"OBSOLETE"))
    w("MANIFEST.tmp", b"{\"half\":")
    # live logs: 99 then 100
    s1 = nxt()
    b0 = nxt()
    nxt()
    s4 = nxt()
    w("wal-99.log", F.wal_put(s1, b"key-20", b"w99")
      + F.wal_batch(b0, [("put", b"key-21", b"w99b"), ("del", b"key-22")])
      + F.wal_put(s4, b"key-23", b"w99-old"))
    expected[b"key-20"] = b"w99"
    expected[b"key-21"] = b"w99b"
    expected.pop(b"key-22", None)
    a, b_, c = nxt(), nxt(), nxt()
    tail_batch = F.wal_batch(seq[0] + 1, [("put", b"key-24", b"UNCOMMITTED"), ("del", b"key-25")])
    tail_batch = tail_batch[: len(tail_batch) - 9]  # BATCH_COMMIT never made it
    torn = F.wal_put(seq[0] + 3, b"key-26", b"TORN" * 10)[:-7]
    w("wal-100.log", F.wal_put(a, b"key-23", b"w100") + F.wal_del(b_, b"key-27")
      + F.wal_put(c, b"len128", value_for(1, 128)) + tail_batch + torn)
    expected[b"key-23"] = b"w100"
    expected.pop(b"key-27", None)
    expected[b"len128"] = value_for(1, 128)
    # last_seq is only a lower bound: it lags behind the tables and the logs
    w("MANIFEST", F.manifest_bytes({"version": 3, "log_number": 99, "next_file": 99,
                                    "last_seq": table_max - 30,
                                    "ssts": [11, 2, 10, 9]}))
    return d, expected, seq[0]


def test_opens_existing_directory(tmp_path):
    d, expected, last_seq = _fixture_dir(str(tmp_path))
    universe = set(expected) | {b"key-%02d" % i for i in range(40)} | {b"ghost", b"zzz"}
    opts = Options(memtable_limit=1 << 20, max_ssts=4, merge_width=3)
    with engine():
        db = DB(d, opts)
        got = read_all(db, universe)
        assert got == as_view(expected, universe), describe_diff(got, as_view(expected, universe))
        snap = db.snapshot()
        assert snap.seq == last_seq, "snapshot of the recovered state must have seq %d, got %d" % (
            last_seq, snap.seq)
        db.put(b"after-1", b"A1")
        db.delete(b"key-00")
        db.write_batch([("put", b"after-2", b"A2"), ("put", b"key-24", b"A3"),
                        ("put", b"key-17", b"A17"), ("put", b"key-16", b"A16")])
        before = dict(expected)
        expected.update({b"after-1": b"A1", b"after-2": b"A2", b"key-24": b"A3",
                         b"key-17": b"A17", b"key-16": b"A16"})
        expected.pop(b"key-00")
        universe |= set(expected)
        got = read_all(db, universe)
        assert got == as_view(expected, universe), "new writes must win: " + describe_diff(
            got, as_view(expected, universe))
        got = read_all(db, universe, snap)
        assert got == as_view(before, universe), "snapshot: " + describe_diff(got, as_view(before, universe))
        db.close()
        fs.reset()
        db = DB(d, opts)
        got = read_all(db, universe)
        assert got == as_view(expected, universe), (
            "writes after recovering a torn log were lost: " + describe_diff(got, as_view(expected, universe)))
        db.flush()
        db.put(b"after-3", b"A4")
        expected[b"after-3"] = b"A4"
        db.close()
        fs.reset()
        db = DB(d, opts)
        got = read_all(db, universe)
        assert got == as_view(expected, universe), describe_diff(got, as_view(expected, universe))
        db.close()
    info = check_on_disk(d, expected, opts)
    assert info["max_seq"] == last_seq + 7
    with engine():
        fs.reset()
        db = DB(d, opts)
        db.compact()
        db.close()
    info = check_on_disk(d, expected, opts)
    table = single_table(info)
    assert [(k, v) for k, _, v in table] == sorted(expected.items())


def test_last_seq_is_only_a_lower_bound(tmp_path):
    """A directory whose MANIFEST last_seq lags behind its tables."""
    d = str(tmp_path / "lag")
    os.makedirs(d)

    def w(name, data):
        with _real_open(os.path.join(d, name), "wb") as f:
            f.write(data)

    old = [(b"acct-%02d" % i, 100 + i, b"old-%d" % i) for i in range(30)]
    new = [(b"acct-%02d" % i, 200 + i, b"new-%d" % i) for i in range(0, 30, 2)]
    w("sst-3.sst", F.sst_bytes(sorted(old, key=lambda e: (e[0], -e[1]))))
    w("sst-5.sst", F.sst_bytes(sorted(new, key=lambda e: (e[0], -e[1]))))
    w("wal-6.log", b"")
    w("MANIFEST", F.manifest_bytes({"version": 3, "log_number": 6, "next_file": 7,
                                    "last_seq": 50, "ssts": [3, 5]}))
    expected = {k: v for k, _, v in old}
    expected.update({k: v for k, _, v in new})
    opts = Options(memtable_limit=1 << 20)
    with engine():
        db = DB(d, opts)
        snap = db.snapshot()
        assert snap.seq == 228, "snapshot must see seq 228, the newest version on disk; got %d" % snap.seq
        for i in range(30):
            db.put(b"acct-%02d" % i, b"fresh-%d" % i)
        for i in range(30):
            assert db.get(b"acct-%02d" % i) == b"fresh-%d" % i, "new write must be the newest version"
            assert db.get(b"acct-%02d" % i, snapshot=snap) == expected[b"acct-%02d" % i]
        db.close()
        fs.reset()
        db = DB(d, opts)
        db.compact()
        for i in range(30):
            assert db.get(b"acct-%02d" % i) == b"fresh-%d" % i
        db.close()
    info = check_on_disk(d, {b"acct-%02d" % i: b"fresh-%d" % i for i in range(30)}, opts)
    assert [s for _, s, _ in single_table(info)] == list(range(229, 259))


# ================================================================ resource bounds

BIG_TABLES = [2, 3, 4, 5, 6, 7]
BIG_KEYS = 3000
BIG_VLEN = 1000
HOT_VERSIONS = 40000


def _big_dir(root):
    """A large directory written by another implementation: six tables of
    about 3 MB each, keys distinct per table, plus one key with 40,000
    versions (pinned once by snapshots that no longer exist)."""
    d = os.path.join(root, "big")
    os.makedirs(d)
    seq = 0
    versions = {}
    layouts = {}
    for sid in BIG_TABLES:
        items = []
        if sid == BIG_TABLES[0]:
            for j in range(HOT_VERSIONS):
                seq += 1
                v = None if j % 97 == 13 else b"hot-%d" % j
                items.append((b"hot", seq, v))
        for i in range(BIG_KEYS):
            seq += 1
            items.append((b"t%d-k%05d" % (sid, i), seq, value_for(seq, BIG_VLEN)))
        for k, sq, v in items:
            versions.setdefault(k, []).append((sq, v))
        items.sort(key=lambda e: (e[0], -e[1]))
        data = F.sst_bytes(items)
        layouts[sid] = F.sst_layout(data)
        with _real_open(os.path.join(d, "sst-%d.sst" % sid), "wb") as f:
            f.write(data)
    with _real_open(os.path.join(d, "wal-8.log"), "wb") as f:
        f.write(b"")
    with _real_open(os.path.join(d, "MANIFEST"), "wb") as f:
        f.write(F.manifest_bytes({"version": 3, "log_number": 8, "next_file": 9,
                                  "last_seq": 0, "ssts": BIG_TABLES}))
    return d, versions, layouts, seq


def _model_from_versions(versions, last_seq):
    m = Model()
    m.seq = last_seq
    m.versions = {k: list(vs) for k, vs in versions.items()}
    for k, vs in versions.items():
        latest = max(vs)[1]
        if latest is not None:
            m.state[k] = latest
    return m


def _read_bytes():
    return fs.stats().get("read_bytes", 0)


def _largest_block(layouts):
    return max(end - off for _, blocks in layouts.values() for _, off, end, _ in blocks)


def test_open_reads_only_footers_and_indexes(tmp_path):
    d, versions, layouts, last_seq = _big_dir(str(tmp_path))
    allowed = os.path.getsize(os.path.join(d, "MANIFEST")) + os.path.getsize(os.path.join(d, "wal-8.log"))
    for footer, _ in layouts.values():
        allowed += footer["index_len"] + footer["footer_len"]
    fs.reset()
    with engine():
        before = _read_bytes()
        db = DB(d, Options(memtable_limit=1 << 20, max_ssts=10))
        used = _read_bytes() - before
        snap = db.snapshot()
        db.close()
    assert snap.seq == last_seq, "recovered sequence number must be %d, got %d" % (last_seq, snap.seq)
    assert used <= allowed, (
        "opening read %d bytes; only MANIFEST, live logs, table footers and "
        "indexes (%d bytes) may be read" % (used, allowed))


def test_get_reads_at_most_two_blocks_per_table(tmp_path):
    d, versions, layouts, last_seq = _big_dir(str(tmp_path))
    rng = random.Random(3)
    keys = [b"t%d-k%05d" % (rng.choice(BIG_TABLES), rng.randrange(BIG_KEYS)) for _ in range(150)]
    keys += [b"t%d-k%05dx" % (rng.choice(BIG_TABLES), rng.randrange(BIG_KEYS)) for _ in range(50)]
    keys += [b"a", b"zzz", b"t4-", b"t9"]
    fs.reset()
    with engine():
        db = DB(d, Options(memtable_limit=1 << 20, max_ssts=10))
        for key in keys:
            # Tables are consulted newest first, down to the one holding the key.
            home = int(key[1:key.index(b"-")]) if key in versions else None
            allowed = 0
            for sid in reversed(BIG_TABLES):
                blocks = layouts[sid][1]
                i = sum(1 for b in blocks if b[0] < key)
                for j in (i - 1, i):
                    if 0 <= j < len(blocks):
                        allowed += blocks[j][2] - blocks[j][1]
                if sid == home:
                    break
            before = _read_bytes()
            got = db.get(key)
            used = _read_bytes() - before
            want = max(versions[key])[1] if key in versions else None
            assert got == want, "wrong value for %r" % key
            assert used <= allowed, (
                "get(%r) read %d bytes; at most two data blocks per consulted "
                "table (%d bytes) may be read" % (key, used, allowed))
        db.close()


def _peak_during(fn):
    tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        base = tracemalloc.get_traced_memory()[0]
        fn()
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    return peak - base


def _mem_bound(n_inputs, largest_block, n_snaps):
    return 2 * 1024 * 1024 + (n_inputs + 2) * largest_block + 1024 * n_snaps


def test_compact_memory_is_bounded(tmp_path):
    d, versions, layouts, last_seq = _big_dir(str(tmp_path))
    model = _model_from_versions(versions, last_seq)
    universe = set(versions) - {b"hot"}
    sample = set(random.Random(5).sample(sorted(universe), 400)) | {b"hot"}
    opts = Options(memtable_limit=1 << 20, max_ssts=10)
    fs.reset()
    with engine():
        r = Runner(DB(d, opts))

        def do(op):
            r.run(op)
            model.apply(op)

        do(("snap", 0))
        do(("put", b"hot", b"hot-new"))
        do(("del", b"t3-k00010"))
        do(("snap", 1))
        do(("put", b"t3-k00010", b"back"))
        do(("del", b"hot"))
        do(("put", b"t7-k00001", b"x" * 50))
        do(("snap", 2))
        do(("del", b"t2-k00002"))
        check_reads(r, model, sample, "before compact")
        largest = _largest_block(layouts)
        n_inputs = len(BIG_TABLES) + 1
        used = _peak_during(r.db.compact)
        bound = _mem_bound(n_inputs, largest, 3)
        check_reads(r, model, sample, "after compact")
        r.db.close()
    assert used <= bound, (
        "compact() used %d bytes of memory at peak; the bound is %d" % (used, bound))
    info = check_on_disk(d, model.state, opts)
    got = single_table(info)
    want = model.expected_tables_after_full_compaction()
    assert got == want, "compact() kept the wrong versions; first difference: %r" % (
        next(((a[:2], b[:2]) for a, b in zip(got, want) if a != b), (len(got), len(want))),)


def test_automatic_merge_memory_is_bounded(tmp_path):
    d, versions, layouts, last_seq = _big_dir(str(tmp_path))
    model = _model_from_versions(versions, last_seq)
    opts = Options(memtable_limit=1 << 20, max_ssts=len(BIG_TABLES), merge_width=3)
    fs.reset()
    with engine():
        r = Runner(DB(d, opts))
        for i in range(50):
            op = ("put", b"t7-k%05d" % (i * 7), b"new-%d" % i)
            r.run(op)
            model.apply(op)
        largest = _largest_block(layouts)
        used = _peak_during(r.db.flush)
        sample = set(random.Random(9).sample(sorted(versions), 300)) | {b"t7-k00007"}
        check_reads(r, model, sample, "after automatic merge")
        r.db.close()
    bound = _mem_bound(3, largest, 0)
    assert used <= bound, (
        "flush() with its automatic merge used %d bytes at peak; the bound is %d" % (used, bound))
    info = check_on_disk(d, model.state, opts)
    assert len(info["manifest"]["ssts"]) == len(BIG_TABLES) - 1


# ================================================================ crash tests

CRASH_OPTS = dict(memtable_limit=260, max_ssts=3, merge_width=2)


def _run_until_crash(d, ops, opts, n):
    """Run ``ops`` on dir ``d`` (fresh or existing) with a crash armed after
    ``n`` fs ops.  Returns (acked, opened, crashed)."""
    acked = 0
    opened = False
    fs.reset()
    fs.arm_crash(n)
    with engine():
        try:
            r = Runner(DB(d, opts))
            opened = True
            for op in ops:
                r.run(op)
                acked += 1
            r.db.close()
        except fs.SimulatedCrash:
            return acked, opened, True
        finally:
            fs.disarm()
    assert not fs.has_crashed()
    return acked, opened, False


def _outcomes(d, base, data_modes, choices=None):
    """Materialise post-crash directories allowed by the durability model.
    Must be called right after a crash, before fs.reset()."""
    out = []
    all_choices = fs.crash_choices(d)
    if choices is not None:
        all_choices = [c for c in all_choices if c in choices or c.startswith("drop:") and "drop:*" in choices]
    k = 0
    for choice in all_choices:
        for mode in data_modes:
            o = "%s.o%d" % (base, k)
            k += 1
            fs.materialize(d, o, choice, mode)
            out.append((o, "dir outcome %s, content outcome %s" % (choice, mode)))
    return out


def _candidates(states, acked, opened, n_ops):
    c = [states[acked]]
    if opened and acked < n_ops:
        c.append(states[acked + 1])
    return c


def _verify_recovery(d, opts, cands, universe, ctx, full=True):
    """Clean reopen must show one of ``cands``; then the DB must keep working
    and persist new writes across further restarts (and, when ``full``,
    snapshots and compaction)."""
    fs.reset()
    with engine():
        db = DB(d, opts)
        got = read_all(db, universe)
    views = [as_view(c, universe) for c in cands]
    assert got in views, "%s: recovered state is not a valid prefix: %s" % (
        ctx, describe_diff(got, views[-1]))
    state = dict(cands[views.index(got)])
    keys = sorted(universe)
    if not full:
        with engine():
            db.put(keys[0], b"after-recovery")
            db.close()
        state[keys[0]] = b"after-recovery"
        fs.reset()
        with engine():
            db = DB(d, opts)
            got = read_all(db, universe)
            db.close()
        assert got == as_view(state, universe), "%s: write after recovery lost: %s" % (
            ctx, describe_diff(got, as_view(state, universe)))
        return
    post = [("put", b"post-a", b"P" * 128), ("del", keys[0]),
            ("batch", [("put", b"post-b", b"PB"), ("put", keys[1], b"PC")]),
            ("put", keys[2], value_for(9, 300)), ("put", keys[3], b"PD")]
    with engine():
        r = Runner(db)
        r.run(("snap", 0))
        pinned = dict(state)
        m = Model()
        m.state = state
        for op in post:
            r.run(op)
            m.apply(op)
        got = read_all(db, universe, r.handles[0])
        assert got == as_view(pinned, universe), "%s: snapshot after recovery: %s" % (
            ctx, describe_diff(got, as_view(pinned, universe)))
        db.close()
    u2 = universe | {b"post-a", b"post-b"}
    for step in ("reopen", "compact"):
        fs.reset()
        with engine():
            db = DB(d, opts)
            if step == "compact":
                db.compact()
            got = read_all(db, u2)
            db.close()
        assert got == as_view(state, u2), "%s: after recovery + new writes (%s): %s" % (
            ctx, step, describe_diff(got, as_view(state, u2)))
    info = check_on_disk(d, state, opts)
    table = single_table(info)
    assert [(k, v) for k, _, v in table] == sorted(state.items()), (
        "%s: compact() without snapshots must keep exactly one version per live key" % ctx)


def _crash_sweep(tmp_path, ops, data_modes):
    opts = Options(**CRASH_OPTS)
    states = prefix_states(ops)
    universe = keys_of(ops)
    n = 0
    crashes = 0
    while True:
        d = str(tmp_path / ("c%d" % n))
        acked, opened, crashed = _run_until_crash(d, ops, opts, n)
        if not crashed:
            assert acked == len(ops)
            break
        crashes += 1
        outs = _outcomes(d, d, data_modes)
        cands = _candidates(states, acked, opened, len(ops))
        for i, (o, desc) in enumerate(outs):
            _verify_recovery(o, opts, cands, universe,
                             "crash before fs op #%d (%d ops acked; %s)" % (n + 1, acked, desc),
                             full=(i < len(data_modes)))
            rmtree(o)
        rmtree(d)
        n += 1
        assert n < 50000, "workload never completes"
    fs.reset()
    assert crashes > 100, "too few crash points: is all I/O going through tinykv.fs?"


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_crash_anywhere(tmp_path, seed):
    ops = gen_workload(100 + seed, 110, n_keys=14, hot=0.2)
    _crash_sweep(tmp_path, ops, ("none", "half", "all", "torn") if seed == 1 else ("none", "torn"))


def test_crash_anywhere_large_values(tmp_path):
    rng = random.Random(7)
    ops = []
    for i in range(40):
        k = b"big-%d" % rng.randrange(6)
        r = rng.random()
        if r < 0.6:
            ops.append(("put", k, value_for(i, rng.choice([128, 129, 300, 16384, 16400]))))
        elif r < 0.8:
            ops.append(("del", k))
        else:
            ops.append(("batch", [("put", k, value_for(i, 128)), ("del", b"big-%d" % rng.randrange(6))]))
        if i % 5 == 0:
            ops.append(("snap", i))
    _crash_sweep(tmp_path, ops, ("none", "torn"))


@pytest.mark.parametrize("mode", ["none", "torn"])
def test_crash_during_recovery(tmp_path, mode):
    """Crash, then crash again while recovering / writing, then recover."""
    opts = Options(**CRASH_OPTS)
    ops = gen_workload(555, 70, n_keys=10)
    states = prefix_states(ops)
    universe = keys_of(ops)
    post = [("put", b"p%d" % i, value_for(i, 40 + i * 30)) for i in range(6)]
    post.insert(3, ("flush",))
    first = 1
    checked = 0
    while True:
        d0 = str(tmp_path / "first")
        rmtree(d0)
        acked, opened, crashed = _run_until_crash(d0, ops, opts, first)
        if not crashed:
            break
        cands = _candidates(states, acked, opened, len(ops))
        firsts = _outcomes(d0, d0, (mode,))
        for f0, desc0 in firsts:
            m = 0
            while True:
                d = f0 + ".second"
                observed = {}
                p_acked = 0
                if os.path.exists(d):
                    rmtree(d)
                shutil.copytree(f0, d)
                fs.reset()
                fs.arm_crash(m)
                crashed2 = False
                with engine():
                    try:
                        r = Runner(DB(d, opts))
                        observed["state"] = read_all(r.db, universe)
                        for op in post:
                            r.run(op)
                            p_acked += 1
                        r.db.close()
                    except fs.SimulatedCrash:
                        crashed2 = True
                    finally:
                        fs.disarm()
                if crashed2:
                    seconds = _outcomes(d, d, (mode,), choices={"all", "none"})
                else:
                    seconds = [(d, "no second crash")]
                for o, desc2 in seconds:
                    fs.reset()
                    with engine():
                        db = DB(o, opts)
                        got = read_all(db, universe)
                        got_post = {op[1]: db.get(op[1]) for op in post if op[0] == "put"}
                        db.close()
                    ctx = "first crash before op #%d (%s), second before op #%d (%s)" % (
                        first + 1, desc0, m + 1, desc2)
                    views = [as_view(c, universe) for c in cands]
                    assert got in views, ctx + ": " + describe_diff(got, views[-1])
                    if "state" in observed:
                        assert got == observed["state"], (
                            ctx + ": state changed between recoveries: "
                            + describe_diff(got, observed["state"]))
                    for op in post[:p_acked]:
                        if op[0] == "put":
                            assert got_post[op[1]] == op[2], ctx + ": acknowledged write %r lost" % op[1]
                    for op in post[p_acked + 1:]:
                        if op[0] == "put":
                            assert got_post[op[1]] is None, ctx + ": write %r appeared before it was issued" % op[1]
                    checked += 1
                    if o != d:
                        rmtree(o)
                rmtree(d)
                if not crashed2:
                    break
                m += 1
            rmtree(f0)
        first += 17
    fs.reset()
    assert checked > 200


def test_writes_are_routed_through_fs(tmp_path):
    d = str(tmp_path / "db")
    fs.reset()
    with engine():
        db = DB(d, Options(memtable_limit=1 << 20))
        before = fs.stats()
        for i in range(20):
            db.put(b"k%d" % i, b"v")
        after = fs.stats()
        db.close()
    assert after.get("append", 0) - before.get("append", 0) >= 20
    assert after.get("fsync", 0) - before.get("fsync", 0) >= 20
    for name in os.listdir(d):
        assert os.path.abspath(os.path.join(d, name)) in fs.touched_paths(), name


def test_crash_model_is_pristine():
    """The crash/durability model in use must be the unmodified original."""
    here = os.path.dirname(os.path.abspath(__file__))
    with _real_open(os.path.join(here, "fs_verifier.py"), "rb") as f:
        pristine = f.read()
    with _real_open(fs.__file__, "rb") as f:
        assert f.read() == pristine, "tinykv/fs.py differs from the original"
    assert os.path.realpath(fs.__file__) == os.path.realpath(
        os.path.join(APP, "tinykv", "fs.py"))
    for name in ("create", "append", "fsync", "fsync_dir", "rename", "unlink", "truncate", "read"):
        fn = getattr(fs, name)
        assert os.path.realpath(fn.__code__.co_filename) == os.path.realpath(fs.__file__), (
            "tinykv.fs.%s has been replaced at runtime" % name)
    assert sys.modules["tinykv.fs"] is fs
