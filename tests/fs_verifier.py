"""File-system layer for tinykv, and the durability contract it is graded on.

*** DO NOT MODIFY THIS FILE. ***  Every byte tinykv puts on disk must go
through the functions below; nothing in the engine may create, write,
rename, delete or truncate files in a database directory any other way.

Durability model (what survives a power failure)
------------------------------------------------
File contents:

* Bytes added with ``append`` are not durable until ``fsync`` is called on
  that file.  ``fsync`` makes the file's current content durable.  It does
  NOT make the file's name durable.
* ``truncate`` is durable as soon as it returns.

Directory entries:

* ``create``, ``rename`` and ``unlink`` change a directory.  Each such change
  is *pending* until ``fsync_dir`` is called on that directory, which makes
  every pending change of that directory durable at once.
* At a power failure, each directory keeps its durable entries, plus some
  subset of its pending changes, applied in their original order.  That
  subset is arbitrary, with one constraint: for every file name, the changes
  that touch that name and survive are a prefix (oldest first) of the
  pending changes that touch that name.  A ``rename`` touches both of its
  names.
* So, before the next ``fsync_dir`` of the directory, a crash may keep a
  later change while losing an earlier one.  For example, an ``unlink``
  of one file can survive while an earlier ``rename`` of another file is
  lost.
* A file whose ``create`` is lost does not exist after the crash, whatever
  was written or fsync'd to it.  A file whose ``unlink`` (or replacement by
  ``rename``) is lost still exists, with its durable content.  A file
  renamed in a surviving ``rename`` keeps its content and its durability
  state.
* Files and directory entries that existed when the current simulated
  process started (see ``reset``) are durable.  Creating the database
  directory itself (``ensure_dir``) is durable immediately.

Non-durable file content that survives a crash: all of it may be lost, part
of it (a prefix) may survive, or all of it may survive.  If the crash
happens during an ``append``, a prefix of the bytes being appended may also
survive.  Code must be correct in every case.

Crash simulation
----------------
``arm_crash(n, mode)`` makes the (n+1)-th mutating operation (create,
append, fsync, fsync_dir, rename, unlink, truncate) crash instead of
executing.  ``SimulatedCrash`` (a ``BaseException``) is raised, and every
further call into this module raises it again until ``reset()`` is called.
``reset()`` starts a new simulated process lifetime.  Code under test must
never catch ``SimulatedCrash``.

Accounting
----------
``stats()`` counts every operation by name.  It also has ``"read_bytes"``:
the total number of bytes returned by ``read`` and ``read_at``.
"""

import builtins
import os

__all__ = [
    "SimulatedCrash", "MODES", "reset", "arm_crash", "disarm", "op_count",
    "stats", "touched_paths", "has_crashed", "ensure_dir", "create", "append",
    "fsync", "fsync_dir", "rename", "unlink", "truncate", "read", "read_at",
    "exists", "size", "listdir",
]

_open = builtins.open
_replace = os.replace
_unlink = os.unlink
_rmdir = os.rmdir
_truncate = os.truncate
_listdir = os.listdir
_makedirs = os.makedirs
_exists = os.path.exists
_isdir = os.path.isdir
_getsize = os.path.getsize
_abspath = os.path.abspath

MODES = ("none", "half", "all", "torn")


class SimulatedCrash(BaseException):
    """Raised when the simulated machine loses power."""


class _Inode:
    __slots__ = ("id", "durable", "loc")

    def __init__(self, ident, durable, loc):
        self.id = ident
        self.durable = durable      # durable content length
        self.loc = loc              # where its bytes currently live


class _State:
    def __init__(self):
        self.ops = 0
        self.crash_after = None
        self.mode = "none"
        self.crashed = False
        self.crash_op = None        # (op, inode, data) of the crashing op
        self.stats = {}
        self.touched = set()
        self.cur = {}               # dir -> {name: inode}
        self.dur = {}               # dir -> {name: inode}
        self.pending = {}           # dir -> [change]
        self.next_ino = 1
        self.graves = set()


_st = _State()


def _rmtree(path):
    if not _exists(path):
        return
    for name in _listdir(path):
        p = os.path.join(path, name)
        if _isdir(p):
            _rmtree(p)
        else:
            _unlink(p)
    _rmdir(path)


def reset():
    """Start a new simulated process lifetime (everything on disk is durable)."""
    global _st
    for g in _st.graves:
        _rmtree(g)
    _st = _State()


def arm_crash(after_ops, mode="none"):
    if mode not in MODES:
        raise ValueError("unknown crash mode %r" % (mode,))
    if after_ops < 0:
        raise ValueError("after_ops must be >= 0")
    _st.crash_after = after_ops
    _st.mode = mode


def disarm():
    _st.crash_after = None


def op_count():
    return _st.ops


def stats():
    return dict(_st.stats)


def touched_paths():
    return set(_st.touched)


def has_crashed():
    return _st.crashed


def _split(path):
    p = _abspath(os.fspath(path))
    return p, os.path.dirname(p), os.path.basename(p)


def _dir(d):
    if d not in _st.cur:
        ns = {}
        if _isdir(d):
            for name in _listdir(d):
                p = os.path.join(d, name)
                if not _isdir(p):
                    ns[name] = _new_inode(_getsize(p), p)
        _st.cur[d] = ns
        _st.dur[d] = dict(ns)
        _st.pending[d] = []
    return _st.cur[d]


def _new_inode(durable, loc):
    ino = _Inode(_st.next_ino, durable, loc)
    _st.next_ino += 1
    return ino


def _grave(d, ino):
    g = os.path.join(os.path.dirname(d), ".tinykv-grave-%d" % os.getpid())
    if g not in _st.graves:
        _makedirs(g, exist_ok=True)
        _st.graves.add(g)
    return os.path.join(g, "%s-%d" % (os.path.basename(d), ino.id))


def _alive():
    if _st.crashed:
        raise SimulatedCrash("the simulated process has already crashed")


def _mutate(op, p, ino=None, data=None):
    _alive()
    if _st.crash_after is not None and _st.ops >= _st.crash_after:
        _st.crashed = True
        _st.crash_op = (op, ino, data)
        raise SimulatedCrash("simulated crash before mutating op #%d (%s %s)"
                             % (_st.ops + 1, op, os.path.basename(p)))
    _st.ops += 1
    _st.stats[op] = _st.stats.get(op, 0) + 1
    _st.touched.add(p)


def _lookup(path):
    p, d, name = _split(path)
    ino = _dir(d).get(name)
    if ino is None:
        raise FileNotFoundError(p)
    return p, d, name, ino


# ---------------------------------------------------------------- mutating

def ensure_dir(path):
    """Create a directory (and parents) if missing.  Durable, not counted."""
    _alive()
    _makedirs(_abspath(os.fspath(path)), exist_ok=True)


def create(path):
    """Create a new empty file.  Raises FileExistsError if it exists."""
    p, d, name = _split(path)
    _alive()
    ns = _dir(d)
    if name in ns or _exists(p):
        raise FileExistsError(p)
    _mutate("create", p)
    with _open(p, "xb"):
        pass
    ino = _new_inode(0, p)
    ns[name] = ino
    _st.pending[d].append(("create", (name,), ino))


def append(path, data):
    """Append bytes to an existing file (not durable until fsync)."""
    data = bytes(data)
    _alive()
    p, d, name, ino = _lookup(path)
    _mutate("append", p, ino, data)
    with _open(p, "ab") as f:
        f.write(data)


def fsync(path):
    """Make the current content of ``path`` durable (not its name)."""
    _alive()
    p, d, name, ino = _lookup(path)
    _mutate("fsync", p, ino)
    ino.durable = _getsize(p)


def fsync_dir(path):
    """Make every pending change of directory ``path`` durable."""
    _alive()
    d = _abspath(os.fspath(path))
    _dir(d)
    _mutate("fsync_dir", d)
    _st.dur[d] = dict(_st.cur[d])
    _st.pending[d] = []


def rename(src, dst):
    """Atomically rename src to dst (same directory), replacing dst."""
    _alive()
    s, d, sname, ino = _lookup(src)
    t, d2, dname = _split(dst)
    if d2 != d:
        raise ValueError("rename must stay within one directory")
    _mutate("rename", s)
    _st.touched.add(t)
    ns = _st.cur[d]
    old = ns.get(dname)
    if old is not None and old is not ino:
        g = _grave(d, old)
        _replace(t, g)
        old.loc = g
    _replace(s, t)
    ino.loc = t
    del ns[sname]
    ns[dname] = ino
    _st.pending[d].append(("rename", (sname, dname), ino))


def unlink(path):
    _alive()
    p, d, name, ino = _lookup(path)
    _mutate("unlink", p)
    g = _grave(d, ino)
    _replace(p, g)
    ino.loc = g
    del _st.cur[d][name]
    _st.pending[d].append(("unlink", (name,), ino))


def truncate(path, length):
    """Shrink a file to ``length`` bytes (durable immediately)."""
    _alive()
    p, d, name, ino = _lookup(path)
    if length < 0 or length > _getsize(p):
        raise ValueError("truncate can only shrink a file")
    _mutate("truncate", p)
    _truncate(p, length)
    ino.durable = min(ino.durable, length)


# ---------------------------------------------------------------- read-only

def _count_read(n):
    _st.stats["read_bytes"] = _st.stats.get("read_bytes", 0) + n


def read(path):
    """The whole content of a file."""
    _alive()
    with _open(_abspath(os.fspath(path)), "rb") as f:
        data = f.read()
    _count_read(len(data))
    return data


def read_at(path, offset, length):
    """Up to ``length`` bytes starting at byte ``offset`` of a file."""
    _alive()
    if offset < 0 or length < 0:
        raise ValueError("negative offset or length")
    with _open(_abspath(os.fspath(path)), "rb") as f:
        f.seek(offset)
        data = f.read(length)
    _count_read(len(data))
    return data


def exists(path):
    _alive()
    return _exists(_abspath(os.fspath(path)))


def size(path):
    _alive()
    return _getsize(_abspath(os.fspath(path)))


def listdir(path):
    """Names in a directory, in arbitrary order."""
    _alive()
    return _listdir(_abspath(os.fspath(path)))


# ---------------------------------------------------------------- verifier

def pending_changes(path):
    """Number of pending directory changes of ``path`` at the crash."""
    d = _abspath(os.fspath(path))
    return len(_st.pending.get(d, ()))


def crash_choices(path, limit=48):
    """Names of the directory outcomes the model allows for ``path`` that the
    verifier explores: all pending changes kept, none kept, and for each
    pending change, everything kept except that change and the later changes
    that must be lost with it."""
    n = pending_changes(path)
    drops = list(range(n))
    if n > limit:
        step = n / float(limit)
        drops = sorted({int(i * step) for i in range(limit)} | {n - 1})
    return ["all", "none"] + ["drop:%d" % i for i in drops]


def _surviving(changes, choice):
    if choice == "all":
        return list(changes)
    if choice == "none":
        return []
    k = int(choice.split(":")[1])
    lost_names = set(changes[k][1])
    keep = []
    for i, ch in enumerate(changes):
        if i < k:
            keep.append(ch)
            continue
        if i == k or lost_names & set(ch[1]):
            lost_names |= set(ch[1])
            continue
        keep.append(ch)
    return keep


def materialize(path, out, choice, data_mode):
    """Write to the new directory ``out`` the state directory ``path`` would
    have after the crash, for one directory outcome (see ``crash_choices``)
    and one content outcome (a ``MODES`` value)."""
    if not _st.crashed:
        raise RuntimeError("no crash has happened")
    d = _abspath(os.fspath(path))
    ns = dict(_st.dur.get(d, {}))
    for kind, names, ino in _surviving(_st.pending.get(d, []), choice):
        if kind == "create":
            ns[names[0]] = ino
        elif kind == "unlink":
            ns.pop(names[0], None)
        else:
            ns.pop(names[0], None)
            ns[names[1]] = ino
    op, crash_ino, crash_data = _st.crash_op
    _makedirs(out)
    for name, ino in ns.items():
        with _open(ino.loc, "rb") as f:
            data = f.read()
        durable = min(ino.durable, len(data))
        extra = len(data) - durable
        if data_mode == "none":
            keep = durable
        elif data_mode == "half":
            keep = durable + extra // 2
        else:
            keep = len(data)
        content = data[:keep]
        if data_mode == "torn" and op == "append" and crash_ino is ino and crash_data:
            content += crash_data[: len(crash_data) // 2]
        with _open(os.path.join(out, name), "wb") as f:
            f.write(content)
