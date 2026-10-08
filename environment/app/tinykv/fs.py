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

Local simulator
---------------
The crash simulation implemented in THIS copy of the module covers only the
file-content part of the model: ``create``, ``rename`` and ``unlink`` are
applied to disk immediately and are never undone by a simulated crash, and
``fsync_dir`` only counts as an operation.  At a crash, every file is cut
back according to ``mode``: ``"none"`` drops all non-durable bytes,
``"half"`` keeps the first half of each file's non-durable suffix, ``"all"``
keeps everything, and ``"torn"`` also writes the first half of the bytes of
the append that crashed.  The grader uses an implementation of the complete
model described above, including lost and reordered directory changes.
"""


import builtins
import os

__all__ = [
    "SimulatedCrash", "MODES", "reset", "arm_crash", "disarm", "op_count",
    "stats", "touched_paths", "has_crashed", "ensure_dir", "create", "append",
    "fsync", "fsync_dir", "rename", "unlink", "truncate", "read", "read_at",
    "exists", "size", "listdir",
]

# Captured at import time so the engine's I/O keeps working even if a test
# harness wraps the builtins to detect bypasses.
_open = builtins.open
_replace = os.replace
_unlink = os.unlink
_truncate = os.truncate
_listdir = os.listdir
_makedirs = os.makedirs
_exists = os.path.exists
_getsize = os.path.getsize
_abspath = os.path.abspath

MODES = ("none", "half", "all", "torn")


class SimulatedCrash(BaseException):
    """Raised when the simulated machine loses power."""


class _State:
    def __init__(self):
        self.durable = {}        # abs path -> durable length in bytes
        self.ops = 0
        self.crash_after = None
        self.mode = "none"
        self.crashed = False
        self.stats = {}
        self.touched = set()


_st = _State()


def reset():
    """Start a new simulated process lifetime (all files on disk are durable)."""
    global _st
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


def _key(path):
    return _abspath(os.fspath(path))


def _alive():
    if _st.crashed:
        raise SimulatedCrash("the simulated process has already crashed")


def _track(p):
    if p not in _st.durable:
        _st.durable[p] = _getsize(p) if _exists(p) else 0


def _crash(op, p, data):
    _st.crashed = True
    mode = _st.mode
    if op == "append" and mode == "torn" and data:
        with _open(p, "ab") as f:
            f.write(data[: len(data) // 2])
    for path, durable in list(_st.durable.items()):
        if not _exists(path):
            continue
        cur = _getsize(path)
        if cur <= durable:
            continue
        if mode == "none":
            keep = durable
        elif mode == "half":
            keep = durable + (cur - durable) // 2
        else:
            keep = cur
        _truncate(path, keep)
    raise SimulatedCrash(
        "simulated crash before mutating op #%d (%s %s, mode=%s)"
        % (_st.ops + 1, op, path_display(p), mode))


def path_display(p):
    return os.path.basename(p)


def _mutate(op, p, data=None):
    _alive()
    if _st.crash_after is not None and _st.ops >= _st.crash_after:
        _crash(op, p, data)
    _st.ops += 1
    _st.stats[op] = _st.stats.get(op, 0) + 1
    _st.touched.add(p)


# ---------------------------------------------------------------- mutating

def ensure_dir(path):
    """Create a directory (and parents) if missing.  Not counted as an op."""
    _alive()
    _makedirs(_key(path), exist_ok=True)


def create(path):
    """Create a new empty file.  Raises FileExistsError if it exists."""
    p = _key(path)
    _alive()
    if _exists(p):
        raise FileExistsError(p)
    _mutate("create", p)
    with _open(p, "xb"):
        pass
    _st.durable[p] = 0


def append(path, data):
    """Append bytes to an existing file (not durable until fsync)."""
    p = _key(path)
    data = bytes(data)
    _alive()
    if not _exists(p):
        raise FileNotFoundError(p)
    _track(p)
    _mutate("append", p, data)
    with _open(p, "ab") as f:
        f.write(data)


def fsync(path):
    """Make the current content of ``path`` durable."""
    p = _key(path)
    _alive()
    if not _exists(p):
        raise FileNotFoundError(p)
    _track(p)
    _mutate("fsync", p)
    _st.durable[p] = _getsize(p)


def fsync_dir(path):
    """Make every pending change of directory ``path`` durable.  (In this
    local simulator directory changes are already durable; see above.)"""
    p = _key(path)
    _alive()
    if not _exists(p):
        raise FileNotFoundError(p)
    _mutate("fsync_dir", p)


def rename(src, dst):
    """Atomically rename src to dst, replacing dst if it exists."""
    s, d = _key(src), _key(dst)
    _alive()
    if not _exists(s):
        raise FileNotFoundError(s)
    _track(s)
    _mutate("rename", s)
    _st.touched.add(d)
    _replace(s, d)
    _st.durable[d] = _st.durable.pop(s)


def unlink(path):
    p = _key(path)
    _alive()
    if not _exists(p):
        raise FileNotFoundError(p)
    _mutate("unlink", p)
    _unlink(p)
    _st.durable.pop(p, None)


def truncate(path, length):
    """Shrink a file to ``length`` bytes (durable immediately)."""
    p = _key(path)
    _alive()
    if not _exists(p):
        raise FileNotFoundError(p)
    if length < 0 or length > _getsize(p):
        raise ValueError("truncate can only shrink a file")
    _track(p)
    _mutate("truncate", p)
    _truncate(p, length)
    _st.durable[p] = min(_st.durable[p], length)


# ---------------------------------------------------------------- read-only

def _count_read(n):
    _st.stats["read_bytes"] = _st.stats.get("read_bytes", 0) + n


def read(path):
    """The whole content of a file."""
    _alive()
    with _open(_key(path), "rb") as f:
        data = f.read()
    _count_read(len(data))
    return data


def read_at(path, offset, length):
    """Up to ``length`` bytes starting at byte ``offset`` of a file."""
    _alive()
    if offset < 0 or length < 0:
        raise ValueError("negative offset or length")
    with _open(_key(path), "rb") as f:
        f.seek(offset)
        data = f.read(length)
    _count_read(len(data))
    return data


def exists(path):
    _alive()
    return _exists(_key(path))


def size(path):
    _alive()
    return _getsize(_key(path))


def listdir(path):
    """Names in a directory, in arbitrary order."""
    _alive()
    return _listdir(_key(path))
