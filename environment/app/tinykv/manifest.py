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
    fs.rename(tmp, os.path.join(dirpath, NAME))
