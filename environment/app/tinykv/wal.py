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
    and the length of the prefix made of complete, valid records.  Reading
    stops at the first torn or corrupt record.
    """
    entries = []
    pos = 0
    in_batch = False
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
        if typ in (PUT, DEL):
            entries.append(val)
        elif typ == BATCH_BEGIN:
            in_batch = True
        elif typ == BATCH_COMMIT:
            if not in_batch:
                break
            in_batch = False
        pos = end
    return entries, pos


class WalWriter:
    """Appends records to one log file.

    To keep write latency down, the log is fsync'd once every ``sync_every``
    appends (group commit) instead of after every single write.
    """

    def __init__(self, path, number, sync_every=8):
        self.path = path
        self.number = number
        self.sync_every = sync_every
        self._unsynced = 0

    def add(self, ops, first_seq):
        fs.append(self.path, encode_ops(ops, first_seq))
        self._unsynced += 1
        if self._unsynced >= self.sync_every:
            self.sync()

    def sync(self):
        fs.fsync(self.path)
        self._unsynced = 0
