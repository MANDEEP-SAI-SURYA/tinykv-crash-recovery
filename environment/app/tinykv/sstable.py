"""Immutable sorted tables.  See FORMAT.md for the file layout."""

import bisect
import struct

from . import fs
from .encoding import CorruptionError, crc32, get_bytes, get_varint, put_varint

MAGIC = b"TKV3"
# index_offset, index_count, entry_count, max_seq, crc32(index), magic
FOOTER = struct.Struct("<QIIQI4s")
IENTRY = struct.Struct("<QI")      # block offset, crc32(block)
INDEX_INTERVAL = 16

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


def write_sst(path, items):
    """Write ``items`` -- (key, seq, value-or-None) triples sorted by key
    ascending then seq descending; None is a tombstone -- to a new table at
    ``path``."""
    data = bytearray()
    index = bytearray()
    icount = 0
    max_seq = 0
    prev = None
    for start in range(0, len(items), INDEX_INTERVAL):
        block = bytearray()
        for key, seq, val in items[start:start + INDEX_INTERVAL]:
            cur = (key, -seq)
            if prev is not None and cur <= prev:
                raise ValueError("sstable entries out of order")
            prev = cur
            max_seq = max(max_seq, seq)
            block += encode_entry(key, seq, val)
        first = items[start][0]
        index += put_varint(len(first)) + first + IENTRY.pack(len(data), crc32(block))
        icount += 1
        data += block
    index = bytes(index)
    footer = FOOTER.pack(len(data), icount, len(items), max_seq, crc32(index), MAGIC)
    fs.create(path)
    fs.append(path, bytes(data) + index + footer)


class SSTReader:
    def __init__(self, path):
        self.path = path
        buf = fs.read(path)
        if len(buf) < FOOTER.size:
            raise CorruptionError("%s: too short" % path)
        index_off, icount, ecount, max_seq, icrc, magic = FOOTER.unpack_from(
            buf, len(buf) - FOOTER.size)
        index_end = len(buf) - FOOTER.size
        if magic != MAGIC or index_off > index_end:
            raise CorruptionError("%s: bad footer" % path)
        index = buf[index_off:index_end]
        if crc32(index) != icrc:
            raise CorruptionError("%s: index checksum mismatch" % path)
        self.buf = buf
        self.data_end = index_off
        self.entry_count = ecount
        self.max_seq = max_seq
        self.index_keys = []
        self.index_offs = []
        pos = 0
        ends = []
        crcs = []
        for _ in range(icount):
            key, pos = get_bytes(index, pos)
            off, bcrc = IENTRY.unpack_from(index, pos)
            pos += IENTRY.size
            self.index_keys.append(key)
            self.index_offs.append(off)
            crcs.append(bcrc)
        if pos != len(index):
            raise CorruptionError("%s: bad index" % path)
        ends = self.index_offs[1:] + [index_off]
        for i, (off, end) in enumerate(zip(self.index_offs, ends)):
            if crc32(buf[off:end]) != crcs[i]:
                raise CorruptionError("%s: block %d checksum mismatch" % (path, i))

    def _entry(self, pos):
        flag = self.buf[pos]
        key, pos = get_bytes(self.buf, pos + 1)
        seq, pos = get_varint(self.buf, pos)
        if flag == FLAG_TOMBSTONE:
            return key, seq, None, pos
        if flag != FLAG_VALUE:
            raise CorruptionError("%s: bad entry flag" % self.path)
        val, pos = get_bytes(self.buf, pos)
        return key, seq, val, pos

    def get(self, key, max_seq):
        """Newest version of ``key`` with seq <= max_seq: its value, None for
        a tombstone, or MISSING if the table has no such version."""
        i = bisect.bisect_right(self.index_keys, key) - 1
        if i < 0:
            return MISSING
        pos = self.index_offs[i]
        while pos < self.data_end:
            k, seq, v, pos = self._entry(pos)
            if k < key:
                continue
            if k > key:
                break
            if seq <= max_seq:
                return v
        return MISSING

    def items(self):
        pos = 0
        while pos < self.data_end:
            k, seq, v, pos = self._entry(pos)
            yield k, seq, v
