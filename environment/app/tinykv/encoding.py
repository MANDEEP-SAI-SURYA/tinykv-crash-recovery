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
