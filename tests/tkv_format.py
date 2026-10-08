"""Independent implementation of the tinykv v3 on-disk format (FORMAT.md),
used by the verifier to inspect and fabricate database directories without
relying on the code under test."""

import json
import os
import re
import struct
import zlib

WAL_RE = re.compile(r"^wal-(\d+)\.log$")
SST_RE = re.compile(r"^sst-(\d+)\.sst$")
FOOTER = struct.Struct("<QIIQI4s")   # index_off, index_count, entry_count, max_seq, crc32(index), magic
IENTRY = struct.Struct("<QI")


class FormatError(AssertionError):
    pass


def crc(b):
    return zlib.crc32(b) & 0xFFFFFFFF


def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def read_varint(buf, pos):
    shift = 0
    val = 0
    while True:
        if pos >= len(buf):
            raise FormatError("truncated varint")
        b = buf[pos]
        pos += 1
        val |= (b & 0x7F) << shift
        if not b & 0x80:
            return val, pos
        shift += 7
        if shift > 70:
            raise FormatError("varint too long")


def read_str(buf, pos):
    n, pos = read_varint(buf, pos)
    if pos + n > len(buf):
        raise FormatError("truncated string")
    return bytes(buf[pos:pos + n]), pos + n


# ------------------------------------------------------------------ MANIFEST

def manifest_bytes(state):
    line = json.dumps(state, sort_keys=True, separators=(",", ":")).encode()
    return line + b"\n" + ("%08x" % crc(line)).encode() + b"\n"


def parse_manifest(data):
    parts = data.split(b"\n")
    if len(parts) != 3 or parts[2] != b"":
        raise FormatError("MANIFEST must be exactly two newline-terminated lines")
    if not re.fullmatch(rb"[0-9a-f]{8}", parts[1]):
        raise FormatError("MANIFEST crc line malformed")
    if int(parts[1], 16) != crc(parts[0]):
        raise FormatError("MANIFEST crc mismatch")
    state = json.loads(parts[0])
    if state.get("version") != 3:
        raise FormatError("MANIFEST version")
    for k in ("log_number", "next_file", "ssts", "last_seq"):
        if k not in state:
            raise FormatError("MANIFEST missing %s" % k)
    return state


# ------------------------------------------------------------------ WAL

def _rec(body):
    return struct.pack("<II", crc(body), len(body)) + body


def wal_put(seq, k, v):
    return _rec(b"\x01" + varint(seq) + varint(len(k)) + k + varint(len(v)) + v)


def wal_del(seq, k):
    return _rec(b"\x02" + varint(seq) + varint(len(k)) + k)


def wal_batch(first_seq, ops):
    out = [_rec(b"\x03" + varint(len(ops)))]
    for i, op in enumerate(ops):
        if op[0] == "put":
            out.append(wal_put(first_seq + i, op[1], op[2]))
        else:
            out.append(wal_del(first_seq + i, op[1]))
    out.append(_rec(b"\x04"))
    return b"".join(out)


def parse_wal(data):
    """Return ([(seq, key, value-or-None)], valid_len) per FORMAT.md."""
    pos = 0
    valid = 0
    ops = []
    batch = None
    while pos + 8 <= len(data):
        c, n = struct.unpack_from("<II", data, pos)
        end = pos + 8 + n
        if end > len(data):
            break
        body = data[pos + 8:end]
        if crc(body) != c or not body:
            break
        try:
            t = body[0]
            p = 1
            if t == 1:
                sq, p = read_varint(body, p)
                k, p = read_str(body, p)
                v, p = read_str(body, p)
                rec = ("op", (sq, k, v))
            elif t == 2:
                sq, p = read_varint(body, p)
                k, p = read_str(body, p)
                rec = ("op", (sq, k, None))
            elif t == 3:
                cnt, p = read_varint(body, p)
                rec = ("begin", cnt)
            elif t == 4:
                rec = ("commit",)
            else:
                break
            if p != len(body):
                break
        except FormatError:
            break
        if batch is None:
            if rec[0] == "op":
                ops.append(rec[1])
                valid = end
            elif rec[0] == "begin":
                batch = (rec[1], [])
            else:
                break
        else:
            if rec[0] == "op":
                batch[1].append(rec[1])
            elif rec[0] == "commit" and len(batch[1]) == batch[0]:
                ops.extend(batch[1])
                batch = None
                valid = end
            else:
                break
        pos = end
    return ops, valid


# ------------------------------------------------------------------ SST

def _entry_bytes(k, sq, v):
    if v is None:
        return b"\x01" + varint(len(k)) + k + varint(sq)
    return b"\x00" + varint(len(k)) + k + varint(sq) + varint(len(v)) + v


def sst_bytes(items):
    """items: [(key, seq, value-or-None)] sorted by key asc, seq desc."""
    data = bytearray()
    index = bytearray()
    ic = 0
    for start in range(0, len(items), 16):
        block = b"".join(_entry_bytes(*e) for e in items[start:start + 16])
        k = items[start][0]
        index += varint(len(k)) + k + IENTRY.pack(len(data), crc(block))
        ic += 1
        data += block
    index = bytes(index)
    max_seq = max([e[1] for e in items] + [0])
    return bytes(data) + index + FOOTER.pack(len(data), ic, len(items), max_seq,
                                             crc(index), b"TKV3")


def sst_layout(buf):
    """Footer fields and block boundaries: (footer_dict, [(first_key, off, end)])."""
    if len(buf) < FOOTER.size:
        raise FormatError("sst too short")
    ioff, ic, ec, mx, icrc, magic = FOOTER.unpack_from(buf, len(buf) - FOOTER.size)
    end = len(buf) - FOOTER.size
    if magic != b"TKV3":
        raise FormatError("bad magic")
    if ioff > end:
        raise FormatError("bad index offset")
    index = buf[ioff:end]
    if crc(index) != icrc:
        raise FormatError("index crc mismatch")
    blocks = []
    pos = 0
    for _ in range(ic):
        k, pos = read_str(index, pos)
        off, bcrc = IENTRY.unpack_from(index, pos)
        pos += IENTRY.size
        blocks.append([k, off, None, bcrc])
    if pos != len(index):
        raise FormatError("trailing bytes in index")
    for i, b in enumerate(blocks):
        b[2] = blocks[i + 1][1] if i + 1 < len(blocks) else ioff
    footer = {"index_offset": ioff, "index_count": ic, "entry_count": ec,
              "max_seq": mx, "index_len": end - ioff, "footer_len": FOOTER.size}
    return footer, blocks


def parse_sst(buf):
    """Strictly validate an SST and return its [(key, seq, value-or-None)]."""
    footer, blocks = sst_layout(buf)
    items = []
    starts = []
    pos = 0
    ioff = footer["index_offset"]
    while pos < ioff:
        starts.append(pos)
        flag = buf[pos]
        k, pos = read_str(buf, pos + 1)
        sq, pos = read_varint(buf, pos)
        if flag == 0:
            v, pos = read_str(buf, pos)
        elif flag == 1:
            v = None
        else:
            raise FormatError("bad entry flag")
        items.append((k, sq, v))
    if pos != ioff:
        raise FormatError("data section overruns index")
    if len(items) != footer["entry_count"]:
        raise FormatError("entry_count mismatch")
    for a, b in zip(items, items[1:]):
        if not (a[0], -a[1]) < (b[0], -b[1]):
            raise FormatError("entries not sorted by (key asc, seq desc)")
    expect = list(range(0, len(items), 16))
    if len(blocks) != len(expect):
        raise FormatError("index_count mismatch")
    for i, (k, off, end, bcrc) in zip(expect, blocks):
        if k != items[i][0] or off != starts[i]:
            raise FormatError("index entry for entry %d wrong" % i)
        if crc(buf[off:end]) != bcrc:
            raise FormatError("block crc mismatch at offset %d" % off)
    if footer["max_seq"] != max([e[1] for e in items] + [0]):
        raise FormatError("footer max_seq is not the largest seq in the table")
    return items


# ------------------------------------------------------------------ whole DB

def load_db(path):
    """Read a closed database directory strictly according to FORMAT.md and
    check its cross-file invariants.

    Returns (state, info): state maps key -> latest value for live keys."""
    names = os.listdir(path)
    with open(os.path.join(path, "MANIFEST"), "rb") as f:
        man = parse_manifest(f.read())
    versions = {}      # key -> [(seq, value)] in file order, oldest file first
    tables = {}
    for sid in sorted(int(s) for s in man["ssts"]):
        with open(os.path.join(path, "sst-%d.sst" % sid), "rb") as f:
            items = parse_sst(f.read())
        tables[sid] = items
        per_key = {}
        for k, sq, v in items:
            per_key.setdefault(k, []).append((sq, v))
        for k, vs in per_key.items():
            prev = versions.get(k)
            if prev and min(s for s, _ in vs) <= max(s for s, _ in prev):
                raise FormatError("table sst-%d holds a version of %r that is not "
                                  "newer than an older table's" % (sid, k))
            versions.setdefault(k, []).extend(sorted(vs))
    wals = sorted(int(WAL_RE.match(n).group(1)) for n in names
                  if WAL_RE.match(n) and int(WAL_RE.match(n).group(1)) >= man["log_number"])
    wal_info = {}
    wal_ops = []
    for w in wals:
        with open(os.path.join(path, "wal-%d.log" % w), "rb") as f:
            data = f.read()
        ops, valid = parse_wal(data)
        wal_info[w] = (len(data), valid)
        wal_ops.extend(ops)
    for a, b in zip(wal_ops, wal_ops[1:]):
        if not a[0] < b[0]:
            raise FormatError("log sequence numbers not increasing")
    for sq, k, v in wal_ops:
        prev = versions.get(k)
        if prev and sq <= max(s for s, _ in prev):
            raise FormatError("log version of %r is not newer than its table versions" % k)
        versions.setdefault(k, []).append((sq, v))
    max_seq = max([s for vs in versions.values() for s, _ in vs] + [0])
    state = {}
    for k, vs in versions.items():
        latest = max(vs, key=lambda e: e[0])
        if latest[1] is not None:
            state[k] = latest[1]
    return state, {"manifest": man, "tables": tables, "wals": wal_info,
                   "wal_ops": wal_ops, "versions": versions, "max_seq": max_seq,
                   "names": names}
