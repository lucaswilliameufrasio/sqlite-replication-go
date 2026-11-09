# SQLite WAL parser vectors

These deterministic parser vectors use the SQLite WAL on-disk field layout and
checksum algorithm documented by `spec/WAL.md`. They are test data, not evidence
of safe concurrent capture or checkpoint behavior.

Generate the files once with `python3 conformance/generate-wal-fixtures.py`;
the generator refuses to overwrite existing vectors. Each contains a valid
header and three frames: one uncommitted page, a two-frame commit with logical
database size 2, then an uncommitted page. `sqlite-le.wal` and
`sqlite-be.wal` use 512-byte pages; `sqlite-le-65536.wal` covers the maximum
SQLite WAL page size, stored literally as 65536 in the WAL header. The `le`
fixtures use magic `0x377f0682` and little-endian checksum words; `sqlite-be.wal`
uses magic `0x377f0683` and big-endian checksum words. Checksum values and all
other multibyte fields are big-endian.

SHA-256:

| File | SHA-256 |
|---|---|
| `sqlite-le.wal` | `2da800ef76e53e1533ea31e1bdbac3d1f17b2b1919827c0d6d20473ecc8b3b18` |
| `sqlite-be.wal` | `e9ab36f78b9616068e25c317391f482fd27e69ebb278911f3d4be88a14ec6812` |
| `sqlite-le-65536.wal` | `c8f43fa1aa7c265f4ed3ce6ce60e323e7634bcf2572dc95ccef68a8bfaf81de3` |

The Go and Rust parsers consume these same bytes. Their tests also mutate header
checksums, frame salts, frame data, and format version, and verify that an
incomplete tail never advances the last committed boundary. These tests cover
parsing only; CP-01 remains blocked on deterministic SQLite concurrency,
checkpoint, spool-failure, and crash-recovery evidence.
