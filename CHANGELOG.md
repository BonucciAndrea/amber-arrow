# Changelog

## 2.9.0

- **Arrow sees Amber's types now**: every int goes out as int64 (a byte column went out as uint8, so -1 read 255),
  0N is a real null, and dates, months, timestamps, timespans, times, seconds and minutes go out as Arrow's own
  temporal types. Tables with those used to fail to export at all.
- **Any Arrow table comes in**: every int and float width, decimals, dictionaries, string views, every temporal
  type, sliced and chunked data, nulls. The engine's importer read a float32 as 8-byte ints, past the end of its
  buffer, and ignored offsets, so a slice came in as the wrong rows. Nested and binary columns say so.
- **A stream comes in once**: 500 batches cost one copy into the engine, not 500 concatenations.
- **One engine, always**: libamber_arrow no longer links libamber, and finds the one python-amber is running, so
  a repaired wheel can't drag in a second engine.
- **Threads**: refcounts are atomic and the engine is only touched from the engine's side, so PyArrow and gRPC can
  release batches on any thread.
- **amberd**: `--readonly` stopped refusing every `select` with a named column, a 600 KB query isn't refused any
  more, a client that stops reading gets dropped after 30 s instead of stalling everyone, `--load` goes through
  the real loader (`.q`, multi-line qSQL, CRLF), and bad ports and addresses fail at startup.
- **amber-flight**: the same read-only rule as amberd, engine errors come back with their text, upload names are
  checked, and concurrent clients are serialised around the engine.
- 132 tests, up from 34.

## 2.0.0 — first release

Streaming Apache Arrow over the Amber engine, the `amberd` query server, and an Arrow Flight daemon.

Tracks Amber engine release 2.0.0, which added the `libamber.so` dynamic C API
this package is built on. Everything here lives outside the engine repository and
reaches it only through that documented seam.

- **`ArrowArrayStream`** over an Amber table — the one piece of the Arrow C Data
  Interface the engine deliberately does not carry. Batches are **windows** onto
  a single export, expressed with Arrow's own `offset` and `length`, so streaming
  400 million rows in 400 batches copies nothing.
- **Lifetime done properly.** The parent export is reference-counted, not owned
  by the stream, so a batch may outlive the stream and its siblings.
- **`amberd`** — a TCP query server: many connections, one single-threaded engine,
  shared state, four reply encodings, and a `\parse` directive that syntax-checks
  without evaluating.
- **Arrow Flight** — a gRPC daemon where a ticket *is* an Amber query, with an
  embedded (zero-copy) backend and a remote one.
- 34 tests against a real server on a real socket, and a real gRPC endpoint.

### Licence

GNU AGPLv3, matching the engine.
