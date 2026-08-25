# Changelog

## 1.9.6 — first release

Streaming Apache Arrow over the Amber engine, the `amberd` query server, and an Arrow Flight daemon.

Tracks Amber engine release 1.9.6, which added the `libamber.so` dynamic C API
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
