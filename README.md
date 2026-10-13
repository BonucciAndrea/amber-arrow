# amber-arrow & amber-flight

**Streaming Apache Arrow over the [Amber](https://github.com/BonucciAndrea/amber) columnar engine, without copying anything — plus a query daemon and an Arrow Flight server.**

```python
import amber as am, amber_arrow as aa

am.gentq(400_000_000)                                    # 400M trades
for batch in aa.query("select from trades", batch_rows=1_000_000):
    process(batch)                                       # 400 batches, 0 copies
```

Every batch above points at the **same** engine-owned column buffers. Not a copy
of them, not a slice of them — a window, expressed with Arrow's own `offset` and
`length` fields.

```python
>>> {b.column("px").buffers()[1].address for b in aa.stream(t, batch_rows=25_000)}
{140234099785792}                                        # one address, 80 batches
>>> _.pop() == am.Table(t).column("px", raw=True).handle()
True                                                     # the engine's buffer
```

---

## What is in here

| | |
|---|---|
| `libamber_arrow.so` | C. Implements `ArrowArrayStream` over an Amber table — the one piece of the Arrow C Data Interface the engine deliberately does not carry. Links `libamber.so`, and nothing else. **No libarrow.** |
| `amberd` | C. A TCP query server: many connections, one engine, four reply encodings. ~450 lines. The thing the Grafana datasource and the VS Code language server connect to. |
| `amber_arrow` | Python. ctypes over `libamber_arrow.so` → `pyarrow.RecordBatchReader`, plus a reference client for the `amberd` protocol. |
| `amber_flight` | Python. An Arrow Flight (gRPC) server. A ticket *is* an Amber query. |

Nothing here modifies the engine, and the engine does not know any of it exists.
The entire contract is section 6 of the engine's `src/ext.h`, checked at load time
via `amber_abi_version()`.

---

## Why a separate library

The engine already exports Arrow: `src/ar.c` produces one `ArrowSchema` /
`ArrowArray` pair for a whole table, in about ninety lines with no Arrow
dependency. That is exactly the right amount of Arrow for a 10,000-line C runtime
to own. It covers "hand me this table" completely.

What it does not cover is *"hand me this 400-million-row table in batches, and
stop when I stop asking"* — which is what a Flight server, a Polars scan, and any
consumer with a memory budget actually need. That is the `ArrowArrayStream`, and
it belongs out here, where it can grow, rather than inside the engine, where it
would be the largest single feature in the tree and would be paid for by every
user who never touches Arrow.

### A batch is a window, not a slice

The obvious way to stream a table in batches is to cut it into pieces. That copies
every byte, once per stream — for a 400M-row table, that *is* the cost of the
operation, and it defeats the point of using Arrow at all.

So this library never cuts anything. It exports the table **once**, and every
batch is a set of `ArrowArray` structs whose `buffers` pointers are the parent's,
with `offset` and `length` moved. Arrow's format defines exactly those semantics
for primitive and utf8 layouts alike, so there is no per-type special case.

That leaves one genuinely hard problem, and it is a lifetime problem: **batch 3
may outlive the stream**, and batches 1 and 2 may be released before batch 3 is
created. So the parent export is not owned by the stream. It is owned by a
reference count that the stream holds one of, and each live batch holds one of,
and it is torn down by whoever drops the last one. There is a test for exactly
that pattern, because it is the one that would otherwise be a use-after-free.

---

## Install

```bash
git clone https://github.com/BonucciAndrea/amber          # the engine
git clone https://github.com/BonucciAndrea/amber-arrow
cd amber-arrow
./build.sh --amberd                                       # libamber_arrow.so + amberd
pip install ./python                                      # amber_arrow + amber_flight
pip install amber-lang                                    # the engine, for the embedded backend
```

### Pointing the build at the engine

`build.sh` produces `build/libamber_arrow.so` (and, with `--amberd`,
`build/amberd`). `amberd` links `libamber.so`. `libamber_arrow.so` deliberately
does NOT: its `amber_*` calls bind to whichever engine is already in the process
(see "One engine, not two"), so a C program links both, `-lamber_arrow -lamber`.
The engine is only needed to build `amberd`:

| variable | meaning | default |
|---|---|---|
| `AMBER_SRC` | an engine **checkout** — `build.sh` will build `libamber.so` here if it is missing | `../amber` |
| `AMBER_LIB_DIR` | a directory that already contains a **prebuilt** `libamber.so`/`.dylib` | `$AMBER_SRC` |

So with a prebuilt engine already on disk, point `build.sh` straight at it and
nothing is compiled twice:

```bash
AMBER_LIB_DIR=/opt/amber ./build.sh --amberd              # use a prebuilt libamber.so
```

The resulting objects carry an `-rpath` to that directory, so they load the
engine without `LD_LIBRARY_PATH` being set.

### Running against the standard library

`AMBER_SRC`/`AMBER_LIB_DIR` are **build-time** and locate the shared library.
At **run time**, `amberd`, `amber-flight` and the test suite need the `.k`
standard-library modules (`amber.k`, `qsql.k`, `fin.k`, …). Point them at that
directory with `--home DIR` or the `$AMBER_HOME` environment variable — usually
the same engine checkout:

```bash
export AMBER_HOME=/opt/amber        # the directory holding amber.k, qsql.k, ...
./build/amberd --home "$AMBER_HOME" --port 5010 --eval "gentq 10000000" -v
```

Without a home directory the engine still runs, but as a **bare engine**:
primitives only, no qSQL and no `gentq`.

### One engine, not two

`amber_arrow` asks python-amber which `libamber` it is actually running
(`amber._amber.library_path()`), makes that one global, and only then loads
`libamber_arrow.so` — which has no dependency on libamber of its own, so there is
nothing for the loader to go looking for. That matters more than it looks: a
wheel repaired by auditwheel or delocate renames and moves its libamber, and a
`DT_NEEDED` on `libamber.so` would find a *second* copy somewhere — a second
**engine**, two heaps, two symbol tables, handles from one meaningless in the
other, and no error anywhere.

`amber_arrow.library_path()` tells you which one you got.

---

## The Python API

```python
import amber_arrow as aa

aa.query("select from trades where px>0", batch_rows=1_000_000)  # -> RecordBatchReader
aa.stream(table, batch_rows=0)                                   # -> RecordBatchReader
aa.table_to_arrow(table)                                         # -> pyarrow.Table
aa.arrow_to_table(pyarrow_table_or_batch_or_reader)              # -> amber.Table
aa.library_path()
```

`batch_rows=0` selects 65,536 — the figure the Arrow ecosystem has converged on
for streaming: large enough that per-batch overhead disappears, small enough that
a handful of columns stays inside L2/L3.

A keyed result (`select … by sym from t`) is flattened with the grouping columns
in front. Arrow has no keyed table, and dropping the keys would be worse than
flattening them.

**What Arrow sees.** Every integer column is `int64` (in Amber `type 1 2 3` is a
long however narrow it is stored), `0N` is a real null, symbols and lists of
strings are `utf8`, and dates, months, timestamps, timespans, times, seconds and
minutes are `date32`, `timestamp[ns]`, `duration[ns]` and `time32`. Floats, longs,
timespans and times are the engine's own memory; the rest is laid out once.

**The inbound direction copies.** Amber stores a 32-byte object header
immediately *before* every vector payload, so it cannot adopt a foreign
allocation as a native column. Any Arrow column comes in: every int and float
width, bools, utf8 / large / view strings, dictionaries, decimals, dates,
timestamps (any unit, any zone: as UTC), durations and times, sliced or chunked,
nulls as Amber's nulls. Nested and binary columns are refused with their name.

**Threads.** Batches can be released on any thread. The engine is not
thread-safe, so the memory of an export whose last batch went on another thread
goes back to the engine on the next `amber_arrow` call, or on
`amber_arrow.collect()`.

---

## `amberd` — the query server

The engine speaks sockets as a **client** (`ipc.k`'s `hopen`/`hsync`, on the
`connect()` in `src/i.c`) but has no listener, and should not grow one: a listener
is a policy decision — which port, which framing, which authentication, which
concurrency model — and the engine's job is to be a runtime.

```bash
./build/amberd --home ../amber --port 5010 --eval "gentq 10000000" -v
./build/amberd --home ../amber --load /srv/hdb/load.k --mode jsonc --readonly
```

### Concurrency

Single-threaded event loop over `poll()`: many connections, one query at a time,
**shared** global state. That is not a compromise — it is the model kdb+ uses, and
it is the only one where two clients can see each other's writes. Forking per
connection would buy parallelism and lose the shared table you connected in order
to query. A long query blocks the loop; that is visible, predictable, and fixed by
running more `amberd` processes over a partitioned database, which is also what
kdb+ does.

### Protocol v1

A request is one line of UTF-8 terminated by `\n`. A reply is a header line
followed by exactly that many bytes:

```
+<nbytes>\n<payload>     success
-<nbytes>\n<payload>     error — the payload is the engine's own diagnostic text
```

| Request | Effect |
|---|---|
| `\mode text\|json\|jsonc\|raw` | set the reply encoding |
| `\ping` | → `pong` |
| `\version` | → `2.0.0` |
| `\quit` | close |
| anything else | evaluate it — bare qSQL included |

| Mode | Payload |
|---|---|
| `text` | rendered as the REPL renders it, ANSI stripped |
| `json` | row-oriented `[{"sym":"AAPL","px":187.2}, …]` |
| `jsonc` | column-oriented `{"sym":["AAPL",…],"px":[187.2,…]}` ← what dashboards want |
| `raw` | **no header**, payload only — for the engine's own `ipc.k` client |

`raw` exists for compatibility with `hsync`, which writes a bare query and reads
until a short read. Do not use it from code: without a length there is no way to
tell a complete reply from a truncated one.

The protocol is small enough to reimplement in an afternoon in any language,
which is the point — the Grafana backend implements it in Go, the language server
implements it in TypeScript, and `amber_arrow.client.AmberClient` is the
reference implementation in Python.

```python
from amber_arrow.client import AmberClient

with AmberClient("localhost", 5010) as c:
    c.ping()
    rows  = c.json("select vwap:wavg[sz;px] by sym from trades")
    cols  = c.columns("select from trades where sym=`AAPL")
    frame = c.pandas("select n:#px by sym from trades")
```

---

## `amber-flight` — Arrow over gRPC

```bash
python -m amber_flight --home ../amber --port 8815 --eval "gentq 10000000"
```

```python
import pyarrow.flight as fl
c = fl.connect("grpc://localhost:8815")

for info in c.list_flights():                            # every table, real schemas
    print(info.descriptor.path, info.total_records)

t = c.do_get(fl.Ticket(b"select vwap:wavg[sz;px] by sym from trades")).read_all()

w, _ = c.do_put(fl.FlightDescriptor.for_path("bars"), table.schema)
w.write_table(table); w.close()                          # binds the global `bars`
```

**A ticket is an Amber query.** There is no query language of its own to learn and
nothing to translate; bare qSQL works exactly as it does at the `amber>` prompt.

### Two backends, and the difference is the whole story

`embedded` (default) runs the engine in this process via `python-amber` and
streams with `amber-arrow`. A batch handed to gRPC points at the engine's own
column buffers: **the result is never copied**, not once, between evaluation and
the socket.

`remote` fronts an `amberd` on another host and pulls columnar JSON, which it
converts to Arrow. That is a real copy and a real parse. It is here because
sometimes the engine genuinely is somewhere else — a tickerplant, an HDB node —
and a slower path beats no path. Do not reach for it when the engine could have
been in-process.

`list_flights` computes its catalogue rather than guessing: Amber has no
catalogue (tables are just globals), so every global that *is* a table is
reported, with its real row count and its real Arrow schema, obtained by
streaming a zero-row slice of it.

---

## Security

There is none, deliberately and visibly. `amberd` and `amber-flight` both let a
client evaluate arbitrary Amber, which includes reading files and spawning
processes. Both bind loopback by default and refuse a public address without
`--i-know-this-is-unauthenticated`.

`--readonly` rejects requests that *look* like mutations. It is a syntactic
filter and is labelled as one: Amber is homoiconic, so `. "t:1"` assigns without
any of the tokens it scans for, and no token scan can ever close that. It exists
to stop an **accident** — a dashboard panel with a typo, a query built by string
concatenation — not an attacker. The real boundary is not exposing the port.

---

## Tests

```bash
./build.sh --amberd                 # builds build/libamber_arrow.so + build/amberd
export AMBER_HOME=/opt/amber         # the .k standard library (see "Running", above)
pip install ./python                 # amber_arrow + amber_flight, into a venv
pip install amber-lang pytest        # the embedded backend the tests stream through
python -m pytest tests -q
```

The tests need `python-amber` and `pyarrow` importable and `$AMBER_HOME` set;
each fixture `importorskip`s what it cannot find, so a partial install skips
rather than fails. If `libamber_arrow.so` is somewhere other than `build/`, point
`$AMBER_ARROW_LIB` at it.

132 tests, covering: every Amber type out and every Arrow type in, with exact
values and nulls; that every batch shares one buffer address and that the address
is the engine's; batches outliving their stream and released on other threads;
leaks; that only one engine is ever loaded; the full `amberd` protocol and its
read-only guard, limits and bad clients against a real server on a real socket;
and Flight discovery, streaming, upload, actions and concurrent clients against a
real gRPC server.

---

## Licence

GNU AGPLv3, matching the engine (`LICENSE`, `NOTICE`). The Arrow C Data Interface
declarations in `include/arrow_c_abi.h` are reproduced from Apache Arrow under
the Apache Licence 2.0.
