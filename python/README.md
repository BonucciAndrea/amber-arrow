# amber-arrow · amber-flight

The Python half of [amber-arrow](https://github.com/BonucciAndrea/amber-arrow):
streaming Apache Arrow over the Amber columnar time-series engine, a reference
client for the `amberd` wire protocol, and an Arrow Flight server.

```python
import amber as am, amber_arrow as aa

am.gentq(400_000_000)
for batch in aa.query("select from trades", batch_rows=1_000_000):
    process(batch)                  # 400 batches, 0 copies
```

Every batch is a **window** onto one export — the same engine-owned column
buffers, with Arrow's `offset` and `length` moved — not a slice of it.

## Install

```bash
pip install amber-arrow            # this package (needs pyarrow)
pip install amber                  # python-amber, for the embedded backend
```

`libamber_arrow.so` is built from the repository root:

```bash
cd ..  &&  ./build.sh --amberd
```

`amber_arrow` finds it via `$AMBER_ARROW_LIB`, a copy bundled in the package, or
`../build/`. `amber_arrow.library_path()` reports which one it loaded — which
matters, because loading a second copy of `libamber.so` means a second **engine**.

## What is here

| | |
|---|---|
| `amber_arrow` | ctypes over `libamber_arrow.so` → `pyarrow.RecordBatchReader`; `query`, `stream`, `table_to_arrow`, `arrow_to_table` |
| `amber_arrow.client` | `AmberClient` — the reference implementation of the `amberd` protocol, in the language the protocol is easiest to read in |
| `amber_flight` | `python -m amber_flight` — an Arrow Flight (gRPC) server. A ticket *is* an Amber query. |

## Documentation

The full design notes — why batching is windowing rather than slicing, how the
parent export's lifetime is reference-counted so a batch may outlive its stream,
the `amberd` protocol specification, and the security posture — are in the
[repository README](https://github.com/BonucciAndrea/amber-arrow#readme).

## Licence

GNU AGPLv3, matching the engine.
