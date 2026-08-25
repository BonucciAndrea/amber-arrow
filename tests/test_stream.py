"""The streaming Arrow bridge, and the claim that it does not copy."""
import gc

import pytest

pa = pytest.importorskip("pyarrow")


def test_stream_covers_every_row(am, aa):
    table = am.q("select from trades", raw=True)
    total = int(am.eval("#trades"))
    seen = batches = 0
    for batch in aa.stream(table, batch_rows=50_000):
        seen += batch.num_rows
        batches += 1
    assert seen == total
    assert batches == -(-total // 50_000)


def test_every_batch_aliases_one_buffer(am, aa):
    """The point of the whole library: batching must not mean copying.

    Every batch is a window onto the SAME export, so every batch's `px` buffer
    must have the same base address -- and that address must be the engine's own
    column payload, not a copy of it.
    """
    table = am.q("select from trades", raw=True)
    payload = am.Table(table).column("px", raw=True).handle()
    addresses = {b.column("px").buffers()[1].address
                 for b in aa.stream(table, batch_rows=25_000)}
    assert len(addresses) == 1
    assert addresses.pop() == payload


def test_offsets_advance_across_batches(am, aa):
    """Windows, not slices: consecutive batches expose consecutive rows."""
    table = am.q("select from trades", raw=True)
    first = next(iter(aa.stream(table, batch_rows=1000)))
    wide = next(iter(aa.stream(table, batch_rows=2000)))
    assert first.column("px")[0].as_py() == wide.column("px")[0].as_py()
    assert first.column("px")[999].as_py() == wide.column("px")[999].as_py()


def test_batch_outlives_its_stream(am, aa):
    """A consumer may hold batch 4 after releasing the reader and batches 1-3.

    The parent export is reference-counted rather than owned by the stream, so
    this is a supported pattern and not a use-after-free waiting to happen.
    """
    reader = aa.query("select from trades", batch_rows=10_000)
    kept = None
    for i, batch in enumerate(reader):
        if i == 3:
            kept = batch
    del reader
    gc.collect()
    assert kept is not None and kept.num_rows == 10_000
    assert isinstance(kept.column("px")[0].as_py(), float)


def test_query_flattens_a_keyed_result(am, aa):
    table = aa.query("select v:avg px, n:#px by sym from trades").read_all()
    assert table.column_names[0] == "sym"
    assert {"v", "n"} <= set(table.column_names)
    assert table.num_rows == len(set(table.column("sym").to_pylist()))


def test_query_rejects_a_non_table(am, aa):
    with pytest.raises(aa.AmberArrowError, match="table"):
        aa.query("1 2 3")


def test_table_to_arrow(am, aa):
    at = aa.table_to_arrow(am.q("select from trades", raw=True))
    assert at.num_rows == int(am.eval("#trades"))
    assert at.column_names == ["sym", "time", "px", "sz"]


def test_arrow_roundtrip(am, aa):
    original = aa.query("select from trades", batch_rows=100_000).read_all()
    back = aa.arrow_to_table(original)
    assert len(back) == original.num_rows
    assert back.columns == original.column_names
    assert list(back["px"][:5]) == original.column("px").to_pylist()[:5]


def test_arrow_reader_roundtrip(am, aa):
    """Importing a multi-batch STREAM exercises the concatenation path."""
    reader = aa.query("select from trades", batch_rows=30_000)
    assert len(aa.arrow_to_table(reader)) == int(am.eval("#trades"))


def test_schema_is_independent_of_the_stream(am, aa):
    """PyArrow reads the schema, releases it, then keeps pulling batches.

    get_schema() must therefore hand out an independent copy; if it handed out
    the parent's, the first release would destroy the stream.
    """
    reader = aa.query("select from trades", batch_rows=50_000)
    schema = reader.schema
    del schema
    gc.collect()
    assert reader.read_all().num_rows == int(am.eval("#trades"))
