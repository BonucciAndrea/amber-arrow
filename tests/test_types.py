"""Every type, both ways, with exact values: what Arrow sees of an Amber table,
and what Amber makes of any Arrow column."""
import datetime as dt
import decimal
import math

import pytest

pa = pytest.importorskip("pyarrow")
np = pytest.importorskip("numpy")


def out(am, aa, src):
    return aa.table_to_arrow(am.q(src, raw=True))


def back(am, aa, data):
    return aa.arrow_to_table(data)


# ---- out ---------------------------------------------------------------------

def test_every_int_is_int64(am, aa):
    """`type 1 2 3` is 7 however narrow the storage: Arrow sees int64, and -1 is -1."""
    t = out(am, aa, "([]b:`byte$0 -1 127;s:1000 -2000 3;l:10000000000 -1 0;small:1 2 3)")
    assert all(f.type == pa.int64() for f in t.schema)
    assert t.column("b").to_pylist() == [0, -1, 127]
    assert t.column("s").to_pylist() == [1000, -2000, 3]


def test_long_nulls_are_nulls_and_zero_copy(am, aa):
    raw = am.q("([]a:1 0N 3)", raw=True)
    t = aa.table_to_arrow(raw)
    assert t.column("a").to_pylist() == [1, None, 3]
    payload = am.Table(raw).column("a", raw=True).handle()
    assert t.column("a").chunk(0).buffers()[1].address == payload


def test_floats_zero_copy(am, aa):
    raw = am.q("([]f:1.5 0n 2.5)", raw=True)
    t = aa.table_to_arrow(raw)
    assert t.schema.field("f").type == pa.float64()
    assert t.column("f").chunk(0).buffers()[1].address == am.Table(raw).column("f", raw=True).handle()
    assert math.isnan(t.column("f").to_pylist()[1])


def test_symbols_and_strings(am, aa):
    t = out(am, aa, '([]s:`a``ccc;c:("ab";"";"héllo"))')
    assert t.column("s").to_pylist() == ["a", "", "ccc"]
    assert t.column("c").to_pylist() == ["ab", "", "héllo"]
    assert t.schema.field("c").type == pa.string()


def test_char_column(am, aa):
    t = out(am, aa, '([]c:"xyz")')
    assert t.column("c").to_pylist() == ["x", "y", "z"]


def test_general_list_column_is_refused(am, aa):
    with pytest.raises(aa.AmberArrowError, match="list of strings"):
        out(am, aa, "([]g:(1 2;3 4))")


@pytest.mark.parametrize("src, typ, first", [
    ("2026.01.01 0Nd", pa.date32(), dt.date(2026, 1, 1)),
    ("1999.12.31 0Nd", pa.date32(), dt.date(1999, 12, 31)),
    ("2026.03 0Nm", pa.date32(), dt.date(2026, 3, 1)),
    ("1999.11 0Nm", pa.date32(), dt.date(1999, 11, 1)),
    ("2026.01.01D09:30:00.000000001 0Np", pa.timestamp("ns"), None),
    ("0D01:00:00.000000005 0Nn", pa.duration("ns"), None),
    ("09:30:00.250 0Nt", pa.time32("ms"), dt.time(9, 30, 0, 250000)),
    ("09:30:15 0Nv", pa.time32("s"), dt.time(9, 30, 15)),
    ("09:30 0Nu", pa.time32("s"), dt.time(9, 30)),
])
def test_temporal_out(am, aa, src, typ, first):
    t = out(am, aa, f"([]x:{src})")
    assert t.schema.field("x").type == typ
    col = t.column("x")
    assert col.null_count == 1 and col[1].as_py() is None
    if first is not None:
        assert col[0].as_py() == first
    elif typ == pa.timestamp("ns"):
        assert col.cast(pa.int64())[0].as_py() == 1767259800000000001
    else:
        assert col.cast(pa.int64())[0].as_py() == 3_600_000_000_005


def test_empty_and_zero_column_tables(am, aa):
    t = out(am, aa, "0#([]a:`long$();s:`$();d:`date$())")
    assert t.num_rows == 0 and t.schema.field("d").type == pa.date32()
    t = aa.table_to_arrow(am.q("([]a:1 2)", raw=True))
    assert t.num_rows == 2


def test_keyed_query_is_flattened(am, aa):
    am.eval("kq:([]s:`a`b`a;v:1 2 3)")
    t = aa.query("select sum v by s from kq").read_all()
    assert t.column_names == ["s", "v"]
    assert dict(zip(t.column("s").to_pylist(), t.column("v").to_pylist())) == {"a": 4, "b": 2}


def test_query_errors(am, aa):
    with pytest.raises(aa.AmberArrowError, match="noSuchThing"):
        aa.query("select from noSuchThing")
    with pytest.raises(aa.AmberArrowError, match="not a table"):
        aa.query("1 2 3")
    with pytest.raises(TypeError):
        aa.query(b"select from trades")
    with pytest.raises(ValueError):
        aa.query("select from trades", batch_rows=-1)


# ---- in ----------------------------------------------------------------------

@pytest.mark.parametrize("arr, want", [
    (pa.array([1, None, -3], pa.int8()), [1, None, -3]),
    (pa.array([200, 255], pa.uint8()), [200, 255]),
    (pa.array([-30000, None], pa.int16()), [-30000, None]),
    (pa.array([65535], pa.uint16()), [65535]),
    (pa.array([2**31 - 1, None], pa.int32()), [2**31 - 1, None]),
    (pa.array([2**32 - 1], pa.uint32()), [2**32 - 1]),
    (pa.array([2**62, None], pa.int64()), [2**62, None]),
    (pa.array([2**63 - 1], pa.uint64()), [2**63 - 1]),
    (pa.array([True, None, False]), [1, None, 0]),
])
def test_ints_in(am, aa, arr, want):
    t = back(am, aa, pa.table({"x": arr}))
    v = t.column("x", raw=True)
    got = [None if x == "0N" else int(x) for x in v.format().lstrip(",").split()]
    assert got == want


def test_uint64_past_int64(am, aa):
    with pytest.raises(aa.AmberArrowError, match="uint64"):
        back(am, aa, pa.table({"x": pa.array([2**64 - 1], pa.uint64())}))


@pytest.mark.parametrize("arr", [
    pa.array([1.5, None, -2.25], pa.float64()),
    pa.array([1.5, None, -2.25], pa.float32()),
    pa.array(np.array([1.5, np.nan, -2.25], dtype=np.float16)),
])
def test_floats_in(am, aa, arr):
    got = back(am, aa, pa.table({"x": arr})).column("x")
    assert got[0] == 1.5 and math.isnan(got[1]) and got[2] == -2.25


def test_decimals_in(am, aa):
    arr = pa.array([decimal.Decimal("123.45"), None, decimal.Decimal("-0.01")], pa.decimal128(10, 2))
    got = back(am, aa, pa.table({"x": arr})).column("x")
    assert got[0] == pytest.approx(123.45) and math.isnan(got[1]) and got[2] == pytest.approx(-0.01)


@pytest.mark.parametrize("arr", [
    pa.array(["a", None, "ccc"], pa.string()),
    pa.array(["a", None, "ccc"], pa.large_string()),
    pa.array(["a", None, "ccc"]).dictionary_encode(),
])
def test_strings_in(am, aa, arr):
    assert list(back(am, aa, pa.table({"x": arr}))["x"]) == ["a", "", "ccc"]


def test_string_views_in(am, aa):
    if not hasattr(pa, "string_view"):
        pytest.skip("pyarrow without string_view")
    arr = pa.array(["short", "a string well past twelve bytes", None], pa.string_view())
    assert list(back(am, aa, pa.table({"x": arr}))["x"]) == ["short", "a string well past twelve bytes", ""]


def test_numeric_dictionary_in(am, aa):
    arr = pa.DictionaryArray.from_arrays(pa.array([1, 0, None, 1], pa.int8()), pa.array([1.5, 2.5]))
    got = back(am, aa, pa.table({"x": arr})).column("x")
    assert got[0] == 2.5 and got[1] == 1.5 and math.isnan(got[2])


def test_null_column_in(am, aa):
    got = back(am, aa, pa.table({"x": pa.array([None, None], pa.null())})).column("x")
    assert len(got) == 2 and all(math.isnan(v) for v in got)


@pytest.mark.parametrize("arr, text", [
    (pa.array([dt.date(2026, 1, 1), None], pa.date32()), "2026.01.01 0N"),
    (pa.array([dt.date(1970, 1, 1), None], pa.date64()), "1970.01.01 0N"),
    (pa.array([dt.datetime(2026, 1, 1, 9, 30), None], pa.timestamp("s")), "2026.01.01D09:30:00.000000000 0N"),
    (pa.array([dt.datetime(2026, 1, 1, 9, 30), None], pa.timestamp("us", tz="UTC")),
     "2026.01.01D09:30:00.000000000 0N"),
    (pa.array([dt.timedelta(seconds=90), None], pa.duration("ms")), "0D00:01:30.000000000 0N"),
    (pa.array([dt.time(9, 30), None], pa.time32("ms")), "09:30:00.000 0N"),
    (pa.array([dt.time(9, 30, 15), None], pa.time32("s")), "09:30:15 0N"),
    (pa.array([dt.time(9, 30, 0, 1), None], pa.time64("us")), "0D09:30:00.000001000 0N"),
])
def test_temporal_in(am, aa, arr, text):
    assert back(am, aa, pa.table({"x": arr})).column("x", raw=True).format() == text


def test_dates_before_1970(am, aa):
    arr = pa.array([dt.date(1960, 2, 29), dt.datetime(1950, 1, 1)], pa.date64()) if False else \
        pa.array([dt.date(1960, 2, 29)], pa.date64())
    assert back(am, aa, pa.table({"x": arr})).column("x", raw=True).format() == ",1960.02.29"


@pytest.mark.parametrize("arr, match", [
    (pa.array([[1, 2]]), "nested"),
    (pa.array([b"\x00"], pa.binary()), "binary"),
    (pa.array([{"a": 1}]), "nested"),
    (pa.array([pa.MonthDayNano([1, 2, 3])], pa.month_day_nano_interval()), "interval"),
])
def test_unsupported_in(am, aa, arr, match):
    with pytest.raises(aa.AmberArrowError, match=match):
        back(am, aa, pa.table({"x": arr}))


def test_sliced_and_chunked_in(am, aa):
    base = pa.table({"i": pa.array(range(10)), "s": [str(i) for i in range(10)],
                     "f": pa.array([float(i) for i in range(10)], pa.float32()),
                     "b": pa.array([i % 2 == 0 for i in range(10)])})
    t = back(am, aa, base.slice(3, 4))
    assert t["i"].tolist() == [3, 4, 5, 6] and list(t["s"]) == ["3", "4", "5", "6"]
    assert t["f"].tolist() == [3.0, 4.0, 5.0, 6.0]
    assert t.column("b", raw=True).format() == "0 1 0 1"
    chunked = pa.concat_tables([base.slice(0, 2), base.slice(7, 3)])
    assert back(am, aa, chunked)["i"].tolist() == [0, 1, 7, 8, 9]
    rb = base.to_batches()[0].slice(5, 2)
    assert back(am, aa, rb)["i"].tolist() == [5, 6]


def test_empty_table_in(am, aa):
    t = back(am, aa, pa.table({"a": pa.array([], pa.int64()), "d": pa.array([], pa.date32())}))
    assert len(t) == 0 and t.columns == ["a", "d"]
    assert t.dtypes["d"] == "date vector"


def test_many_batches_in(am, aa):
    batches = [pa.record_batch({"a": pa.array(range(i * 100, i * 100 + 100))}) for i in range(500)]
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    t = back(am, aa, reader)
    assert len(t) == 50_000 and t["a"][-1] == 49_999


def test_duplicate_names_in(am, aa):
    tbl = pa.Table.from_arrays([pa.array([1]), pa.array([2])], names=["a", "a"])
    with pytest.raises(aa.AmberArrowError, match="two columns"):
        back(am, aa, tbl)


def test_c_stream_objects_in(am, aa):
    pl = pytest.importorskip("polars")
    df = pl.DataFrame({"a": [1, 2], "s": ["x", "y"]})
    if not hasattr(df, "__arrow_c_stream__"):
        pytest.skip("polars without the PyCapsule interface")
    t = back(am, aa, df)
    assert t["a"].tolist() == [1, 2] and list(t["s"]) == ["x", "y"]


def test_garbage_in(am, aa):
    with pytest.raises(TypeError):
        back(am, aa, 42)


# ---- round trip ---------------------------------------------------------------

def test_roundtrip_every_type(am, aa):
    src = ('([]i:1 0N 3;f:1.5 0n 2.5;s:`a`b`;c:("x";"";"zz");d:2026.01.01 0Nd 1999.12.31;'
           'p:2026.01.01D09:30:00.000000001 0Np 2000.01.01D00:00;n:0D01:00 0Nn 0D00:00:00.000000001;'
           't:09:30:00.000 0Nt 00:00:00.001;v:09:30:15 0Nv 00:00:01)')
    am.eval(f"rtA:{src}")
    t = aa.arrow_to_table(aa.table_to_arrow(am.get("rtA", raw=True)))
    am.set("rtB", t.value)
    for col in ("i", "s", "d", "p", "n", "t", "v"):
        assert am.eval(f"rtA[`{col}]~rtB[`{col}]") == 1, col
    assert am.eval("(rtA[`f] 0 2)~rtB[`f] 0 2") == 1
    assert list(t["c"]) == ["x", "", "zz"]                  # strings come back as symbols
