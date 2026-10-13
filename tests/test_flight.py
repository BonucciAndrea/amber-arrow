"""Arrow Flight: discovery, streaming, upload, actions."""
import pytest

fl = pytest.importorskip("pyarrow.flight")
pa = pytest.importorskip("pyarrow")


@pytest.fixture(scope="module")
def client(flight_server):
    c = fl.connect(flight_server)
    yield c
    c.close()


def test_version_action(client):
    version = next(client.do_action(fl.Action("version", b""))).body.to_pybytes().decode()
    # A dotted version like "2.0.0"; not pinned to a major so it survives an
    # engine version bump.
    assert version[:1].isdigit() and "." in version


def test_tables_action(client):
    names = next(client.do_action(fl.Action("tables", b""))).body.to_pybytes().decode().split(",")
    assert "trades" in names and "quotes" in names


def test_list_flights_reports_real_schemas(client):
    """The catalogue is computed, not guessed: each entry's schema comes from
    actually streaming a zero-row slice of the table."""
    found = {info.descriptor.path[0].decode(): info for info in client.list_flights()}
    assert "trades" in found
    assert [f.name for f in found["trades"].schema] == ["sym", "time", "px", "sz"]
    assert found["trades"].total_records == 20_000


def test_a_ticket_is_an_amber_query(client):
    table = client.do_get(fl.Ticket(b"select vwap:wavg[sz;px], n:#px by sym from trades")).read_all()
    assert table.column_names == ["sym", "vwap", "n"]
    assert table.num_rows > 0


def test_full_table_streams(client):
    table = client.do_get(fl.Ticket(b"select from trades")).read_all()
    assert table.num_rows == 20_000
    assert table.column_names == ["sym", "time", "px", "sz"]


def test_get_flight_info_for_a_command(client):
    descriptor = fl.FlightDescriptor.for_command(b"select from quotes")
    info = client.get_flight_info(descriptor)
    assert "bid" in [f.name for f in info.schema]


def test_do_put_binds_a_global(client):
    uploaded = pa.table({"a": [1, 2, 3], "b": [1.5, 2.5, 3.5]})
    writer, _ = client.do_put(fl.FlightDescriptor.for_path("uploadedByTest"), uploaded.schema)
    writer.write_table(uploaded)
    writer.close()
    back = client.do_get(fl.Ticket(b"select from uploadedByTest")).read_all()
    assert back.to_pydict() == {"a": [1, 2, 3], "b": [1.5, 2.5, 3.5]}


def test_eval_action(client):
    client.do_action(fl.Action("eval", b"flightTestGlobal:99"))
    for _ in client.do_action(fl.Action("eval", b"flightTestGlobal:99")):
        pass
    table = client.do_get(fl.Ticket(b"([]v:enlist flightTestGlobal)")).read_all()
    assert table.column("v").to_pylist() == [99]


def test_a_bad_query_is_an_error_not_a_hang(client):
    with pytest.raises(Exception):
        client.do_get(fl.Ticket(b"noSuchGlobalAtAll+1")).read_all()


def test_temporal_columns_stream(client):
    t = client.do_get(fl.Ticket(b"([]d:2026.01.01 0Nd;p:2026.01.01D09:30 0Np;n:0D01:00 0Nn)")).read_all()
    assert [f.type for f in t.schema] == [pa.date32(), pa.timestamp("ns"), pa.duration("ns")]
    assert t.column("d").null_count == 1


def test_do_put_temporal_and_strings(client):
    import datetime as dt
    up = pa.table({"d": pa.array([dt.date(2026, 1, 2)], pa.date32()),
                   "s": pa.array(["x"]).dictionary_encode(),
                   "i": pa.array([7], pa.int8())})
    writer, _ = client.do_put(fl.FlightDescriptor.for_path("putTemporal"), up.schema)
    writer.write_table(up)
    writer.close()
    back = client.do_get(fl.Ticket(b"select from putTemporal")).read_all()
    assert back.column("d").to_pylist() == [dt.date(2026, 1, 2)]
    assert back.column("s").to_pylist() == ["x"] and back.column("i").to_pylist() == [7]


def test_do_put_refuses_a_bad_name(client):
    up = pa.table({"a": [1]})
    with pytest.raises(fl.FlightError):
        writer, _ = client.do_put(fl.FlightDescriptor.for_path("bad name;x:1"), up.schema)
        writer.write_table(up)
        writer.close()


def test_engine_errors_carry_their_text(client):
    with pytest.raises(fl.FlightError, match="noSuchGlobalAtAll"):
        client.do_get(fl.Ticket(b"noSuchGlobalAtAll+1")).read_all()


def test_concurrent_gets(client, flight_server):
    import threading
    out, errs = [], []

    def get():
        try:
            c = fl.connect(flight_server)
            out.append(c.do_get(fl.Ticket(b"select from trades")).read_all().num_rows)
            c.close()
        except Exception as e:                       # noqa: BLE001
            errs.append(e)

    ths = [threading.Thread(target=get) for _ in range(8)]
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    assert not errs and out == [20_000] * 8



def test_engine_runs_on_a_thread_with_a_real_stack():
    """gRPC's own threads get 512 KB on macOS; the engine runs on one of ours."""
    from amber_flight.server import ENGINE_STACK, _EngineThread
    import threading
    e = _EngineThread()
    assert e.run(lambda: threading.current_thread().name) == "amber-engine"
    assert e.run(lambda: e.run(lambda: 41) + 1) == 42          # nested: no deadlock
    with pytest.raises(ZeroDivisionError):
        e.run(lambda: 1 / 0)
    assert ENGINE_STACK >= 16 << 20
