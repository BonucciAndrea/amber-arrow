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
    assert next(client.do_action(fl.Action("version", b""))).body.to_pybytes().decode().startswith("1.")


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
