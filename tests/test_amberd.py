"""The amberd wire protocol, against a real server on a real socket."""
import pytest

from amber_arrow.client import AmberClient, AmberRemoteError


@pytest.fixture
def client(amberd):
    host, port = amberd
    with AmberClient(host, port) as c:
        yield c


def test_ping_and_version(client):
    assert client.ping()
    assert client.version().startswith("1.")


def test_text_mode(client):
    assert client.text("2+3").strip() == "5"


def test_text_is_ansi_stripped(client):
    """The engine colours table output unconditionally. A wire protocol must
    not: a caller that wants colour can add it, a caller that does not cannot
    reliably remove it."""
    assert "\x1b[" not in client.text("3#trades")


def test_json_rows(client):
    rows = client.json("3#trades")
    assert isinstance(rows, list) and len(rows) == 3
    assert set(rows[0]) == {"sym", "time", "px", "sz"}


def test_jsonc_columns(client):
    cols = client.columns("select n:#px by sym from trades")
    assert set(cols) == {"sym", "n"}
    assert len(cols["sym"]) == len(cols["n"])


def test_bare_qsql_works_verbatim(client):
    cols = client.columns("select v:avg px by sym from trades where px>0")
    assert "v" in cols and cols["v"]


def test_errors_carry_the_engine_diagnostic(client):
    with pytest.raises(AmberRemoteError, match="value"):
        client.text("noSuchGlobal+1")


def test_connection_survives_an_error(client):
    with pytest.raises(AmberRemoteError):
        client.text("noSuchGlobal+1")
    assert client.text("40+2").strip() == "42"


def test_state_is_shared_between_connections(amberd):
    """A single-threaded event loop over one engine means two clients see each
    other's writes -- the property that forking per connection would destroy."""
    host, port = amberd
    with AmberClient(host, port) as a, AmberClient(host, port) as b:
        a.text("sharedGlobal:12345")
        assert b.text("sharedGlobal").strip() == "12345"


def test_unknown_directive_is_an_error_not_a_disconnect(client):
    with pytest.raises(AmberRemoteError):
        client.request("\\nosuchdirective")
    assert client.ping()


def test_pandas_helper(client):
    pytest.importorskip("pandas")
    frame = client.pandas("select n:#px, v:avg px by sym from trades")
    assert list(frame.columns) == ["sym", "n", "v"] and len(frame)


def test_arrow_helper(client):
    pytest.importorskip("pyarrow")
    assert client.arrow("select n:#px by sym from trades").column_names == ["sym", "n"]


def test_parse_directive_accepts_valid_syntax(client):
    assert client.request("\\parse select vwap:wavg[sz;px] by sym from trades") == "ok"


def test_parse_directive_rejects_bad_syntax(client):
    for bad in ("(1;2", '"unterminated', "}{"):
        with pytest.raises(AmberRemoteError, match="parse"):
            client.request(f"\\parse {bad}")


def test_parse_does_not_evaluate(client):
    """The property a language server depends on: syntax-checking a line the
    user is still typing must not run it. `\\parse` goes through the engine's
    `\\ast`, which never touches the compiler or the evaluator."""
    with pytest.raises(AmberRemoteError):
        client.text("parseSideEffect")            # not defined yet
    client.request("\\parse parseSideEffect:999")  # would define it if it ran
    with pytest.raises(AmberRemoteError, match="value"):
        client.text("parseSideEffect")            # still not defined
