"""amberd's edges: read-only, limits, bad options, scripts, clients that misbehave."""
import os
import socket
import subprocess
import time

import pytest

from amber_arrow.client import AmberClient, AmberRemoteError

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BINARY = os.path.join(ROOT, "build", "amberd")


def _port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _start(am, *args):
    if not os.path.isfile(BINARY):
        pytest.skip("amberd not built (run ./build.sh --amberd)")
    port = _port()
    proc = subprocess.Popen([BINARY, "--home", am.home(), "--port", str(port), *args],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    deadline = time.time() + 20
    while time.time() < deadline:
        if proc.poll() is not None:
            return proc, None
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return proc, port
        except OSError:
            time.sleep(0.05)
    proc.kill()
    pytest.fail("amberd did not start")


def _stop(proc):
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture(scope="module")
def readonly(am):
    proc, port = _start(am, "--readonly", "--eval", "ro:([]s:`a`b`a;px:1.0 2.0 3.0;sz:10 20 30)")
    assert port, proc.stderr.read().decode()
    yield port
    _stop(proc)


@pytest.mark.parametrize("query", [
    "select vwap:wavg[sz;px] by s from ro",
    "select n:count px, mx:max px from ro where s=`a",
    "exec px from ro",
    "{x+1} 41",
    "count ro",
])
def test_readonly_allows_reads(readonly, query):
    with AmberClient("127.0.0.1", readonly) as c:
        c.text(query)


@pytest.mark.parametrize("query", [
    "x:1", "  y:2", "1;z:3", "ro::0", "delete from `ro", "`ro insert (`c;4.0;40)",
    "ro upsert ([]s:enlist`d;px:enlist 5.0;sz:enlist 50)", "system \"ls\"", "a.b:1",
])
def test_readonly_refuses_writes(readonly, query):
    with AmberClient("127.0.0.1", readonly) as c:
        with pytest.raises(AmberRemoteError, match="readonly"):
            c.text(query)


def test_bad_options_fail_fast(am):
    if not os.path.isfile(BINARY):
        pytest.skip("amberd not built")
    for args in (["--port", "0"], ["--port", "70000"], ["--port", "abc"],
                 ["--bind", "not-an-address", "--i-know-this-is-unauthenticated"],
                 ["--bind", "0.0.0.0"], ["--load", "/no/such/script.q"], ["--bogus"]):
        r = subprocess.run([BINARY, "--home", am.home(), *args], capture_output=True, timeout=20)
        assert r.returncode != 0, args


def test_load_runs_q_scripts_through_the_loader(am, tmp_path):
    script = tmp_path / "boot.q"
    script.write_text("tbl:([]a:1 2 3;b:`x`y`z)\r\nr:select a\r\n  from tbl where a>1\r\n", encoding="utf-8")
    proc, port = _start(am, "--load", str(script))
    try:
        assert port, proc.stderr.read().decode()
        with AmberClient("127.0.0.1", port) as c:
            assert c.columns("r") == {"a": [2, 3]}
    finally:
        _stop(proc)


def test_long_lines_and_line_limit(amberd):
    host, port = amberd
    with AmberClient(host, port) as c:
        big = "count " + "\"" + "x" * 600_000 + "\""
        assert c.text(big).strip() == "600000"         # under 1 MiB: fine (it was refused)
    with socket.create_connection((host, port), timeout=10) as s:
        s.sendall(b"count \"" + b"y" * (1 << 20) + b"\"\n")
        head = s.recv(64)
        assert head.startswith(b"-") and b"limit" in s.recv(256) + head
    with AmberClient(host, port) as c:                  # and the server is still fine
        assert c.ping()


def test_a_client_that_vanishes_mid_request(amberd):
    host, port = amberd
    for _ in range(20):
        s = socket.create_connection((host, port), timeout=5)
        s.sendall(b"select from tra")
        s.close()
    with AmberClient(host, port) as c:
        assert c.ping()


def test_many_clients_at_once(amberd):
    host, port = amberd
    clients = [AmberClient(host, port).connect() for _ in range(40)]
    try:
        for i, c in enumerate(clients):
            assert c.text(f"{i}+1").strip() == str(i + 1)
    finally:
        for c in clients:
            c.close()


def test_garbage_bytes(amberd):
    host, port = amberd
    with socket.create_connection((host, port), timeout=10) as s:
        s.sendall(b"\xff\xfe\x00\x01 garbage\n")
        assert s.recv(16)[:1] in (b"+", b"-")
    with AmberClient(host, port) as c:
        assert c.ping()


@pytest.mark.parametrize("text, mutating", [
    ("select vwap:wavg[sz;px] by s from t", False), ("x:1", True), ("  a.b:2", True),
    ("1+1;y:2", True), ("{x:1} 2", False), ('"a:b"', False), ("t::0", True), ("exec px from t", False),
])
def test_python_guard_matches_amberd(text, mutating):
    from amber_arrow.client import looks_like_mutation
    assert looks_like_mutation(text) is mutating
