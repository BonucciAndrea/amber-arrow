import os
import socket
import subprocess
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="session")
def am():
    amber = pytest.importorskip("amber")
    amber.init()
    if amber.home() is None:
        pytest.skip("no Amber standard library found (set $AMBER_HOME)")
    amber.gentq(200_000)
    return amber


@pytest.fixture(scope="session")
def aa(am):
    import amber_arrow
    try:
        amber_arrow._load()
    except Exception as exc:                       # noqa: BLE001
        pytest.skip(f"libamber_arrow not built: {exc}")
    return amber_arrow


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def amberd(am):
    """A live amberd on a free port, with data loaded, torn down afterwards."""
    binary = os.path.join(ROOT, "build", "amberd")
    if not os.path.isfile(binary):
        pytest.skip("amberd not built (run ./build.sh --amberd)")
    port = _free_port()
    proc = subprocess.Popen(
        [binary, "--home", am.home(), "--port", str(port), "--eval", "gentq 20000"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # Wait for the port rather than sleeping a guessed interval: a fixture that
    # races the server under load fails in CI and nowhere else.
    deadline = time.time() + 30
    while time.time() < deadline:
        if proc.poll() is not None:
            pytest.fail(f"amberd exited: {proc.stderr.read().decode()}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                break
        except OSError:
            time.sleep(0.1)
    else:
        proc.kill()
        pytest.fail("amberd did not start within 30s")
    yield ("127.0.0.1", port)
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture(scope="session")
def flight_server(am):
    """A live amber-flight server on a free port."""
    pytest.importorskip("pyarrow.flight")
    port = _free_port()
    proc = subprocess.Popen(
        [os.sys.executable, "-m", "amber_flight", "--port", str(port),
         "--home", am.home(), "--eval", "gentq 20000"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    deadline = time.time() + 60
    while time.time() < deadline:
        if proc.poll() is not None:
            pytest.fail(f"amber-flight exited: {proc.stderr.read().decode()}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                time.sleep(0.5)      # gRPC needs a moment after the port opens
                break
        except OSError:
            time.sleep(0.2)
    else:
        proc.kill()
        pytest.fail("amber-flight did not start within 60s")
    yield f"grpc://127.0.0.1:{port}"
    proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
