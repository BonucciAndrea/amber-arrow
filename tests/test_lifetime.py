"""Lifetimes and threads: batches outliving everything, released anywhere."""
import gc
import os
import subprocess
import sys
import threading

import pytest

pa = pytest.importorskip("pyarrow")


def _rss_mb():
    if "asan" in os.environ.get("LD_PRELOAD", ""):
        pytest.skip("ASan's quarantine holds freed memory: RSS says nothing")
    try:
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 2**20
    except (OSError, ValueError, AttributeError):
        pytest.skip("RSS needs /proc")


def test_batches_outlive_stream_and_table(am, aa):
    raw = am.q("([]x:til 100000;s:100000#`a`b`c)", raw=True)
    batches = list(aa.stream(raw, batch_rows=10_000))
    del raw
    gc.collect()
    am.eval("junkL:til 1000000")
    assert sum(b.num_rows for b in batches) == 100_000
    assert batches[-1].column("x")[-1].as_py() == 99_999
    assert batches[3].column("s")[0].as_py() == "a"


def test_batch_released_on_another_thread(am, aa):
    batches = list(aa.stream(am.q("([]x:til 50000)", raw=True), batch_rows=1000))
    done = []

    def drop():
        while batches:
            batches.pop()
        gc.collect()
        done.append(1)

    th = threading.Thread(target=drop)
    th.start()
    th.join()
    assert done
    aa.collect()                                       # the engine side, on this thread
    assert am.eval("1+1") == 2


def test_no_leak_streaming(am, aa):
    am.eval("leakT:([]x:til 200000;f:200000?1.0;s:200000#`a`b;d:200000#2026.01.01)")
    raw = am.get("leakT", raw=True)
    for _ in range(5):
        aa.table_to_arrow(raw)
    aa.collect(); gc.collect()
    before = _rss_mb()
    for _ in range(60):
        for b in aa.stream(raw, batch_rows=50_000):
            b.num_rows
        aa.table_to_arrow(raw)
        aa.collect()
    gc.collect(); aa.collect()
    assert _rss_mb() - before < 40


def test_no_leak_importing(am, aa):
    tbl = pa.table({"a": pa.array(range(100_000)), "s": [str(i % 100) for i in range(100_000)]})
    for _ in range(5):
        aa.arrow_to_table(tbl)
    gc.collect()
    before = _rss_mb()
    for _ in range(60):
        aa.arrow_to_table(tbl)
    gc.collect()
    assert _rss_mb() - before < 40


def test_threads_streaming_concurrently(am, aa):
    readers = [aa.query("select from trades", batch_rows=777) for _ in range(6)]
    counts, errors = [], []

    def drain(r):
        try:
            counts.append(sum(b.num_rows for b in r))
        except Exception as e:                         # noqa: BLE001
            errors.append(e)

    ths = [threading.Thread(target=drain, args=(r,)) for r in readers]
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    assert not errors and counts == [int(am.eval("#trades"))] * 6
    del readers
    gc.collect()
    aa.collect()


def test_one_engine_only(am, aa):
    """libamber_arrow must not drag a second engine in with it."""
    if not sys.platform.startswith("linux"):
        pytest.skip("reads /proc/self/maps")
    with open("/proc/self/maps") as f:
        libs = {line.split()[-1] for line in f if "libamber" in line and "arrow" not in line}
    assert len(libs) == 1, libs


def test_import_from_a_fresh_process(am, aa):
    code = ("import amber as am, amber_arrow as aa, pyarrow as pa; am.init();"
            "t = aa.arrow_to_table(pa.table({'a': [1, 2, 3]})); print(t['a'].tolist())")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60,
                       env={**os.environ})
    assert r.returncode == 0, r.stderr[-800:]
    assert r.stdout.strip() == "[1, 2, 3]"
