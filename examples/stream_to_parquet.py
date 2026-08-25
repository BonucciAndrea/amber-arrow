#!/usr/bin/env python3
"""Stream a very large Amber query straight to Parquet, in bounded memory.

    python3 examples/stream_to_parquet.py 20000000 /tmp/trades.parquet

The point of the exercise: the peak resident set stays flat as the row count
grows. Nothing here ever holds the whole result -- the engine keeps one export,
this script pulls one batch at a time, and each batch is a window onto that
export rather than a copy of part of it.
"""
from __future__ import annotations

import os
import resource
import sys
import time

import amber as am
import amber_arrow as aa
import pyarrow.parquet as pq


def rss_mb() -> float:
    kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return kb / (1024 * 1024) if sys.platform == "darwin" else kb / 1024


def main() -> int:
    rows = int(sys.argv[1]) if len(sys.argv) > 1 else 5_000_000
    out = sys.argv[2] if len(sys.argv) > 2 else "/tmp/amber_trades.parquet"

    am.init()
    print(f"generating {rows:,} trades ...", flush=True)
    started = time.time()
    am.gentq(rows)
    print(f"  {am.eval('#trades'):,} rows in {time.time() - started:.1f}s"
          f"   rss {rss_mb():.0f} MB")

    reader = aa.query("select from trades", batch_rows=1 << 20)
    started = time.time()
    written = batches = 0
    with pq.ParquetWriter(out, reader.schema, compression="zstd") as writer:
        for batch in reader:
            writer.write_batch(batch)
            written += batch.num_rows
            batches += 1
            if batches % 5 == 0:
                print(f"  {written:,} rows, {batches} batches, rss {rss_mb():.0f} MB",
                      flush=True)
    elapsed = time.time() - started
    size = os.path.getsize(out)
    print(f"wrote {written:,} rows in {batches} batches, {elapsed:.1f}s "
          f"({written / max(elapsed, 1e-9) / 1e6:.1f}M rows/s)")
    print(f"  {out}: {size / 1e6:.1f} MB   peak rss {rss_mb():.0f} MB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
