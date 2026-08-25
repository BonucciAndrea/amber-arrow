"""``python -m amber_flight`` -- run the Arrow Flight server."""
from __future__ import annotations

import argparse
import sys


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="amber-flight",
        description="Serve Amber query results as Apache Arrow Flight streams.",
        epilog="A Flight ticket IS an Amber query. Anyone who can reach this port "
               "can evaluate arbitrary Amber -- bind loopback unless you have put "
               "authentication in front of it.")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8815)
    p.add_argument("--home", default=None,
                   help="directory holding the .k standard library (embedded backend)")
    p.add_argument("--remote", default=None, metavar="HOST:PORT",
                   help="front an amberd instead of embedding the engine "
                        "(slower: columnar JSON on the wire, one copy)")
    p.add_argument("--batch-rows", type=int, default=1 << 20)
    p.add_argument("--readonly", action="store_true")
    p.add_argument("--eval", action="append", default=[], metavar="EXPR",
                   help="run EXPR at startup (repeatable), e.g. --eval 'gentq 10000000'")
    p.add_argument("--i-know-this-is-unauthenticated", action="store_true", dest="public")
    args = p.parse_args(argv)

    if args.host not in ("127.0.0.1", "localhost") and not args.public:
        print("amber-flight: refusing to bind a non-loopback address without "
              "--i-know-this-is-unauthenticated (a Flight ticket is an Amber "
              "expression; there is no authentication here)", file=sys.stderr)
        return 2

    from . import serve
    try:
        serve(args.host, args.port, home=args.home, remote=args.remote,
              batch_rows=args.batch_rows, readonly=args.readonly, startup=args.eval)
    except KeyboardInterrupt:
        print("\namber-flight: stopped", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
