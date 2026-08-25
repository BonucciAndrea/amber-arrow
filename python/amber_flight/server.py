"""An Apache Arrow Flight server in front of the Amber engine.

    $ python -m amber_flight --home /path/to/amber --port 8815 --eval "gentq 10000000"
    $ python -m amber_flight --remote localhost:5010            # front an amberd

Flight is gRPC plus Arrow: a client asks for a *ticket* and gets back a stream of
record batches, already columnar, with no JSON, no CSV and no row-by-row
decoding at either end. For Amber that is close to ideal, because the engine's
columns are already the layout Arrow wants.

**Two backends, and the difference is the whole story.**

``embedded`` (the default) runs the engine inside this process via
``python-amber`` and streams with ``amber-arrow``. A batch handed to gRPC points
at the engine's own column buffers: the query result is never copied, not once,
between evaluation and the socket. This is the mode to use.

``remote`` connects to an ``amberd`` on another host and pulls results as
columnar JSON, which it then converts to Arrow. That is a real copy and a real
parse, and it is here because sometimes the engine is genuinely somewhere else --
a tickerplant, a historical database node -- and a slower path to it beats no
path. Do not reach for it when the engine could have been in-process.

**A ticket is an Amber query.** ``pa.flight.Ticket(b"select from trades where
sym=`AAPL")``. There is no query language of its own to learn and nothing to
translate; bare qSQL works exactly as it does at the ``amber>`` prompt. That also
means a client who can reach this server can evaluate arbitrary Amber, so read
the security note in the README before binding anything but loopback.
"""

from __future__ import annotations

import sys
from typing import Iterable

try:
    import pyarrow as pa
    import pyarrow.flight as flight
except ImportError as exc:                                  # pragma: no cover
    raise SystemExit(
        "amber-flight needs PyArrow with Flight support:\n"
        "    pip install 'pyarrow>=12'\n"
        f"(import failed: {exc})"
    )


DEFAULT_BATCH = 1 << 20      # 1M rows: gRPC message overhead disappears here


class AmberFlightServer(flight.FlightServerBase):
    """Serve Amber query results as Arrow Flight streams."""

    def __init__(self, location: str, *, home: str | None = None,
                 remote: str | None = None, batch_rows: int = DEFAULT_BATCH,
                 readonly: bool = False, startup: Iterable[str] = ()):
        super().__init__(location)
        self.location_str = location
        self.batch_rows = batch_rows
        self.readonly = readonly
        self.remote = remote
        self._client = None
        self._am = None
        self._aa = None

        if remote:
            from amber_arrow.client import AmberClient  # noqa: PLC0415
            host, _, port = remote.partition(":")
            self._client = AmberClient(host or "127.0.0.1", int(port or 5010)).connect()
            print(f"amber-flight: remote backend -> {remote} "
                  f"(engine {self._client.version()})", file=sys.stderr)
        else:
            import amber as am              # noqa: PLC0415
            import amber_arrow as aa        # noqa: PLC0415
            am.init(home)
            self._am, self._aa = am, aa
            print(f"amber-flight: embedded backend, Amber {am.version()} "
                  f"(stdlib: {am.home() or 'NOT LOADED'})", file=sys.stderr)
            for line in startup:
                am.q(line)
                print(f"amber-flight: ran {line!r}", file=sys.stderr)

    # ---- discovery -------------------------------------------------------

    def list_flights(self, context, criteria):
        """Advertise every table currently defined in the engine.

        A Flight client that has never seen this server needs *some* way to find
        out what is queryable. Amber has no catalogue -- tables are just globals
        -- so the catalogue is computed: every global that is a table, with its
        real row count and its real Arrow schema, obtained by streaming a
        zero-row slice of it rather than by guessing from names.
        """
        for name in self._tables():
            try:
                ticket = f"select from {name}".encode()
                schema = self._schema_of(name)
                descriptor = flight.FlightDescriptor.for_path(name)
                endpoint = flight.FlightEndpoint(ticket, [flight.Location(self.location_str)])
                rows = self._rowcount(name)
                yield flight.FlightInfo(schema, descriptor, [endpoint], rows, -1)
            except Exception:                               # noqa: BLE001, PERF203
                continue                                    # a global that is not queryable

    def get_flight_info(self, context, descriptor):
        query = self._descriptor_query(descriptor)
        reader = self._reader(query, batch_rows=0, limit=0)
        schema = reader.schema
        endpoint = flight.FlightEndpoint(query.encode(), [flight.Location(self.location_str)])
        return flight.FlightInfo(schema, descriptor, [endpoint], -1, -1)

    # ---- the data path ---------------------------------------------------

    def do_get(self, context, ticket):
        query = ticket.ticket.decode()
        self._guard(query)
        reader = self._reader(query, self.batch_rows)
        # GeneratorStream pulls batches lazily, so a 400-million-row result is
        # streamed at `batch_rows` granularity and never materialised whole --
        # which is the only reason a Flight server is worth building over a
        # "return the table" RPC.
        return flight.GeneratorStream(reader.schema, reader)

    def do_put(self, context, descriptor, reader, writer):
        """Accept an Arrow stream and bind it to an Amber global.

        The path is named by the descriptor: ``FlightDescriptor.for_path("bars")``
        binds the uploaded table to the global ``bars``.
        """
        if self.readonly:
            raise flight.FlightUnauthorizedError("this amber-flight is --readonly")
        if self.remote:
            raise flight.FlightUnavailableError("do_put needs the embedded backend")
        name = self._descriptor_query(descriptor)
        table = self._aa.arrow_to_table(reader.read_all())
        self._am.set(name, table.value)
        print(f"amber-flight: put {len(table):,} rows -> `{name}", file=sys.stderr)

    def do_action(self, context, action):
        """``version``, ``tables``, and ``eval`` for a statement with no result."""
        kind = action.type
        if kind == "version":
            version = self._am.version() if self._am else self._client.version()
            return [flight.Result(version.encode())]
        if kind == "tables":
            return [flight.Result(",".join(self._tables()).encode())]
        if kind == "eval":
            statement = action.body.to_pybytes().decode()
            self._guard(statement)
            if self._am:
                self._am.q(statement)
            else:
                self._client.text(statement)
            return [flight.Result(b"ok")]
        raise flight.FlightUnavailableError(f"unknown action {kind!r}")

    def list_actions(self, context):
        return [("version", "engine version"),
                ("tables", "comma-separated list of queryable tables"),
                ("eval", "evaluate a statement, discard the result")]

    # ---- internals -------------------------------------------------------

    def _guard(self, query: str) -> None:
        if self.readonly and any(tok in query for tok in ("::", "delete ", "upd\"", "del\"")):
            raise flight.FlightUnauthorizedError(
                "this amber-flight is --readonly and the request looks like a mutation")

    @staticmethod
    def _descriptor_query(descriptor) -> str:
        if descriptor.descriptor_type == flight.DescriptorType.CMD:
            return descriptor.command.decode()
        return "/".join(p.decode() if isinstance(p, bytes) else str(p) for p in descriptor.path)

    def _tables(self) -> list[str]:
        if self._am:
            names = self._am.eval("!`")
            out = []
            for name in names:
                if name.startswith(("pyamber", "amberarrow", "amberd")):
                    continue
                try:
                    if self._am.eval(f"`M~@{name}"):
                        out.append(name)
                except Exception:                           # noqa: BLE001, PERF203
                    continue
            return out
        raw = self._client.text("(!`) where {`M~@ . x}'!`")
        return [n for n in raw.replace("`", " ").split() if n]

    def _rowcount(self, name: str) -> int:
        try:
            return int(self._am.eval(f"#{name}")) if self._am else -1
        except Exception:                                   # noqa: BLE001
            return -1

    def _schema_of(self, name: str):
        return self._reader(f"select from {name}", batch_rows=0, limit=0).schema

    def _reader(self, query: str, batch_rows: int, limit: int | None = None):
        """A RecordBatchReader for `query`, from whichever backend is configured."""
        if self._aa:
            text = query if limit is None else f"{limit}#({query})" if limit else f"0#({query})"
            return self._aa.query(text, batch_rows=batch_rows)
        # remote: columnar JSON over the amberd wire, then one conversion
        text = query if limit is None else f"{limit}#({query})" if limit else f"0#({query})"
        table = pa.table(self._client.columns(text))
        return table.to_reader(max_chunksize=batch_rows or DEFAULT_BATCH)
