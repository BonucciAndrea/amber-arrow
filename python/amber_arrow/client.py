"""A client for ``amberd``, the Amber TCP query server.

The wire protocol is deliberately small enough to reimplement in an afternoon in
any language -- which is the point, because the Grafana backend implements it in
Go and the VS Code language server implements it in TypeScript. This is the
reference implementation, and the docstring below is the specification.

    >>> from amber_arrow.client import AmberClient
    >>> with AmberClient("localhost", 5010) as c:
    ...     c.ping()
    ...     rows = c.json("select vwap:wavg[sz;px] by sym from trades")
    ...     frame = c.pandas("select from trades where sym=`AAPL")

**Protocol v1.** A request is one line of UTF-8 terminated by ``\\n``. A reply is
a header line — ``+<nbytes>\\n`` for success, ``-<nbytes>\\n`` for an error —
followed by exactly that many bytes. The error payload is the engine's own
diagnostic text, caret line and all.

Directives: ``\\mode text|json|jsonc|raw``, ``\\ping``, ``\\version``, ``\\quit``.
Anything else is evaluated, bare qSQL included.

``raw`` mode drops the header entirely, for compatibility with the engine's own
``ipc.k`` client. Do not use it from code: without a length there is no way to
distinguish a complete reply from a truncated one.
"""

from __future__ import annotations

import json
import socket
from typing import Any


_MUTATING = ("::", "delete ", "update ", 'upd"', 'del"', "hdel", "insert", "upsert",
             " set ", "`set", ".Q.dpft", "system", "\\\\")


def looks_like_mutation(text: str) -> bool:
    """amberd's --readonly rule, for Python servers: a best-effort guard against
    ACCIDENTS (a typo in a dashboard), not a security boundary -- Amber can
    assign through `.` without any of these tokens appearing.

    A mutating token anywhere, or `name:` at the start of a statement. Inside a
    statement `name:` names a column (`select vwap:wavg[sz;px] from t`) and is
    fine."""
    if any(tok in text for tok in _MUTATING):
        return True
    for stmt in text.split(";"):
        cut = min([i for i in (stmt.find('"'), stmt.find("{")) if i >= 0], default=len(stmt))
        head = stmt[:cut].lstrip(" \t")
        name = 0
        while name < len(head) and (head[name].isalnum() or head[name] == "."):
            name += 1
        if name and name < len(head) and head[name] == ":" and head[name + 1:name + 2] != ":":
            return True
        if cut < len(stmt):
            break
    return False


class AmberRemoteError(RuntimeError):
    """The server evaluated the request and the engine raised."""


class AmberClient:
    """A synchronous connection to one ``amberd``."""

    def __init__(self, host: str = "127.0.0.1", port: int = 5010, *, timeout: float = 30.0):
        self.host, self.port, self.timeout = host, port, timeout
        self._sock: socket.socket | None = None
        self._mode = "text"

    # ---- connection ------------------------------------------------------
    def connect(self) -> "AmberClient":
        if self._sock is None:
            self._sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
            self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return self

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.sendall(b"\\quit\n")
            except OSError:
                pass
            self._sock.close()
            self._sock = None

    def __enter__(self): return self.connect()
    def __exit__(self, *exc): self.close()

    # ---- framing ---------------------------------------------------------
    def _readline(self) -> bytes:
        assert self._sock is not None
        out = bytearray()
        while not out.endswith(b"\n"):
            chunk = self._sock.recv(1)
            if not chunk:
                raise ConnectionError("amberd closed the connection")
            out += chunk
        return bytes(out)

    def _readn(self, n: int) -> bytes:
        assert self._sock is not None
        out = bytearray()
        while len(out) < n:
            chunk = self._sock.recv(min(1 << 16, n - len(out)))
            if not chunk:
                raise ConnectionError("amberd closed the connection mid-reply")
            out += chunk
        return bytes(out)

    def request(self, line: str) -> str:
        """Send one request; return the payload, or raise on an error reply."""
        self.connect()
        assert self._sock is not None
        self._sock.sendall(line.encode() + b"\n")
        header = self._readline()
        status, size = header[:1], int(header[1:])
        payload = self._readn(size).decode("utf-8", "replace")
        if status == b"-":
            raise AmberRemoteError(payload)
        return payload

    # ---- directives ------------------------------------------------------
    def mode(self, name: str) -> None:
        self.request(f"\\mode {name}")
        self._mode = name

    def ping(self) -> bool:
        return self.request("\\ping") == "pong"

    def version(self) -> str:
        return self.request("\\version")

    # ---- queries ---------------------------------------------------------
    def text(self, query: str) -> str:
        """The result rendered as the REPL renders it (ANSI already stripped)."""
        if self._mode != "text":
            self.mode("text")
        return self.request(query)

    def json(self, query: str) -> Any:
        """Row-oriented: ``[{col: value, ...}, ...]``."""
        if self._mode != "json":
            self.mode("json")
        return json.loads(self.request(query))

    def columns(self, query: str) -> dict[str, list]:
        """Column-oriented: ``{col: [values...]}``. The shape a time-series
        dashboard wants, and the one the Grafana backend uses."""
        if self._mode != "jsonc":
            self.mode("jsonc")
        return json.loads(self.request(query))

    def pandas(self, query: str):
        """A DataFrame built from the columnar reply.

        One copy at the wire, and JSON's copy at that: this is the REMOTE path,
        where the data has to cross a socket regardless. When the engine is in
        your own process, ``python-amber``'s ``.to_pandas()`` has no wire and no
        copy -- prefer it whenever you are not actually talking to another host.
        """
        import pandas as pd  # noqa: PLC0415
        return pd.DataFrame(self.columns(query))

    def arrow(self, query: str):
        """A ``pyarrow.Table`` built from the columnar reply. Same caveat as
        :meth:`pandas`: JSON on the wire, one copy."""
        import pyarrow as pa  # noqa: PLC0415
        return pa.table(self.columns(query))
