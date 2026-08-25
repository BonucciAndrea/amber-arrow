"""amber-arrow -- streaming Apache Arrow over the Amber engine, zero-copy.

    >>> import amber as am, amber_arrow as aa
    >>> am.gentq(50_000_000)
    >>> reader = aa.query("select from trades", batch_rows=1_000_000)
    >>> for batch in reader:                       # 50 batches, no copies
    ...     process(batch)

``python-amber`` gives you a whole table at once. This gives you a
``pyarrow.RecordBatchReader``: the result is exported **once**, and each batch is
a window onto that export's buffers with Arrow's ``offset`` and ``length`` moved.
Pull one batch or five hundred; the bytes never move, and the engine's columns
stay alive exactly as long as some batch still points at them.

**One engine, not two.** This module deliberately loads ``libamber.so`` from
inside the installed ``amber`` package, with ``RTLD_GLOBAL``, *before* loading
``libamber_arrow.so``. Skip that and the loader is free to satisfy
libamber_arrow's dependency with a second copy from somewhere else on the search
path -- and a second copy is a second engine: two heaps, two symbol tables, two
global namespaces, and handles from one that are meaningless in the other, with
no error at any point because nothing is technically wrong. The engine records a
SONAME so that an already-loaded copy is recognised and reused; this module's job
is to make sure the right one is loaded first.
"""

from __future__ import annotations

import ctypes
import os
import sys
from pathlib import Path

__all__ = [
    "query", "stream", "table_to_arrow", "arrow_to_table",
    "ArrowArrayStream", "DEFAULT_BATCH", "AmberArrowError", "library_path",
]

DEFAULT_BATCH = 65536


class AmberArrowError(RuntimeError):
    """Raised when libamber_arrow reports a failure."""


# --------------------------------------------------------------------------
# the Arrow C Data Interface, in ctypes
# --------------------------------------------------------------------------

class ArrowSchema(ctypes.Structure):
    pass


ArrowSchema._fields_ = [
    ("format", ctypes.c_char_p), ("name", ctypes.c_char_p), ("metadata", ctypes.c_char_p),
    ("flags", ctypes.c_int64), ("n_children", ctypes.c_int64),
    ("children", ctypes.POINTER(ctypes.POINTER(ArrowSchema))),
    ("dictionary", ctypes.POINTER(ArrowSchema)),
    ("release", ctypes.CFUNCTYPE(None, ctypes.POINTER(ArrowSchema))),
    ("private_data", ctypes.c_void_p),
]


class ArrowArray(ctypes.Structure):
    pass


ArrowArray._fields_ = [
    ("length", ctypes.c_int64), ("null_count", ctypes.c_int64), ("offset", ctypes.c_int64),
    ("n_buffers", ctypes.c_int64), ("n_children", ctypes.c_int64),
    ("buffers", ctypes.POINTER(ctypes.c_void_p)),
    ("children", ctypes.POINTER(ctypes.POINTER(ArrowArray))),
    ("dictionary", ctypes.POINTER(ArrowArray)),
    ("release", ctypes.CFUNCTYPE(None, ctypes.POINTER(ArrowArray))),
    ("private_data", ctypes.c_void_p),
]


class ArrowArrayStream(ctypes.Structure):
    pass


ArrowArrayStream._fields_ = [
    ("get_schema", ctypes.CFUNCTYPE(ctypes.c_int, ctypes.POINTER(ArrowArrayStream),
                                    ctypes.POINTER(ArrowSchema))),
    ("get_next", ctypes.CFUNCTYPE(ctypes.c_int, ctypes.POINTER(ArrowArrayStream),
                                  ctypes.POINTER(ArrowArray))),
    ("get_last_error", ctypes.CFUNCTYPE(ctypes.c_char_p, ctypes.POINTER(ArrowArrayStream))),
    ("release", ctypes.CFUNCTYPE(None, ctypes.POINTER(ArrowArrayStream))),
    ("private_data", ctypes.c_void_p),
]


# --------------------------------------------------------------------------
# loading
# --------------------------------------------------------------------------

_SOEXT = "dylib" if sys.platform == "darwin" else "so"
_lib = None
_lib_path: str | None = None


def _engine_library() -> str | None:
    """The libamber the ``amber`` package is already using, if it is installed."""
    try:
        import amber  # noqa: PLC0415
    except ImportError:
        return None
    candidate = Path(amber.__file__).parent / f"libamber.{_SOEXT}"
    return str(candidate) if candidate.is_file() else None


def _search_paths() -> list[Path]:
    here = Path(__file__).resolve().parent
    name = f"libamber_arrow.{_SOEXT}"
    out = []
    env = os.environ.get("AMBER_ARROW_LIB")
    if env:
        out.append(Path(env))
    out += [
        here / name,                                    # bundled in the wheel
        here.parent.parent / "build" / name,            # a source checkout
        Path.cwd() / "build" / name,
        Path(f"/usr/local/lib/{name}"),
        Path(f"/usr/lib/{name}"),
    ]
    return out


def _load():
    global _lib, _lib_path
    if _lib is not None:
        return _lib

    # Order matters -- see the module docstring. RTLD_GLOBAL on the engine first,
    # by its exact path, so libamber_arrow's DT_NEEDED binds to the copy the
    # `amber` package is already running and not to a second one.
    engine = _engine_library()
    if engine:
        ctypes.CDLL(engine, mode=ctypes.RTLD_GLOBAL)

    last = None
    for candidate in _search_paths():
        if not candidate.is_file():
            continue
        try:
            _lib = ctypes.CDLL(str(candidate))
            _lib_path = str(candidate)
            break
        except OSError as exc:                          # noqa: PERF203
            last = exc
    if _lib is None:
        raise AmberArrowError(
            f"libamber_arrow.{_SOEXT} not found. Build it with ./build.sh, or set "
            f"$AMBER_ARROW_LIB to its path. Tried:\n  " +
            "\n  ".join(str(p) for p in _search_paths()) +
            (f"\nlast error: {last}" if last else "")
        )

    _lib.amber_arrow_abi_version.restype = ctypes.c_int
    _lib.amber_arrow_last_error.restype = ctypes.c_char_p
    _lib.amber_arrow_stream_table.argtypes = [ctypes.c_uint64, ctypes.c_longlong, ctypes.c_void_p]
    _lib.amber_arrow_stream_table.restype = ctypes.c_int
    _lib.amber_arrow_stream_query.argtypes = [ctypes.c_char_p, ctypes.c_longlong, ctypes.c_void_p]
    _lib.amber_arrow_stream_query.restype = ctypes.c_int
    _lib.amber_arrow_export_table.argtypes = [ctypes.c_uint64, ctypes.c_void_p, ctypes.c_void_p]
    _lib.amber_arrow_export_table.restype = ctypes.c_int
    _lib.amber_arrow_import_table.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.amber_arrow_import_table.restype = ctypes.c_uint64
    _lib.amber_arrow_import_stream.argtypes = [ctypes.c_void_p]
    _lib.amber_arrow_import_stream.restype = ctypes.c_uint64
    return _lib


def library_path() -> str:
    """Where ``libamber_arrow`` was loaded from. Useful when two are installed."""
    _load()
    return _lib_path or "<unknown>"


def _fail() -> AmberArrowError:
    return AmberArrowError((_lib.amber_arrow_last_error() or b"").decode() or
                           "amber-arrow: unspecified failure")


# --------------------------------------------------------------------------
# streaming
# --------------------------------------------------------------------------

def _reader_from(stream_struct: ArrowArrayStream):
    """Hand a filled ArrowArrayStream to PyArrow, which MOVES it."""
    import pyarrow as pa  # noqa: PLC0415
    return pa.RecordBatchReader._import_from_c(ctypes.addressof(stream_struct))


def stream(table, batch_rows: int = 0):
    """A ``pyarrow.RecordBatchReader`` over an Amber table.

    ``table`` may be an ``amber.Table``, an ``amber.Value`` holding a table, or a
    raw 64-bit engine handle.
    """
    lib = _load()
    handle = _handle_of(table)
    out = ArrowArrayStream()
    if lib.amber_arrow_stream_table(ctypes.c_uint64(handle),
                                    ctypes.c_longlong(batch_rows),
                                    ctypes.byref(out)) != 0:
        raise _fail()
    return _reader_from(out)


def query(text: str, batch_rows: int = 0):
    """Evaluate ``text`` -- bare qSQL accepted -- and stream the result.

    A keyed result (``select ... by sym from t``) is flattened with the grouping
    columns in front: Arrow has no keyed table, and dropping the keys would be
    worse than flattening them.
    """
    lib = _load()
    out = ArrowArrayStream()
    if lib.amber_arrow_stream_query(text.encode(),
                                    ctypes.c_longlong(batch_rows),
                                    ctypes.byref(out)) != 0:
        raise _fail()
    return _reader_from(out)


def table_to_arrow(table):
    """The whole table as a ``pyarrow.Table`` (one batch, still zero-copy)."""
    import pyarrow as pa  # noqa: PLC0415
    lib = _load()
    schema, array = ArrowSchema(), ArrowArray()
    if lib.amber_arrow_export_table(ctypes.c_uint64(_handle_of(table)),
                                    ctypes.byref(schema), ctypes.byref(array)) != 0:
        raise _fail()
    batch = pa.RecordBatch._import_from_c(ctypes.addressof(array), ctypes.addressof(schema))
    return pa.Table.from_batches([batch])


def arrow_to_table(source):
    """Import a ``pyarrow.Table``, ``RecordBatch`` or ``RecordBatchReader`` into
    the engine, returning an ``amber.Table``.

    This direction copies -- Amber stores a 32-byte object header immediately
    before every vector payload, so it cannot adopt a foreign allocation.
    """
    import amber  # noqa: PLC0415
    import pyarrow as pa  # noqa: PLC0415
    lib = _load()

    if isinstance(source, pa.RecordBatchReader):
        holder = ArrowArrayStream()
        source._export_to_c(ctypes.addressof(holder))
        handle = lib.amber_arrow_import_stream(ctypes.byref(holder))
    else:
        if isinstance(source, pa.Table):
            batches = source.combine_chunks().to_batches()
            if not batches:
                raise ValueError("amber-arrow: table has no record batches")
            source = batches[0]
        schema, array = ArrowSchema(), ArrowArray()
        source._export_to_c(ctypes.addressof(array), ctypes.addressof(schema))
        handle = lib.amber_arrow_import_table(ctypes.byref(schema), ctypes.byref(array))
    if not handle:
        raise _fail()
    return amber.Table(_wrap_handle(handle))


# --------------------------------------------------------------------------
# handle plumbing
# --------------------------------------------------------------------------

def _handle_of(table) -> int:
    if isinstance(table, int):
        return table
    value = getattr(table, "value", table)          # amber.Table -> amber.Value
    handle = getattr(value, "handle", None)
    if handle is None:
        raise TypeError(f"amber-arrow: cannot get an engine handle from {type(table).__name__}")
    return handle()


def _wrap_handle(handle: int):
    """Adopt a raw engine handle as an ``amber.Value``.

    The C side returned an OWNED reference. ``amber.get_global`` is used as the
    adoption path -- bind the handle to a scratch global through the engine, read
    it back as a Value, release the original -- because it is the only route that
    keeps refcounting entirely inside the engine. Manufacturing a Value from an
    integer in Python would work right up until the day the ownership rule
    changed, and would then fail as a use-after-free rather than as an error.
    """
    import amber  # noqa: PLC0415
    from amber import _amber  # noqa: PLC0415
    engine = ctypes.CDLL(_engine_library() or "libamber.so", mode=ctypes.RTLD_GLOBAL)
    engine.amber_set_global.argtypes = [ctypes.c_char_p, ctypes.c_uint64]
    engine.amber_set_global.restype = ctypes.c_int
    engine.amber_release.argtypes = [ctypes.c_uint64]
    engine.amber_release.restype = None
    name = b"amberarrow.scratch"
    if engine.amber_set_global(name, ctypes.c_uint64(handle)) != 0:
        engine.amber_release(ctypes.c_uint64(handle))
        raise AmberArrowError("amber-arrow: could not adopt the imported table")
    engine.amber_release(ctypes.c_uint64(handle))
    value = _amber.get_global("amberarrow.scratch")
    _amber.set_global("amberarrow.scratch", None)
    return value
