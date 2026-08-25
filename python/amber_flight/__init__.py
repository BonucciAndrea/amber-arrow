"""amber-flight -- Apache Arrow Flight over the Amber engine."""
from .server import AmberFlightServer, DEFAULT_BATCH  # noqa: F401

__all__ = ["AmberFlightServer", "DEFAULT_BATCH", "serve"]
__version__ = "1.9.6"


def serve(host: str = "127.0.0.1", port: int = 8815, **kwargs) -> None:
    """Run a Flight server until interrupted."""
    location = f"grpc://{host}:{port}"
    server = AmberFlightServer(location, **kwargs)
    import sys
    print(f"amber-flight: serving {location}", file=sys.stderr)
    server.serve()
