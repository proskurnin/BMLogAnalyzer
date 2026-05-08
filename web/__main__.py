from __future__ import annotations

import argparse
from typing import Sequence


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the BM Log Analyzer web service.")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind.")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind.")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise SystemExit("uvicorn is not installed. Install fastapi and uvicorn to run the web service.") from exc

    uvicorn.run("web.app:create_app", host=args.host, port=args.port, reload=args.reload, factory=True)
    return 0


if __name__ == "__main__":  # pragma: no cover - manual entrypoint
    raise SystemExit(main())
