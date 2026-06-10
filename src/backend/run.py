"""
Backend entry point.

Run from src/ (so `backend` / `common` import cleanly):

    python -m backend.run                 # serve on BACKEND_HOST:BACKEND_PORT
    BACKEND_PORT=9000 python -m backend.run

For the host-against-stack setup (Kafka/Mongo published on localhost):

    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 MONGO_HOST=localhost python -m backend.run
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import uvicorn  # noqa: E402

from common import config  # noqa: E402


def main() -> int:
    uvicorn.run("backend.main:app", host=config.BACKEND_HOST, port=config.BACKEND_PORT)
    return 0


if __name__ == "__main__":
    sys.exit(main())
