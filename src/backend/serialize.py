"""
Make a MongoDB document JSON-serialisable for a FastAPI response.

Mongo hands back two types FastAPI's default encoder can't emit: the `_id`
`ObjectId` and `datetime` objects. `jsonify` returns a shallow copy with `_id`
turned into a string `id` and every datetime rendered as an ISO-8601 string, so
the same shape is safe to return from any endpoint or push down a WebSocket.
"""

from __future__ import annotations

from datetime import datetime

from bson import ObjectId


def jsonify(doc: dict) -> dict:
    """Return a JSON-safe shallow copy of a Mongo document."""
    out: dict = {}
    for key, value in doc.items():
        if key == "_id":
            out["id"] = str(value)
        elif isinstance(value, ObjectId):
            out[key] = str(value)
        elif isinstance(value, datetime):
            out[key] = value.isoformat()
        else:
            out[key] = value
    return out
