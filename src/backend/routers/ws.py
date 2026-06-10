"""
WebSocket endpoints — the dashboard's live feeds, fanned out from Kafka by the Hub.

  /ws/lane/{lane_id}  — every camera crossing on that lane (drives the animation).
  /ws/violations      — each newly detected violation (drives the live log).

Each handler registers its socket with the Hub and then parks on a receive loop; it
sends nothing itself — the Hub pushes messages as Kafka delivers them. The receive
loop exists only to detect the client disconnecting so we can deregister.
"""

from __future__ import annotations

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from backend import db
from backend.live import hub

router = APIRouter(tags=["websocket"])


@router.websocket("/ws/lane/{lane_id}")
async def ws_lane(ws: WebSocket, lane_id: int) -> None:
    """Stream live camera events for one lane."""
    if db.get_lane(lane_id) is None:
        await ws.close(code=4404)   # policy-violation range: lane does not exist
        return
    await hub.connect_lane(ws, lane_id)
    try:
        while True:
            await ws.receive_text()   # ignore inbound; just wait for disconnect
    except WebSocketDisconnect:
        pass
    finally:
        hub.disconnect_lane(ws, lane_id)


@router.websocket("/ws/violations")
async def ws_violations(ws: WebSocket) -> None:
    """Stream every newly detected violation."""
    await hub.connect_violations(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        hub.disconnect_violations(ws)
