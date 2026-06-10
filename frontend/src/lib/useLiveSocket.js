import { useEffect, useRef, useState } from "react";

// Subscribe to one of the backend's WebSocket feeds (`/ws/lane/{id}` or
// `/ws/violations`). Each decoded JSON message is handed to `onMessage`. The hook
// auto-reconnects with backoff and exposes a connection `status` so the UI can show
// a LIVE / CONNECTING / OFFLINE indicator — the dashboard stays informative even
// when no broker is delivering (the feeds simply read OFFLINE until traffic flows).
//
// `status`: "connecting" | "live" | "offline"

const WS_BASE = import.meta.env.VITE_WS_BASE ?? "";

function wsUrl(path) {
  if (WS_BASE) return `${WS_BASE}${path}`;
  const proto = location.protocol === "https:" ? "wss" : "ws";
  return `${proto}://${location.host}${path}`;
}

export function useLiveSocket(path, onMessage, { enabled = true } = {}) {
  const [status, setStatus] = useState("connecting");
  const handlerRef = useRef(onMessage);
  handlerRef.current = onMessage;

  useEffect(() => {
    if (!enabled || !path) return;

    let ws;
    let retry;
    let closed = false;
    let attempt = 0;

    const connect = () => {
      setStatus("connecting");
      ws = new WebSocket(wsUrl(path));

      ws.onopen = () => {
        attempt = 0;
        setStatus("live");
      };
      ws.onmessage = (ev) => {
        try {
          handlerRef.current?.(JSON.parse(ev.data));
        } catch {
          /* ignore malformed frame */
        }
      };
      ws.onclose = () => {
        if (closed) return;
        setStatus("offline");
        attempt += 1;
        const delay = Math.min(1000 * 2 ** attempt, 10000); // capped backoff
        retry = setTimeout(connect, delay);
      };
      ws.onerror = () => ws.close();
    };

    connect();
    return () => {
      closed = true;
      clearTimeout(retry);
      ws?.close();
    };
  }, [path, enabled]);

  return status;
}
