import { useCallback, useRef } from "react";
import { useLiveSocket } from "../lib/useLiveSocket.js";
import LaneHighway3D from "./LaneHighway3D.jsx";

// One lane's live 3D highway: owns its own socket and event queue.
export default function HighwayStrip({ lane, cameras, violations = [] }) {
  const queueRef = useRef([]);
  const onEvent = useCallback((ev) => {
    queueRef.current.push(ev);
  }, []);
  useLiveSocket(`/ws/lane/${lane.lane_id}`, onEvent);

  return (
    <div style={{ marginBottom: 12 }}>
      <div className="eyebrow" style={{ marginBottom: 4 }}>
        {lane.name} · LANE {lane.lane_id}
      </div>
      <LaneHighway3D cameras={cameras} eventQueueRef={queueRef} violations={violations} />
    </div>
  );
}