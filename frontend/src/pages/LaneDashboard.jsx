import { useCallback, useEffect, useRef, useState } from "react";
import { useParams } from "react-router-dom";

import { api } from "../lib/api.js";
import { useLiveSocket } from "../lib/useLiveSocket.js";
import { useViolationFeed } from "../lib/useViolationFeed.js";
import { Panel, StatCard, ConnectionDot, Spinner, ErrorState } from "../components/ui.jsx";
import LaneSchematic from "../components/LaneSchematic.jsx";
import LiveViolationFeed from "../components/LiveViolationFeed.jsx";
import { fmtKm } from "../lib/format.js";
import "./pages.css";

export default function LaneDashboard() {
  const { laneId } = useParams();
  const id = Number(laneId);
  const [lane, setLane] = useState(null);
  const [error, setError] = useState(null);

  // Buffer camera-event crossings off the socket; the schematic drains the queue
  // on its own animation tick (decoupling socket cadence from rendering).
  const eventQueueRef = useRef([]);

  useEffect(() => {
    setLane(null);
    setError(null);
    eventQueueRef.current = [];
    api.getLane(id).then(setLane).catch(setError);
  }, [id]);

  const onCameraEvent = useCallback((ev) => {
    eventQueueRef.current.push(ev);
  }, []);
  const laneStatus = useLiveSocket(`/ws/lane/${id}`, onCameraEvent);
  const { feed, status: vioStatus } = useViolationFeed({ laneId: id });

  if (error) return <ErrorState error={error} />;
  if (!lane) return <Spinner label={`Loading lane ${id}`} />;

  const { total, instantaneous, average } = lane.violations;

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <div className="eyebrow">Lane {lane.lane_id} · monitoring</div>
          <h1 className="page-title">{lane.name}</h1>
          <p className="page-desc">
            {lane.cameras.length} cameras over {fmtKm(lane.cameras.at(-1)?.position_km)} ·
            limits {[...new Set(lane.cameras.map((c) => c.speed_limit))].join("/")} km/h
          </p>
        </div>
        <div className="page-head-actions">
          <ConnectionDot status={laneStatus} label="CAMERA FEED" />
          <ConnectionDot status={vioStatus} label="VIOLATIONS" />
        </div>
      </div>

      <div className="stat-grid">
        <StatCard label="Total Violations" value={total} tone="info" />
        <StatCard label="Instantaneous" value={instantaneous} tone="instant" />
        <StatCard label="Average-Speed" value={average} tone="average" />
        <StatCard label="Cameras" value={lane.cameras.length} tone="live" />
      </div>

      <Panel
        title="Live Lane Schematic"
        action={<span className="eyebrow">real-time crossings</span>}
        className="rise"
        style={{ marginBottom: 18 }}
        bodyClass=""
      >
        <LaneSchematic cameras={lane.cameras} eventQueueRef={eventQueueRef} />
      </Panel>

      <div className="grid-2">
        <Panel title="Camera Gantries">
          <div className="cam-row">
            {lane.cameras.map((c) => (
              <div key={c.camera_id} className="cam-chip">
                <span className="cam-id">CAM {c.camera_id}</span>
                <span className="cam-pos">{c.position_km}</span>
                <span className="cam-limit mono">km · {c.speed_limit} limit</span>
              </div>
            ))}
          </div>
        </Panel>

        <Panel
          title="Lane Violation Feed"
          action={<ConnectionDot status={vioStatus} label="STREAMING" />}
        >
          <div style={{ maxHeight: 360, overflowY: "auto", margin: -16 }}>
            <LiveViolationFeed
              violations={feed}
              emptyHint="No violations on this lane yet."
            />
          </div>
        </Panel>
      </div>
    </div>
  );
}
