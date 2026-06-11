import { useCallback, useEffect, useRef, useState } from "react";
import { useParams } from "react-router-dom";

import { api } from "../lib/api.js";
import { useLiveSocket } from "../lib/useLiveSocket.js";
import { useViolationFeed } from "../lib/useViolationFeed.js";
import { Panel, StatCard, ConnectionDot, Spinner, ErrorState } from "../components/ui.jsx";
import LaneSchematic from "../components/LaneSchematic.jsx";
import LaneHighway3D from "../components/LaneHighway3D.jsx";
import LiveViolationFeed from "../components/LiveViolationFeed.jsx";
import { fmtKm } from "../lib/format.js";
import "./pages.css";

export default function LaneDashboard() {
  const { laneId } = useParams();
  const id = Number(laneId);
  const [lane, setLane] = useState(null);
  const [error, setError] = useState(null);

  // Buffer camera-event crossings off the socket; the schematic and the 3D
  // highway each drain their own queue on their own animation tick.
  const eventQueueRef = useRef([]);
  const highwayQueueRef = useRef([]);

  useEffect(() => {
    setLane(null);                 // reset only when switching lanes
    setError(null);
    eventQueueRef.current = [];
    highwayQueueRef.current = [];

    let alive = true;
    const loadData = () => {
      api.getLane(id)
        .then((data) => { if (alive) setLane(data); })
        .catch((e) => { if (alive) setLane((cur) => { if (!cur) setError(e); return cur; }); });
    };

    loadData();
    const interval = setInterval(loadData, 5000);
    return () => { alive = false; clearInterval(interval); };
  }, [id]);

  const onCameraEvent = useCallback((ev) => {
    eventQueueRef.current.push(ev);
    highwayQueueRef.current.push(ev);
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

      <Panel
        title="Lane Highway · 3D"
        action={<span className="eyebrow">live crossings</span>}
        style={{ marginBottom: 18 }}
        bodyClass=""
      >
         <LaneHighway3D cameras={lane.cameras} eventQueueRef={highwayQueueRef} violations={feed} />
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