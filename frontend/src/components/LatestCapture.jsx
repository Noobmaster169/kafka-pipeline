import { useEffect, useState } from "react";
import { api } from "../lib/api.js";
import { Panel } from "./ui.jsx";
import CarViewer from "./CarViewer.jsx";
import { fmtAgo, violationSpeed } from "../lib/format.js";

export default function LatestCapture({ violation }) {
  const [car, setCar] = useState(null);

  useEffect(() => {
    if (!violation) return;
    let alive = true;
    api.getCar(violation.car_plate)
      .then((c) => { if (alive) setCar(c); })
      .catch(() => { if (alive) setCar(null); });
    return () => { alive = false; };
  }, [violation?.car_plate]);

  if (!violation) {
    return (
      <Panel title="Latest Capture" style={{ marginTop: 18 }}>
        <div className="faint mono">Waiting for the next detection…</div>
      </Panel>
    );
  }

  const isAvg = violation.violation_type === "AVERAGE";
  return (
    <Panel
      title="Latest Capture"
      action={<span className="eyebrow">{car?.vehicle_type ?? "vehicle"}</span>}
      style={{ marginTop: 18 }}
    >
      <CarViewer type={car?.vehicle_type} plate={violation.car_plate} />
      <div className="mono" style={{ display: "flex", gap: 16, justifyContent: "center", paddingTop: 8 }}>
        <span className={isAvg ? "legend-average" : "legend-instant"}>
          {violation.violation_type}
        </span>
        <span>{Math.round(violationSpeed(violation))} km/h · limit {violation.speed_limit}</span>
        <span className="faint">
          {isAvg ? `cam ${violation.camera_id_start}→${violation.camera_id_end}` : `cam ${violation.camera_id_start}`}
          {" · "}{fmtAgo(violation.detected_at ?? violation.timestamp_end)}
        </span>
      </div>
    </Panel>
  );
}