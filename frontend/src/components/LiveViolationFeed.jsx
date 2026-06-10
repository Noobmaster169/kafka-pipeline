import { useEffect, useRef } from "react";
import { ViolationBadge } from "./ui.jsx";
import { fmtAgo, fmtSpeed, violationKey, violationSpeed } from "../lib/format.js";
import "./LiveViolationFeed.css";

export default function LiveViolationFeed({ violations, showLane = false, emptyHint }) {
  // Track which keys we've already shown so only genuinely new rows flash.
  const seen = useRef(new Set());
  useEffect(() => {
    violations.forEach((v) => seen.current.add(violationKey(v)));
  });

  if (!violations.length) {
    return (
      <div className="feed-empty mono faint">
        {emptyHint || "No violations yet — waiting for the stream."}
      </div>
    );
  }

  return (
    <ul className="feed">
      {violations.map((v) => {
        const key = violationKey(v);
        const isNew = !seen.current.has(key);
        const speed = violationSpeed(v);
        return (
          <li key={key} className={`feed-row ${isNew ? "feed-row-new" : ""}`}>
            <span className="feed-rail" data-type={v.violation_type} />
            <div className="feed-main">
              <div className="feed-line1">
                <ViolationBadge type={v.violation_type} />
                <span className="feed-plate mono">{v.car_plate}</span>
                {showLane && <span className="tag">lane {v.lane_id}</span>}
              </div>
              <div className="feed-line2 mono faint">
                cam {v.camera_id_start}
                {v.camera_id_end !== v.camera_id_start && `→${v.camera_id_end}`} ·{" "}
                limit {fmtSpeed(v.speed_limit)}
              </div>
            </div>
            <div className="feed-right">
              <div className="feed-speed mono" data-type={v.violation_type}>
                {fmtSpeed(speed)}
                <span className="feed-unit"> km/h</span>
              </div>
              <div className="feed-ago mono faint">{fmtAgo(v.detected_at)}</div>
            </div>
          </li>
        );
      })}
    </ul>
  );
}
