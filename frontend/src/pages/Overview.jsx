import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

import { api } from "../lib/api.js";
import { useViolationFeed } from "../lib/useViolationFeed.js";
import { Panel, StatCard, ConnectionDot, Spinner, ErrorState } from "../components/ui.jsx";
import LiveViolationFeed from "../components/LiveViolationFeed.jsx";
import { IconArrowRight } from "../components/icons.jsx";
import "./pages.css";

export default function Overview() {
  const [lanes, setLanes] = useState(null);
  const [error, setError] = useState(null);
  const { feed, status } = useViolationFeed();

  useEffect(() => {
    api.listLanes().then(setLanes).catch(setError);
  }, []);

  if (error) return <ErrorState error={error} />;
  if (!lanes) return <Spinner label="Loading network" />;

  const totals = lanes.reduce(
    (acc, l) => {
      acc.total += l.violations.total;
      acc.instant += l.violations.instantaneous;
      acc.average += l.violations.average;
      acc.cameras += l.camera_count;
      return acc;
    },
    { total: 0, instant: 0, average: 0, cameras: 0 }
  );

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <div className="eyebrow">Network status</div>
          <h1 className="page-title">Operations Overview</h1>
          <p className="page-desc">
            Average-speed enforcement across {lanes.length} monitored lanes.
          </p>
        </div>
        <div className="page-head-actions">
          <ConnectionDot status={status} label="VIOLATION FEED" />
        </div>
      </div>

      <div className="stat-grid">
        <StatCard label="Total Violations" value={totals.total} tone="info" />
        <StatCard label="Instantaneous" value={totals.instant} tone="instant" sub="single-camera over-limit" />
        <StatCard label="Average-Speed" value={totals.average} tone="average" sub="segment over-limit" />
        <StatCard label="Cameras Online" value={totals.cameras} tone="live" sub={`${lanes.length} lanes`} />
      </div>

      <div className="grid-2">
        <Panel title="Monitored Lanes" bodyClass="">
          <div className="lane-cards">
            {lanes.map((lane) => {
              const { total, instantaneous, average } = lane.violations;
              const denom = total || 1;
              return (
                <Link key={lane.lane_id} to={`/lanes/${lane.lane_id}`} className="lane-card">
                  <div className="lane-card-head">
                    <div>
                      <div className="lane-card-name">{lane.name}</div>
                      <div className="lane-card-meta mono">
                        LANE {lane.lane_id} · {lane.camera_count} cameras
                      </div>
                    </div>
                    <IconArrowRight style={{ color: "var(--text-2)" }} />
                  </div>
                  <div className="lane-card-total">{total}</div>
                  <div className="split-bar">
                    <i className="seg-instant" style={{ width: `${(instantaneous / denom) * 100}%` }} />
                    <i className="seg-average" style={{ width: `${(average / denom) * 100}%` }} />
                  </div>
                  <div className="split-legend">
                    <span className="legend-instant">{instantaneous} instant</span>
                    <span className="legend-average">{average} average</span>
                  </div>
                </Link>
              );
            })}
          </div>
        </Panel>

        <Panel
          title="Live Violation Feed"
          action={<ConnectionDot status={status} label="STREAMING" />}
          bodyClass="no-pad"
        >
          <div style={{ maxHeight: 460, overflowY: "auto", margin: -16 }}>
            <LiveViolationFeed
              violations={feed}
              showLane
              emptyHint="No violations recorded yet. New detections will appear here in real time."
            />
          </div>
        </Panel>
      </div>
    </div>
  );
}
