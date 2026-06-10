import { useEffect, useState } from "react";

import { api } from "../lib/api.js";
import { Panel, ViolationBadge, Spinner, ErrorState } from "../components/ui.jsx";
import { IconDownload } from "../components/icons.jsx";
import { fmtDateTime, fmtSpeed, violationSpeed } from "../lib/format.js";
import "./pages.css";

const PAGE = 20;

export default function Violations() {
  const [lanes, setLanes] = useState([]);
  const [filters, setFilters] = useState({
    lane_id: "",
    violation_type: "",
    car_plate: "",
    date: "",
  });
  const [skip, setSkip] = useState(0);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    api.listLanes().then(setLanes).catch(() => {});
  }, []);

  useEffect(() => {
    setResult(null);
    api
      .listViolations({ ...filters, skip, limit: PAGE })
      .then(setResult)
      .catch(setError);
  }, [filters, skip]);

  const set = (k) => (e) => {
    setFilters((f) => ({ ...f, [k]: e.target.value }));
    setSkip(0);
  };

  if (error) return <ErrorState error={error} />;

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <div className="eyebrow">Enforcement record</div>
          <h1 className="page-title">Violation Tracking</h1>
          <p className="page-desc">
            Every recorded violation, filterable and exportable. One document per offence.
          </p>
        </div>
        <div className="page-head-actions">
          <a className="btn btn-primary" href={api.exportCsvUrl(filters)} download>
            <IconDownload width={16} height={16} /> Export CSV
          </a>
        </div>
      </div>

      <Panel style={{ marginBottom: 18 }}>
        <div className="toolbar">
          <div className="field">
            <label>Lane</label>
            <select className="select" value={filters.lane_id} onChange={set("lane_id")}>
              <option value="">All lanes</option>
              {lanes.map((l) => (
                <option key={l.lane_id} value={l.lane_id}>
                  {l.name}
                </option>
              ))}
            </select>
          </div>
          <div className="field">
            <label>Type</label>
            <select
              className="select"
              value={filters.violation_type}
              onChange={set("violation_type")}
            >
              <option value="">All types</option>
              <option value="INSTANTANEOUS">Instantaneous</option>
              <option value="AVERAGE">Average</option>
            </select>
          </div>
          <div className="field">
            <label>Plate</label>
            <input
              className="input"
              placeholder="exact plate"
              value={filters.car_plate}
              onChange={set("car_plate")}
            />
          </div>
          <div className="field">
            <label>Date</label>
            <input className="input" type="date" value={filters.date} onChange={set("date")} />
          </div>
        </div>
      </Panel>

      <Panel title={result ? `${result.total.toLocaleString()} violations` : "Violations"}>
        {!result ? (
          <Spinner label="Querying" />
        ) : result.items.length === 0 ? (
          <div className="empty mono">No violations match these filters.</div>
        ) : (
          <>
            <table className="table">
              <thead>
                <tr>
                  <th>Detected</th>
                  <th>Type</th>
                  <th>Plate</th>
                  <th>Lane</th>
                  <th>Segment</th>
                  <th style={{ textAlign: "right" }}>Speed</th>
                  <th style={{ textAlign: "right" }}>Limit</th>
                </tr>
              </thead>
              <tbody>
                {result.items.map((v, i) => (
                  <tr key={`${v.id ?? i}`}>
                    <td className="num faint">{fmtDateTime(v.detected_at)}</td>
                    <td><ViolationBadge type={v.violation_type} /></td>
                    <td className="num" style={{ fontWeight: 700 }}>{v.car_plate}</td>
                    <td className="num">{v.lane_id}</td>
                    <td className="num faint">
                      cam {v.camera_id_start}
                      {v.camera_id_end !== v.camera_id_start && `→${v.camera_id_end}`}
                    </td>
                    <td
                      className="num"
                      style={{
                        textAlign: "right",
                        fontWeight: 700,
                        color:
                          v.violation_type === "AVERAGE" ? "var(--average)" : "var(--instant)",
                      }}
                    >
                      {fmtSpeed(violationSpeed(v))}
                    </td>
                    <td className="num faint" style={{ textAlign: "right" }}>
                      {fmtSpeed(v.speed_limit)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div className="pager">
              <span className="mono">
                {skip + 1}–{Math.min(skip + PAGE, result.total)} of{" "}
                {result.total.toLocaleString()}
              </span>
              <div className="pager-btns">
                <button
                  className="btn"
                  disabled={skip === 0}
                  onClick={() => setSkip(Math.max(0, skip - PAGE))}
                >
                  Prev
                </button>
                <button
                  className="btn"
                  disabled={skip + PAGE >= result.total}
                  onClick={() => setSkip(skip + PAGE)}
                >
                  Next
                </button>
              </div>
            </div>
          </>
        )}
      </Panel>
    </div>
  );
}
