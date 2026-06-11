import { useEffect, useState } from "react";

import { api } from "../lib/api.js";
import { Panel, Spinner, ErrorState } from "../components/ui.jsx";
import { IconPlus } from "../components/icons.jsx";
import "./pages.css";

export default function Cameras() {
  const [lanes, setLanes] = useState(null);
  const [cameras, setCameras] = useState([]);
  const [error, setError] = useState(null);

  const [laneId, setLaneId] = useState("");
  const [limit, setLimit] = useState("");
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState(null);

  const load = () =>
    Promise.all([api.listLanes(), api.listCameras()])
      .then(([ls, cs]) => {
        setLanes(ls);
        setCameras(cs);
        if (!laneId && ls.length) setLaneId(String(ls[0].lane_id));
      })
      .catch(setError);

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function addCamera(e) {
    e.preventDefault();
    setBusy(true);
    setMsg(null);
    try {
      const body = { lane_id: Number(laneId) };
      if (limit) body.speed_limit = Number(limit);
      const cam = await api.addCamera(body);
      setMsg({
        ok: true,
        text: `Camera ${cam.camera_id} appended at ${cam.position_km} km (${cam.speed_limit} km/h). The running simulator picks it up on its next config refresh — no restart.`,
      });
      setLimit("");
      await load();
    } catch (err) {
      setMsg({ ok: false, text: err.message });
    } finally {
      setBusy(false);
    }
  }

  async function removeLast(lane) {
  if (!confirm(`Remove the last camera from ${lane.name}?`)) return;
  setBusy(true);
  setMsg(null);
  try {
    const { removed } = await api.removeLastCamera(lane.lane_id);
    setMsg({
      ok: true,
      text: `Camera ${removed.camera_id} removed from ${lane.name} (was at ${removed.position_km} km). The simulator drops it on its next config refresh — no restart.`,
    });
    await load();
  } catch (err) {
    setMsg({ ok: false, text: err.message });
  } finally {
    setBusy(false);
  }
}

  if (error) return <ErrorState error={error} />;
  if (!lanes) return <Spinner label="Loading cameras" />;

  const byLane = lanes.map((lane) => ({
    lane,
    cams: cameras
      .filter((c) => c.lane_id === lane.lane_id)
      .sort((a, b) => a.position_km - b.position_km),
  }));

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <div className="eyebrow">Infrastructure</div>
          <h1 className="page-title">Camera Management</h1>
          <p className="page-desc">
            {cameras.length} cameras across {lanes.length} lanes. New cameras append to the
            end of a lane and become active without a pipeline restart.
          </p>
        </div>
      </div>

      <Panel title="Append Camera" style={{ marginBottom: 18 }}>
        <form className="form-grid" onSubmit={addCamera}>
          <div className="field">
            <label>Lane</label>
            <select className="select" value={laneId} onChange={(e) => setLaneId(e.target.value)}>
              {lanes.map((l) => (
                <option key={l.lane_id} value={l.lane_id}>
                  {l.name} (lane {l.lane_id})
                </option>
              ))}
            </select>
          </div>
          <div className="field">
            <label>Speed limit (optional)</label>
            <input
              className="input"
              type="number"
              min="1"
              placeholder="inherit lane limit"
              value={limit}
              onChange={(e) => setLimit(e.target.value)}
            />
          </div>
          <button className="btn btn-primary" disabled={busy} type="submit">
            <IconPlus width={16} height={16} />
            {busy ? "Appending…" : "Append camera"}
          </button>
        </form>
        {msg && <div className={`form-msg ${msg.ok ? "ok" : "err"}`}>{msg.text}</div>}
      </Panel>
      <div className="grid-cols">
        {byLane.map(({ lane, cams }) => (
          <Panel
            key={lane.lane_id}
            title={`${lane.name} · LANE ${lane.lane_id}`}
            action={
              <button
                className="btn"
                type="button"
                disabled={busy || cams.length <= 2}
                title={
                  cams.length <= 2
                    ? "A lane needs at least 2 cameras"
                    : "Remove the end-of-lane camera"
                }
                onClick={() => removeLast(lane)}
              >
                − Remove last
              </button>
            }
          >
            <div className="cam-row">
              {cams.map((c) => (
                <div key={c.camera_id} className="cam-chip">
                  <span className="cam-id">CAM {c.camera_id}</span>
                  <span className="cam-pos">{c.position_km}</span>
                  <span className="cam-limit mono">km · {c.speed_limit} limit</span>
                </div>
              ))}
            </div>
          </Panel>
        ))}
      </div>
    </div>
  );
}
