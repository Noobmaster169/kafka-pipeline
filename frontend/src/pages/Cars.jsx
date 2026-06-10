import { useEffect, useState } from "react";

import { api } from "../lib/api.js";
import { Panel, Spinner, ErrorState } from "../components/ui.jsx";
import LiveViolationFeed from "../components/LiveViolationFeed.jsx";
import { IconSearch, IconPlus } from "../components/icons.jsx";
import { fmtDateTime } from "../lib/format.js";
import "./pages.css";

const PAGE = 12;

export default function Cars() {
  const [query, setQuery] = useState("");
  const [skip, setSkip] = useState(0);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  const [selected, setSelected] = useState(null); // detailed car (with violations)
  const [registering, setRegistering] = useState(false);

  useEffect(() => {
    api
      .listCars({ plate: query || undefined, skip, limit: PAGE })
      .then(setResult)
      .catch(setError);
  }, [query, skip]);

  function openCar(plate) {
    setRegistering(false);
    setSelected("loading");
    api.getCar(plate).then(setSelected).catch(setError);
  }

  if (error) return <ErrorState error={error} />;

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <div className="eyebrow">Registry</div>
          <h1 className="page-title">Vehicle Management</h1>
          <p className="page-desc">
            {result ? result.total.toLocaleString() : "—"} registered vehicles. Search by
            plate, inspect a vehicle's violation history, or register a new one.
          </p>
        </div>
        <div className="page-head-actions">
          <button
            className="btn"
            onClick={() => {
              setRegistering(true);
              setSelected(null);
            }}
          >
            <IconPlus width={16} height={16} /> Register vehicle
          </button>
        </div>
      </div>

      <div className="grid-2">
        <Panel
          title="Vehicle Registry"
          action={
            <div className="field" style={{ minWidth: 220 }}>
              <div style={{ position: "relative" }}>
                <IconSearch
                  width={15}
                  height={15}
                  style={{
                    position: "absolute",
                    left: 10,
                    top: "50%",
                    transform: "translateY(-50%)",
                    color: "var(--text-2)",
                  }}
                />
                <input
                  className="input"
                  style={{ paddingLeft: 32, width: "100%" }}
                  placeholder="plate prefix…"
                  value={query}
                  onChange={(e) => {
                    setQuery(e.target.value.toUpperCase());
                    setSkip(0);
                  }}
                />
              </div>
            </div>
          }
          bodyClass=""
        >
          {!result ? (
            <Spinner label="Searching" />
          ) : result.items.length === 0 ? (
            <div className="empty mono">No vehicles match “{query}”.</div>
          ) : (
            <>
              <table className="table">
                <thead>
                  <tr>
                    <th>Plate</th>
                    <th>Owner</th>
                    <th>Type</th>
                  </tr>
                </thead>
                <tbody>
                  {result.items.map((c) => (
                    <tr
                      key={c.car_plate}
                      className={`clickable ${
                        selected?.car_plate === c.car_plate ? "row-selected" : ""
                      }`}
                      onClick={() => openCar(c.car_plate)}
                    >
                      <td className="num" style={{ fontWeight: 700 }}>{c.car_plate}</td>
                      <td>{c.owner_name}</td>
                      <td><span className="tag">{c.vehicle_type}</span></td>
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

        {registering ? (
          <RegisterPanel
            onDone={(plate) => {
              setRegistering(false);
              setQuery(plate.toUpperCase());
              setSkip(0);
              openCar(plate);
            }}
          />
        ) : (
          <CarDetail car={selected} />
        )}
      </div>
    </div>
  );
}

function CarDetail({ car }) {
  if (!car) {
    return (
      <Panel title="Vehicle Detail">
        <div className="empty mono">Select a vehicle to view its record and violations.</div>
      </Panel>
    );
  }
  if (car === "loading") {
    return (
      <Panel title="Vehicle Detail">
        <Spinner label="Loading vehicle" />
      </Panel>
    );
  }
  return (
    <Panel title="Vehicle Detail">
      <div className="detail-grid">
        <Field label="Plate" value={car.car_plate} mono big />
        <Field label="Type" value={car.vehicle_type} />
        <Field label="Owner" value={car.owner_name} />
        <Field label="Registered" value={fmtDateTime(car.registration_date)} mono />
        <Field label="Address" value={car.owner_addr} span />
      </div>
      <div className="eyebrow" style={{ margin: "18px 0 8px" }}>
        Violations ({car.violations.length})
      </div>
      <div style={{ margin: "0 -16px -16px", maxHeight: 320, overflowY: "auto" }}>
        <LiveViolationFeed
          violations={car.violations}
          showLane
          emptyHint="Clean record — no violations."
        />
      </div>
    </Panel>
  );
}

function Field({ label, value, mono, big, span }) {
  return (
    <div className="detail-field" style={span ? { gridColumn: "1 / -1" } : undefined}>
      <div className="eyebrow">{label}</div>
      <div
        className={mono ? "mono" : ""}
        style={{ fontSize: big ? 18 : 14, fontWeight: big ? 700 : 500, marginTop: 3 }}
      >
        {value}
      </div>
    </div>
  );
}

function RegisterPanel({ onDone }) {
  const [form, setForm] = useState({
    car_plate: "",
    owner_name: "",
    owner_addr: "",
    vehicle_type: "Sedan",
    registration_date: new Date().toISOString().slice(0, 10),
  });
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState(null);
  const set = (k) => (e) => setForm({ ...form, [k]: e.target.value });

  async function submit(e) {
    e.preventDefault();
    setBusy(true);
    setMsg(null);
    try {
      await api.addCar(form);
      onDone(form.car_plate);
    } catch (err) {
      setMsg(err.message);
      setBusy(false);
    }
  }

  return (
    <Panel title="Register Vehicle">
      <form onSubmit={submit} className="reg-form">
        <div className="field">
          <label>Plate</label>
          <input className="input" required value={form.car_plate} onChange={set("car_plate")} />
        </div>
        <div className="field">
          <label>Owner name</label>
          <input className="input" required value={form.owner_name} onChange={set("owner_name")} />
        </div>
        <div className="field">
          <label>Address</label>
          <input className="input" value={form.owner_addr} onChange={set("owner_addr")} />
        </div>
        <div className="form-grid">
          <div className="field">
            <label>Type</label>
            <select className="select" value={form.vehicle_type} onChange={set("vehicle_type")}>
              {["Sedan", "Coupe", "Truck", "SUV", "Van", "Motorcycle"].map((t) => (
                <option key={t}>{t}</option>
              ))}
            </select>
          </div>
          <div className="field">
            <label>Registration date</label>
            <input
              className="input"
              type="date"
              value={form.registration_date}
              onChange={set("registration_date")}
            />
          </div>
        </div>
        <button className="btn btn-primary" disabled={busy} type="submit" style={{ marginTop: 14 }}>
          {busy ? "Registering…" : "Register vehicle"}
        </button>
        {msg && <div className="form-msg err">{msg}</div>}
      </form>
    </Panel>
  );
}
