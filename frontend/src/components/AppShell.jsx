import { useEffect, useState } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";

import { api } from "../lib/api.js";
import {
  IconOverview,
  IconLane,
  IconCamera,
  IconCar,
  IconViolations,
} from "./icons.jsx";
import "./AppShell.css";

function Clock() {
  const [now, setNow] = useState(new Date());
  useEffect(() => {
    const t = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(t);
  }, []);
  return (
    <span className="clock mono">
      {now.toLocaleTimeString("en-GB", { hour12: false })}
      <span className="clock-z"> UTC{-now.getTimezoneOffset() / 60 >= 0 ? "+" : ""}
        {-now.getTimezoneOffset() / 60}
      </span>
    </span>
  );
}

export default function AppShell() {
  const [lanes, setLanes] = useState([]);
  const location = useLocation();

  // Lane nav reflects live lane membership; refresh on navigation so a newly
  // seeded/added lane shows up without a reload.
  useEffect(() => {
    api.listLanes().then(setLanes).catch(() => setLanes([]));
  }, [location.pathname]);

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">
            <span className="brand-pulse" />
          </div>
          <div className="brand-text">
            <div className="brand-name">AWAS</div>
            <div className="brand-sub mono">ENFORCEMENT OPS</div>
          </div>
        </div>

        <nav className="nav">
          <NavLink to="/" end className="nav-item">
            <IconOverview />
            <span>Overview</span>
          </NavLink>

          <div className="nav-group-label">Lanes</div>
          {lanes.map((lane) => (
            <NavLink key={lane.lane_id} to={`/lanes/${lane.lane_id}`} className="nav-item">
              <IconLane />
              <span className="nav-lane-name">{lane.name}</span>
              {lane.violations?.total > 0 && (
                <span className="nav-count mono">{lane.violations.total}</span>
              )}
            </NavLink>
          ))}

          <div className="nav-group-label">Manage</div>
          <NavLink to="/cameras" className="nav-item">
            <IconCamera />
            <span>Cameras</span>
          </NavLink>
          <NavLink to="/cars" className="nav-item">
            <IconCar />
            <span>Vehicles</span>
          </NavLink>
          <NavLink to="/violations" className="nav-item">
            <IconViolations />
            <span>Violations</span>
          </NavLink>
        </nav>

        <div className="sidebar-foot mono">
          <div>FIT3182 · A3</div>
          <div className="faint">Avg-speed enforcement</div>
        </div>
      </aside>

      <div className="main">
        <header className="topbar">
          <div className="topbar-route mono">
            <span className="faint">awas</span>
            <span className="topbar-sep">/</span>
            {location.pathname === "/" ? "overview" : location.pathname.slice(1)}
          </div>
          <div className="topbar-right">
            <Clock />
          </div>
        </header>
        <main className="content">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
