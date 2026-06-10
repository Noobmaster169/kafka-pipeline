import { useEffect, useRef, useState } from "react";
import "./LaneSchematic.css";

// How long a car stays on the schematic after its last crossing, and how long a
// camera "pulses" after a car passes it (milliseconds).
const CAR_TTL = 6500;
const PULSE_MS = 800;

// Map a position (km) onto an x-percentage across the road, with padding either
// side of the camera span so cars visibly enter and exit.
function makeProjector(cameras) {
  const positions = cameras.map((c) => c.position_km);
  const lo = Math.min(...positions);
  const hi = Math.max(...positions);
  const span = hi - lo || 1;
  const domLo = lo - span * 0.18;
  const domHi = hi + span * 0.18;
  return (km) => {
    const pct = ((km - domLo) / (domHi - domLo)) * 100;
    return Math.max(2, Math.min(98, pct));
  };
}

export default function LaneSchematic({ cameras, eventQueueRef }) {
  // Live state, kept in refs so the animation tick mutates without re-render churn;
  // a `frame` counter forces the repaint at a steady cadence.
  const carsRef = useRef(new Map()); // plate -> { km, overLimit, lastSeen }
  const pulsesRef = useRef(new Map()); // camera_id -> timestamp
  const [, setFrame] = useState(0);

  const sorted = [...cameras].sort((a, b) => a.position_km - b.position_km);
  const project = makeProjector(sorted.length ? sorted : [{ position_km: 0 }]);

  useEffect(() => {
    const tick = setInterval(() => {
      const now = Date.now();
      const queue = eventQueueRef?.current;

      // Drain everything the socket buffered since the last tick.
      if (queue && queue.length) {
        for (const ev of queue.splice(0)) {
          carsRef.current.set(ev.car_plate, {
            km: ev.position_km,
            overLimit: ev.speed_reading > ev.speed_limit,
            speed: ev.speed_reading,
            lastSeen: now,
          });
          pulsesRef.current.set(ev.camera_id, now);
        }
      }

      // Expire stale cars and finished pulses.
      for (const [plate, car] of carsRef.current) {
        if (now - car.lastSeen > CAR_TTL) carsRef.current.delete(plate);
      }
      for (const [cam, ts] of pulsesRef.current) {
        if (now - ts > PULSE_MS) pulsesRef.current.delete(cam);
      }

      setFrame((f) => (f + 1) % 1_000_000);
    }, 120);
    return () => clearInterval(tick);
  }, [eventQueueRef]);

  const now = Date.now();
  const cars = [...carsRef.current.entries()];

  return (
    <div className="schematic">
      <div className="schematic-road">
        <div className="road-surface">
          <div className="road-dashes" />
        </div>

        {/* camera gantries */}
        {sorted.map((cam) => {
          const x = project(cam.position_km);
          const pulsing = pulsesRef.current.has(cam.camera_id);
          return (
            <div
              key={cam.camera_id}
              className={`gantry ${pulsing ? "gantry-pulse" : ""}`}
              style={{ left: `${x}%` }}
            >
              <div className="gantry-node">
                <span className="gantry-eye" />
              </div>
              <div className="gantry-line" />
              <div className="gantry-label mono">
                <span className="gantry-id">CAM {cam.camera_id}</span>
                <span className="gantry-limit">{cam.speed_limit}</span>
              </div>
            </div>
          );
        })}

        {/* live cars */}
        {cars.map(([plate, car]) => {
          const x = project(car.km);
          const fading = now - car.lastSeen > CAR_TTL - 1500;
          return (
            <div
              key={plate}
              className={`car ${car.overLimit ? "car-over" : "car-ok"} ${
                fading ? "car-fade" : ""
              }`}
              style={{ left: `${x}%` }}
              title={`${plate} · ${Math.round(car.speed)} km/h`}
            >
              <span className="car-dot" />
              <span className="car-plate mono">{plate}</span>
            </div>
          );
        })}
      </div>

      <div className="schematic-foot mono">
        <span className="faint">
          {sorted.length} cameras · {(sorted.at(-1)?.position_km ?? 0).toFixed(1)} km span
        </span>
        <span className="faint">
          {cars.length} vehicle{cars.length === 1 ? "" : "s"} in segment
        </span>
      </div>
    </div>
  );
}
