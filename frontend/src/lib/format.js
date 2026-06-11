// Small formatting helpers shared across the data-dense panels.

// Backend timestamps are UTC but lack the trailing "Z"; without it, JS
// parses them as local time, shifting everything by the UTC offset.
function parseUtc(ts) {
  if (ts instanceof Date) return ts;
  return new Date(/Z|[+-]\d{2}:?\d{2}$/.test(ts) ? ts : `${ts}Z`);
}

export function fmtSpeed(v) {
  return v == null ? "—" : `${Math.round(v)}`;
}

export function fmtKm(v) {
  return v == null ? "—" : `${Number(v).toFixed(1)} km`;
}

export function fmtTime(iso) {
  if (!iso) return "—";
  const d = parseUtc(iso);
  return d.toLocaleTimeString("en-GB", { hour12: false });
}

export function fmtDateTime(iso) {
  if (!iso) return "—";
  const d = parseUtc(iso);
  return d.toLocaleString("en-GB", { hour12: false });
}

// "12s ago" / "4m ago" — used by the live feed.
export function fmtAgo(iso) {
  if (!iso) return "—";
  const secs = Math.max(0, (Date.now() - parseUtc(iso).getTime()) / 1000);
  if (secs < 60) return `${Math.floor(secs)}s ago`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  return `${Math.floor(secs / 3600)}h ago`;
}

// The speed a violation row is "about": instantaneous reading, or average speed.
export function violationSpeed(v) {
  return v.violation_type === "AVERAGE" ? v.avg_speed : v.speed_reading;
}

// The natural key of a violation — matches the backend's unique index, so a row
// streamed live over Kafka and the same row fetched via REST collapse to one.
export function violationKey(v) {
  return `${v.car_plate}|${v.violation_type}|${v.timestamp_start}|${v.camera_id_start}`;
}