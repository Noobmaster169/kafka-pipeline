// Thin REST client for the AWAS backend. In dev, calls go to `/api/*`, which Vite
// proxies to the uvicorn backend (see vite.config.js); override with VITE_API_BASE.

const API_BASE = import.meta.env.VITE_API_BASE ?? "/api";

async function request(path, options) {
  const res = await fetch(`${API_BASE}${path}`, options);
  if (!res.ok) {
    let detail = res.statusText;
    try {
      detail = (await res.json()).detail ?? detail;
    } catch {
      /* non-JSON error body */
    }
    const err = new Error(detail);
    err.status = res.status;
    throw err;
  }
  return res.json();
}

function qs(params) {
  const clean = Object.fromEntries(
    Object.entries(params || {}).filter(([, v]) => v !== undefined && v !== null && v !== "")
  );
  const s = new URLSearchParams(clean).toString();
  return s ? `?${s}` : "";
}

export const api = {
  // lanes
  listLanes: () => request("/lanes"),
  getLane: (id) => request(`/lanes/${id}`),

  // cameras
  listCameras: (laneId) => request(`/cameras${qs({ lane_id: laneId })}`),
  addCamera: (body) =>
    request("/cameras", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
  removeLastCamera: (laneId) =>
    request(`/cameras/last${qs({ lane_id: laneId })}`, { method: "DELETE" }),

  // cars
  listCars: (params) => request(`/cars${qs(params)}`),
  getCar: (plate) => request(`/cars/${encodeURIComponent(plate)}`),
  addCar: (body) =>
    request("/cars", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),

  // violations
  listViolations: (params) => request(`/violations${qs(params)}`),
  // returns a fully-qualified URL for a direct browser download
  exportCsvUrl: (params) => `${API_BASE}/violations/export.csv${qs(params)}`,
};
