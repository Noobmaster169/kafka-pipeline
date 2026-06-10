import { useCallback, useEffect, useState } from "react";

import { api } from "./api.js";
import { violationKey } from "./format.js";
import { useLiveSocket } from "./useLiveSocket.js";

const CAP = 60; // most-recent N kept in the feed

// Seed a violation feed from REST, then keep it current from the `/ws/violations`
// live feed (prepending new rows, de-duplicated by their natural key). Optionally
// scoped to one lane. Returns the merged list plus the live connection status.
export function useViolationFeed({ laneId, restParams = {} } = {}) {
  const [feed, setFeed] = useState([]);

  useEffect(() => {
    const params = { ...restParams, limit: CAP };
    if (laneId != null) params.lane_id = laneId;
    api
      .listViolations(params)
      .then((r) => setFeed(r.items))
      .catch(() => setFeed([]));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [laneId]);

  const onMessage = useCallback(
    (v) => {
      if (laneId != null && v.lane_id !== laneId) return;
      setFeed((prev) => {
        const key = violationKey(v);
        if (prev.some((p) => violationKey(p) === key)) return prev;
        return [v, ...prev].slice(0, CAP);
      });
    },
    [laneId]
  );

  const status = useLiveSocket("/ws/violations", onMessage);
  return { feed, status };
}
