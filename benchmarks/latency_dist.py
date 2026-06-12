"""Plot the latency DISTRIBUTION of violations already in MongoDB.

`run_benchmark.py` only records the *summary* (p50/p95/max) of a run. To see the actual
shape — how the per-violation latencies spread out — we need every individual value, and
those live in the `violations` collection: latency = `detected_at - timestamp_end`.

This tool reads them straight out of Mongo (no re-run needed), writes:
  * benchmarks/latency_<label>.csv  — the raw per-violation latencies (so the figure is
                                       reproducible and the numbers can be re-plotted/checked)
  * benchmarks/latency_<label>.png  — a histogram with p50 / p95 marked

and prints a plain summary. It does NOT touch results.csv.

Run it where `mongo` resolves (inside the Spark container), AFTER a run, BEFORE the next
`--reset-violations` wipes the collection:

    cd /app && python benchmarks/latency_dist.py --label live-rate3
"""
from __future__ import annotations

import argparse
import csv
import os
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pymongo import MongoClient

OUT = os.path.dirname(os.path.abspath(__file__))


def _aware(dt: datetime) -> datetime:
    """Coerce a (possibly naive) Mongo datetime to UTC-aware for safe subtraction."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _pct(values: list[float], q: float) -> float:
    """Percentile q (0..1) of a SORTED list, nearest-rank."""
    if not values:
        return float("nan")
    return values[min(len(values) - 1, int(round(q * (len(values) - 1))))]


def main() -> None:
    ap = argparse.ArgumentParser(description="Latency distribution of violations in Mongo.")
    ap.add_argument("--label", required=True, help="Tag for the output files, e.g. live-rate3.")
    ap.add_argument("--mongo", default=os.getenv("MONGO_URI", "mongodb://mongo:27017"))
    ap.add_argument("--db", default=os.getenv("MONGO_DB", "awas"))
    ap.add_argument("--cap", type=float, default=660.0,
                    help="Drop latencies above this many seconds as implausible/late state.")
    ap.add_argument("--bins", type=int, default=30)
    args = ap.parse_args()

    coll = MongoClient(args.mongo)[args.db]["violations"]
    rows = list(coll.find({}, {"_id": 0, "detected_at": 1, "timestamp_end": 1, "violation_type": 1}))
    if not rows:
        raise SystemExit("no violations in Mongo — run the pipeline + a benchmark first.")

    lat = []
    for r in rows:
        det, end = r.get("detected_at"), r.get("timestamp_end")
        if isinstance(det, datetime) and isinstance(end, datetime):
            s = (_aware(det) - _aware(end)).total_seconds()
            if 0.0 <= s <= args.cap:
                lat.append(s)
    lat.sort()
    n = len(lat)
    if not n:
        raise SystemExit(f"found {len(rows)} violations but none had a plausible latency (<= {args.cap}s).")

    p50, p95, p99 = _pct(lat, 0.50), _pct(lat, 0.95), _pct(lat, 0.99)
    lo, hi, mean = lat[0], lat[-1], sum(lat) / n

    # Raw values — so the distribution is reproducible, not just a picture.
    csv_path = os.path.join(OUT, f"latency_{args.label}.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["latency_s"])
        w.writerows([[round(s, 4)] for s in lat])

    # Histogram with the percentiles marked.
    png_path = os.path.join(OUT, f"latency_{args.label}.png")
    plt.figure(figsize=(8, 4.5))
    plt.hist(lat, bins=args.bins, color="#dc2657", edgecolor="white", linewidth=0.5)
    for x, c, txt in ((p50, "#1f2937", f"p50 {p50:.2f}s"), (p95, "#2563eb", f"p95 {p95:.2f}s")):
        plt.axvline(x, color=c, linestyle="--", linewidth=1.4)
        plt.text(x, plt.ylim()[1] * 0.92, " " + txt, color=c, fontsize=9, va="top")
    plt.xlabel("end-to-end latency (s):  detected_at − timestamp_end")
    plt.ylabel("number of violations")
    plt.title(f"AWAS detection latency — {args.label}  (n={n})")
    plt.tight_layout()
    plt.savefig(png_path, dpi=150)

    print(f"  label            : {args.label}")
    print(f"  violations (n)   : {n}")
    print(f"  min              : {lo:.3f} s")
    print(f"  p50 (median)     : {p50:.3f} s")
    print(f"  mean             : {mean:.3f} s")
    print(f"  p95              : {p95:.3f} s")
    print(f"  p99              : {p99:.3f} s")
    print(f"  max              : {hi:.3f} s")
    print(f"\n  wrote {png_path}")
    print(f"  wrote {csv_path}  (raw values — paste/share these to verify or re-plot)")


if __name__ == "__main__":
    main()
