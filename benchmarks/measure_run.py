"""Measure one pipeline run: detection latency + throughput.

Usage (stack up, a run completed): python benchmarks/measure_run.py --label "3-partitions"
"""
import argparse, csv, os
from datetime import datetime, timezone

from pymongo import MongoClient
from kafka import KafkaConsumer, TopicPartition
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

MONGO = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB = os.getenv("MONGO_DB", "awas")
BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "camera-events")
OUT = os.path.dirname(os.path.abspath(__file__))

def topic_event_count():
    c = KafkaConsumer(bootstrap_servers=BROKER, consumer_timeout_ms=4000)
    parts = c.partitions_for_topic(TOPIC) or set()
    tps = [TopicPartition(TOPIC, p) for p in parts]
    end = c.end_offsets(tps)
    c.close()
    return sum(end.values()), len(parts)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    args = ap.parse_args()

    db = MongoClient(MONGO)[DB]
    rows = list(db.violations.find({}, {"detected_at": 1, "timestamp_end": 1, "_id": 0}))
    if not rows:
        raise SystemExit("no violations in Mongo - run the pipeline + simulator first")

    lat = sorted((r["detected_at"] - r["timestamp_end"]).total_seconds()
                 for r in rows if r.get("detected_at") and r.get("timestamp_end"))
    lat = [s for s in lat if s >= 0]
    n = len(lat)
    p = (lambda q: lat[min(n - 1, int(q * n))]) if n else (lambda q: float("nan"))

    first = min(r["detected_at"] for r in rows)
    last = max(r["detected_at"] for r in rows)
    window = max(1e-9, (last - first).total_seconds())
    events, partitions = topic_event_count()

    summary = {
        "label": args.label,
        "partitions": partitions,
        "events_in_topic": events,
        "violations": n,
        "window_s": round(window, 1),
        "events_per_s": round(events / window, 1),
        "violations_per_s": round(n / window, 2),
        "latency_p50_s": round(p(0.50), 2),
        "latency_p95_s": round(p(0.95), 2),
        "latency_max_s": round(lat[-1], 2) if n else float("nan"),
        "measured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

    csv_path = os.path.join(OUT, "results.csv")
    new = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary))
        if new:
            w.writeheader()
        w.writerow(summary)

    png = None
    if n:
        png = os.path.join(OUT, f"latency_{args.label}.png")
        plt.figure(figsize=(7, 4))
    plt.hist(lat, bins=40, color="#dc2657")
    plt.xlabel("detection latency (s): detected_at - timestamp_end")
    plt.ylabel("violations")
    plt.title(f"Detection latency - {args.label} (p50={summary['latency_p50_s']}s, p95={summary['latency_p95_s']}s)")
    plt.tight_layout()
    png = os.path.join(OUT, f"latency_{args.label}.png")
    plt.savefig(png, dpi=150)

    print("\n".join(f"{k:>18}: {v}" for k, v in summary.items()))
    print(f"\nwrote {png} and appended to {csv_path}")

if __name__ == "__main__":
    main()
