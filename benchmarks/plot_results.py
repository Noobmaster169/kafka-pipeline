"""
Plot the partition-sweep results (REPORT.md §8.4 figure) from results.csv.

Plots the PIPELINE's end-to-end processing rate (`pipeline_eps` — events ÷ time from run
start to last violation written) against the Kafka partition count. The producer's own rate
(`produce_eps`) is deliberately not plotted: it saturates at the Python producer's speed
regardless of partitions and says nothing about pipeline scaling.

Uses the most recent row per partition count, so re-running a setting supersedes it.
No Spark needed — just matplotlib:

    python -m benchmarks.plot_results          # reads results.csv, writes results.png
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Plot pipeline throughput vs Kafka partition count.")
    p.add_argument("--csv", default=str(Path(__file__).resolve().parent / "results.csv"))
    p.add_argument("--png", default=str(Path(__file__).resolve().parent / "results.png"))
    return p.parse_args()


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"[plot] {csv_path} does not exist — run the benchmark first")
        return 1

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("[plot] matplotlib not installed")
        return 1

    # Most recent row per partition count (rows are appended chronologically).
    latest: dict[int, dict] = {}
    with csv_path.open() as f:
        for row in csv.DictReader(f):
            try:
                latest[int(row["kafka_partitions"])] = row
            except (KeyError, ValueError):
                continue
    parts = sorted(p for p, r in latest.items() if r.get("pipeline_eps") not in (None, "", "nan"))
    if not parts:
        print("[plot] no usable rows (pipeline_eps missing — re-run with the current harness)")
        return 1
    eps = [float(latest[p]["pipeline_eps"]) for p in parts]
    shuffle = latest[parts[0]].get("shuffle_partitions", "?")
    events = latest[parts[0]].get("events_estimate", "?")

    mean_eps = sum(eps) / len(eps)

    fig, ax = plt.subplots(figsize=(9, 5.5))
    ax.plot(parts, eps, "o-", color="#2980b9", linewidth=2,
            label="pipeline throughput — events/s from run start to last violation written")
    ax.axhline(mean_eps, color="#7f8c8d", linestyle="--", linewidth=1,
               label=f"mean ≈ {mean_eps:,.0f} ev/s (≈ {mean_eps * 3600 / 1e6:.0f}M events/hour)")
    for p, e in zip(parts, eps):
        ax.annotate(f"{e:,.0f}", (p, e), textcoords="offset points", xytext=(0, 10),
                    ha="center", fontsize=10)
    ax.set_xlabel("Kafka partitions of the camera-events topic (the A3 scalability knob)")
    ax.set_ylabel(f"Pipeline events/s ({events} events per run, fast mode)")
    ax.set_title("Single-host capacity is CPU-bound, not partition-bound — flat across partitions\n"
                 f"(same cores at every setting, shuffle fixed at {shuffle}; partitions enable "
                 "scale-out, not single-host speed-up)", fontsize=11)
    ax.set_xticks(parts)
    ax.set_ylim(bottom=0, top=max(eps) * 1.25)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="lower left", fontsize=9)
    fig.tight_layout()
    fig.savefig(args.png, dpi=150)
    print(f"[plot] written to {args.png}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
