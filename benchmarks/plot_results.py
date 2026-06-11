"""
Plot throughput across the partition-sweep runs in results.csv.

Latency is deliberately NOT plotted here: the sweep uses fast-mode bursts whose
synthetic event timeline makes per-violation latency undefined (see README).
Latency evidence comes from the live-rate runs' histograms.
"""
import csv, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUT = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(OUT, "results.csv")) as f:
    runs = list(csv.DictReader(f))

labels = [r["label"] for r in runs]
parts = [r["partitions"] for r in runs]
tput = [float(r["events_per_s"]) for r in runs]

plt.figure(figsize=(7, 4.2))
bars = plt.bar([f"{p} partition{'s' if p != '1' else ''}" for p in parts], tput, color="#2563eb")
for bar, v in zip(bars, tput):
    plt.text(bar.get_x() + bar.get_width() / 2, v + 150, f"{v:,.0f}",
             ha="center", fontsize=10)
plt.ylabel("events processed / s")
plt.title("Sustained throughput vs Kafka partition count\n(identical 60k-event burst, single host, local[*])")
plt.tight_layout()
plt.savefig(os.path.join(OUT, "comparison.png"), dpi=150)
print("wrote benchmarks/comparison.png")