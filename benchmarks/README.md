# Performance harness

Two harnesses, both generating **real** evidence (no static numbers). See the root
[`README.md`](../README.md) §9 for the analytical model and how these measurements support the
scalability claim.

## 1. `join_compare.py` — join-strategy comparison (offline, no broker)

Runs identical synthetic workloads through both join strategies — **adjacent** (A3 refinement,
consecutive cameras only, O(k) rows/vehicle) vs **all-pairs** (A2 baseline, every camera pair,
O(k²) rows/vehicle) — sweeping cameras-per-lane `k`, and records join output rows (the
literature's "comparisons" cost unit) + join time. Both strategies flag identical violations
(asserted by `pipeline/verify_detect.py`); the gap is pure surplus join work.

```bash
deployment/scripts/stack.sh joincmp                 # default sweep k ∈ {3,5,10,15}, 200 vehicles
python -m benchmarks.join_compare --plot-only       # regenerate the plot from the CSV (no Spark)
```

Outputs `join_compare.csv` and `join_compare.png` (the figure embedded in [`README.md`](../README.md) §9, Experiment 1).

## 2. `run_benchmark.py` — live throughput / latency (needs the full stack)

Drives the simulator against the live stack and reads the resulting `violations` documents out of
MongoDB.

## What it measures

| Metric | How |
|---|---|
| **`pipeline_eps`** — the scaling metric | events ÷ drain time, where drain = (last violation's `detected_at`) − (run start). The pipeline's end-to-end processing rate. |
| **`produce_eps`** — context only | `events ÷ produce_seconds`: the SIMULATOR's speed. Saturates at the Python producer (~3–4k ev/s) regardless of partitions — never use it as a scaling result. |
| **End-to-end latency** (s) | per violation, `detected_at − timestamp_end` — both already stored in the document, so no extra instrumentation. Reported as p50 / p95 / max. Only meaningful in live (`--rate`) mode. |
| **Knobs in effect** | every row is tagged with `KAFKA_TOPIC_PARTITIONS`, `SPARK_SHUFFLE_PARTITIONS` and `JOIN_STRATEGY`. |

Every run is **warmup-gated**: a 20-trip batch is produced first and the harness waits for a
violation to land in Mongo before measuring, so a pipeline that isn't consuming yet aborts the
run loudly instead of recording a 0-violation row (`--no-warmup` skips this).

Plot the sweep with `python -m benchmarks.plot_results` → `results.png` (the [`README.md`](../README.md) §9, Experiment 2 figure).

Results are appended to `benchmarks/results.csv` (created on first run), so repeated runs across
knob settings accumulate into a sweep.

## Prerequisites

- The full stack is up (`deployment/scripts/stack.sh up`) and seeded (`stack.sh seed`).
- The Spark pipeline is running in another terminal (`stack.sh pipeline`).
- Run the harness where `kafka` / `mongo` resolve — i.e. **inside the Spark container**.

## Run a single measurement

```bash
deployment/scripts/stack.sh shell        # shell inside the spark container (/app/src)
cd /app && python -m benchmarks.run_benchmark --label baseline --total 400 --rate 3
```

Useful flags: `--fast` (throughput, latency flagged unreliable), `--rate N` (live mode, best for
latency), `--total N` (trips), `--settle S` (drain wait before reading Mongo), `--reset-violations`
(clean slate before the run), `--out path.csv`.

## Run the partition sweep

Kafka cannot repartition an existing topic, so each partition setting needs a fresh topic. Do one
setting per stack:

```bash
for P in 1 2 4 6; do
  KAFKA_TOPIC_PARTITIONS=$P deployment/scripts/stack.sh down
  KAFKA_TOPIC_PARTITIONS=$P deployment/scripts/stack.sh up
  KAFKA_TOPIC_PARTITIONS=$P deployment/scripts/stack.sh topics      # set the partitions
  # start the pipeline (another terminal):  KAFKA_TOPIC_PARTITIONS=$P stack.sh pipeline
  KAFKA_TOPIC_PARTITIONS=$P deployment/scripts/stack.sh seed
  # then, inside the container:
  #   python -m benchmarks.run_benchmark --label p$P --total 400 --fast --reset-violations
done
```

The shuffle-partition sweep is lighter — it needs only a pipeline restart, not a fresh topic:

```bash
SPARK_SHUFFLE_PARTITIONS=8 deployment/scripts/stack.sh pipeline
python -m benchmarks.run_benchmark --label shuffle8 --total 400 --rate 3
```

## Reading the output

`results.csv` columns: `ran_at, label, mode, trips, kafka_partitions, shuffle_partitions,
join_strategy, produce_seconds, events_estimate, produce_eps, drain_seconds, pipeline_eps,
violations_new, instant, average, latency_n, latency_p50_s, latency_p95_s, latency_max_s,
latency_reliable`.

Plot `pipeline_eps` against `kafka_partitions` for the scaling curve (`python -m
benchmarks.plot_results` does exactly this), and `latency_p50_s` against load (trips/rate, live
mode) for the latency curve. Per the spec (§5d), only numbers produced by an actual run go into
the report — this harness is how they are generated.
