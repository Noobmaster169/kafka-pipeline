# AWAS A3 — Real-Time Average-Speed Enforcement Platform

Assignment 3 extends the Assignment 2 speed-camera prototype into a scalable, reproducible,
real-time analytics platform. See **`A3_PROPOSAL.md`** for the architecture and its
research-backed justification.

> This README grows with each build phase. It now documents Phases 1-6: scaffold, seed,
> simulator, Spark pipeline, the backend API, the live operations dashboard and the
> empirical evaluation in benchmarks/.

## Project structure

```
34328041_34423680_assignment03/
├── A3_PROPOSAL.md            # academic proposal (architecture + references)
├── requirements.txt          # Python deps (backend / simulator / pipeline / seeding)
├── src/
│   ├── common/               # shared config, event schema, Mongo + Kafka helpers
│   │   ├── config.py         #   all settings, env-overridable
│   │   ├── schema.py         #   the enriched camera-event payload
│   │   ├── mongo.py          #   Mongo client + indexes (lanes/cameras/cars/violations)
│   │   └── kafka_io.py       #   Kafka producer/consumer factories
│   ├── seed_db.py            # seed 3 lanes, >=3 cameras/lane, cars from vehicle.csv
│   ├── simulator/            # traffic generator (normal/speeder/sneaky)
│   │   ├── behaviors.py      #   pure trip-generation logic (unit-tested)
│   │   └── run.py            #   runner: Mongo config -> Kafka, live/fast/csv modes
│   ├── pipeline/             # Spark Structured Streaming job
│   │   ├── detect.py         #   read + self-join + dedup (the §4.2 transforms)
│   │   ├── sink.py           #   foreachBatch -> Mongo (idempotent) + Kafka violations
│   │   ├── run.py            #   Spark session + wiring + query
│   │   └── verify_detect.py  #   offline batch test of the join (no broker needed)
│   └── backend/             # FastAPI API behind the dashboard (Phase 4)
│       ├── db.py            #   all MongoDB reads/writes, returning JSON-safe dicts
│       ├── serialize.py     #   Mongo doc -> JSON (ObjectId/datetime)
│       ├── models.py        #   request bodies (camera / car creation)
│       ├── live.py          #   Kafka -> WebSocket hub (camera-events + violations feeds)
│       ├── routers/         #   lanes, cameras (append + remove last), cars, violations, ws
│       ├── main.py          #   app: CORS + routers + live-hub lifecycle
│       └── run.py           #   uvicorn entry point
├── frontend/                # React + Vite operations dashboard (Phase 5)
│   ├── public/models/       #   Kenney CC0 vehicle models for the 3D views
│   └── src/
│       ├── lib/             #   api client, live-socket hook, UTC-safe formatters
│       ├── components/      #   shell, lane schematic, 3D lane highway, 3D car viewer,
│       │                    #   latest capture, live feed, UI kit
│       └── pages/           #   overview, lane, cameras, vehicles, violations
├── deployment/
│   ├── config/docker-compose.yml   # full stack: zookeeper + kafka + mongo + spark (kafka-net)
│   ├── Dockerfile                  # baked Spark image (fit3182/pyspark + A3 deps)
│   └── scripts/
│       ├── stack.sh                # single entry point: up/down/restart/seed/verify/pipeline
│       ├── seed.sh                 # convenience wrapper for seed_db.py
│       └── spark.sh                # backwards-compatible alias -> stack.sh
└── benchmarks/               # Phase 6: measure_run.py, plot_results.py, results.csv,
                              #   comparison + latency charts, README (method + results)
```

## Quick start (Phase 1)

```bash
# 1. Bring up the whole stack (Zookeeper, Kafka, MongoDB, Spark) on kafka-net
deployment/scripts/stack.sh up
#   stack.sh down    # stop + remove everything    stack.sh ps   # status

# 2. Install Python deps (only needed for host-side seeding / simulator)
pip install -r requirements.txt

# 3. Seed reference data into MongoDB
deployment/scripts/stack.sh seed                 # all 10k cars
#   or:  deployment/scripts/stack.sh seed --cars-limit 500   # faster demo
```

After seeding you should have, in the `awas` database: 3 lanes, 9 cameras (3 per lane,
1 km apart), and the car pool. The `violations` collection is created with its indexes
and filled once the pipeline runs (Phase 3).

## Traffic simulator (Phase 2)

Generates synthetic traffic and streams enriched events to the single `camera-events`
topic (keyed by `car_plate`). Each trip is `normal`, `speeder`, or `sneaky` (brakes under
the limit at every camera but speeds between them — caught only by the average-speed join).

Run it **inside the stack** via `stack.sh sim` — this `docker exec`s into the Spark
container, where `kafka:9092` and `mongo` resolve (a host shell can't resolve the
`kafka` hostname the broker advertises, so it would connect but receive nothing). All
flags pass straight through:

```bash
deployment/scripts/stack.sh sim                                       # live, default mix (70/20/10), 0.5 trips/s
deployment/scripts/stack.sh sim --rate 2 --total 200                  # 2 trips/s, stop after 200 trips
deployment/scripts/stack.sh sim --normal 0.5 --speeder 0.3 --sneaky 0.2   # custom behaviour mix
deployment/scripts/stack.sh sim --fast --total 100000                 # load test: emit as fast as possible
deployment/scripts/stack.sh sim --source csv --scale 5                # replay the A2 camera_event_*.csv files
```

`stack.sh sim ...` is exactly `cd src && python -m simulator.run ...` run in the container.
You can run that module directly on the host **only** if you've made `kafka`/`mongo`
resolvable there (e.g. `/etc/hosts`) or overridden `KAFKA_BOOTSTRAP_SERVERS`/`MONGO_HOST`.

Camera config is re-read every `--refresh` seconds, so cameras an admin adds at runtime
start receiving traffic with no restart.

## Spark pipeline (Phase 3)

Reads the single `camera-events` topic, detects INSTANTANEOUS + AVERAGE violations (the
generalised self-join of proposal §4.2), de-duplicates per `(car_plate, violation_type)`,
and writes one document per violation to MongoDB while republishing each to the
`violations` Kafka topic for the live dashboard. Requires **Java + Spark** (use the
`fit3182/pyspark` image).

Run both **inside the Spark container** via `stack.sh` (it `docker exec`s in, where the
broker and Mongo resolve and Java + Spark are already installed):

```bash
deployment/scripts/stack.sh verify     # OFFLINE batch test of the join — no broker needed
deployment/scripts/stack.sh pipeline   # start the streaming query (runs with --reset; needs broker + mongo)
```

These are `cd src && python -m pipeline.verify_detect` and `... python -m pipeline.run --reset`
run in the container; run them on the host only with a local Spark install and resolvable
`kafka`/`mongo`. `verify_detect` feeds one normal, one speeder, and one sneaky driver through
the real transforms and asserts the classification — the offline proof the self-join works.

## Backend API (Phase 4)

A FastAPI service that backs the dashboard. REST endpoints read the reference and violation
data from MongoDB; two WebSockets fan out the live Kafka feeds. It boots even when the broker
is down (REST stays usable; the live feeds reconnect on their own).

```bash
cd src
python -m backend.run                     # serve on BACKEND_HOST:BACKEND_PORT (default 0.0.0.0:8000)
# host against the docker stack (Kafka/Mongo published on localhost):
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 MONGO_HOST=localhost python -m backend.run
```

Interactive API docs at `http://localhost:8000/docs`.

**REST**

| Method & path | Purpose |
|---|---|
| `GET /lanes` | All lanes with camera count + violation tallies (overview). |
| `GET /lanes/{id}` | One lane with its ordered cameras and summary. |
| `GET /cameras[?lane_id=]` | Cameras, all or one lane's, ordered along the road. |
| `POST /cameras` | Append a camera to a lane — `position_km`/`camera_id` assigned server-side (the runtime hot-add). Body: `{lane_id, speed_limit?}`. |
| `DELETE /cameras/last?lane_id=` | Remove the end of lane camera (the inverse of the auto-append). 409 if the lane would drop below 2 cameras. |
| `GET /cars[?plate=&skip=&limit=]` | Paginated car search by plate prefix. |
| `GET /cars/{plate}` | A car with its violation history. |
| `POST /cars` | Register a car (409 if the plate exists). |
| `GET /violations[?lane_id=&violation_type=&car_plate=&date=&skip=&limit=]` | Filtered, paginated violations (newest first) with a total. |
| `GET /violations/{id}` | One violation by id. |
| `GET /violations/export.csv[?...filters]` | Streamed CSV download. |
| `GET /health` | Liveness check. |

**WebSocket** (fed by Kafka, drive the live dashboard — proposal §4.1/§4.4)

- `GET /ws/lane/{lane_id}` — every camera crossing on that lane (drives the lane animation).
- `GET /ws/violations` — each newly detected violation (drives the live log).

> The WebSocket feeds need a reachable broker. Because the stack's Kafka advertises itself as
> `kafka:9092`, run the backend **inside `kafka-net`** (or make `kafka` resolvable on the host)
> for the live feeds to deliver; REST works from anywhere the Mongo port is reachable.

## Live dashboard (Phase 5)

A React + Vite single-page operations console that consumes the backend: a network
overview, a live lane dashboard, camera management with runtime append and remove,
a vehicle registry with per-vehicle violation history and a filterable violation
tracker with CSV export. Every view renders fully from REST and layers the live
Kafka feeds on top, then re-polls summaries every 5 seconds so every counter on
screen stays current without a reload.

Highlights:

- Live lane schematic: vehicles stream past the camera gantries as the
  camera-events feed arrives and over-limit crossings glow rose
- Lane Highway 3D: the same feed rendered as a 3D road. Each crossing spawns a
  low poly vehicle that drives at a speed proportional to its actual reading,
  over-limit crossings fire a white flash on that gantry and gantries are placed
  from the lane's real camera list so a hot-added camera appears live
- Latest Capture (overview page): when a violation arrives the offender's car is
  shown as a rotating 3D model matched to its registered vehicle_type with its
  plate rendered on both bumpers, plus type, speed and limit
- All timestamps are parsed as UTC (parseUtc in lib/format.js) so relative times
  like "5s ago" are correct in any local timezone

The 3D views use three.js via @react-three/fiber and @react-three/drei. Vehicle
models are the Kenney Car Kit (kenney.nl, CC0) in frontend/public/models/.

```bash
cd frontend
npm install
npm run dev        # Vite dev server on http://localhost:5173
```

The dev server proxies `/api/*` and `/ws/*` to the backend (default `http://localhost:8000`;
override with `VITE_BACKEND`), so only the backend needs to be running alongside it — no CORS
setup required. `npm run build` emits a static bundle to `dist/`.

Typical local bring-up (four terminals): the docker stack (Kafka/Mongo/Spark), the pipeline
(`stack.sh pipeline`), the simulator (`stack.sh sim --rate 3`), and the dashboard (`npm run
dev`). The backend (`python -m backend.run`) runs on the host — REST works anywhere Mongo is
reachable; its live WebSocket feeds need `kafka` resolvable (see the Backend note above).

## Configuration

Everything is env-overridable (defaults in `src/common/config.py`): `KAFKA_BOOTSTRAP_SERVERS`,
`KAFKA_TOPIC`, `KAFKA_TOPIC_PARTITIONS`, `MONGO_HOST`, `MONGO_PORT`, `MONGO_DB`,
`WATERMARK_DURATION`, `JOIN_WINDOW`, `DEDUP_WINDOW`, `CAMERA_SPACING_KM`, `BACKEND_HOST`,
`BACKEND_PORT`, `CORS_ORIGINS`.


## Empirical evaluation (Phase 6)

Everything in benchmarks/ comes from real executed runs of the pipeline.
The numbers in results.csv and the charts were produced by measure_run.py
and plot_results.py against the live system. Nothing is hand made or
estimated.

### What we measured and how

#### Throughput (the partition sweep)

The proposal claims that parallelism in our design is a deployment knob
rather than something fixed by the number of cameras. To test that we ran
the exact same workload against the topic configured with 1, 3 and 6
partitions.

For each partition count N:

1. Delete and recreate the camera-events topic with N partitions
2. Start the pipeline fresh (it runs with --reset so the violations
   collection starts empty)
3. Fire a fixed burst: sim --fast --total 20000 which is 20000 trips ->
   60000 camera events delivered in about 6 seconds at roughly 10000
   trips per second. The producer is much faster than the pipeline so the
   pipeline is the bottleneck which is exactly what a capacity test needs
4. Wait until the stream fully drains then run measure_run.py

Throughput is computed as total events in the topic (taken from the real
Kafka end offsets) divided by the detection window which is the time span
between the first and last violation written to MongoDB.

#### Latency

End to end detection latency is detected_at - timestamp_end for each
violation. In plain words: the time from the moment a vehicle crosses the
second camera to the moment its violation document exists in MongoDB.

Latency is measured under live paced load (sim --rate 3). The fast burst
runs cannot be used for latency because the simulator compresses a long
synthetic timeline into a few wall clock seconds so the event timestamps
do not line up with real time. measure_run.py records those latency
columns as nan on purpose. The latency evidence is the two live rate
histograms in benchmarks/:

- latency_baseline-rate3.png
- latency_3-partitions-heavy.png

### Results

Throughput across the partition sweep with an identical 60000 event burst
per configuration on a single host:

    +------------+----------------+------------------------+
    | partitions | workload       | throughput (events/s)  |
    +------------+----------------+------------------------+
    | 1          | 60000 burst    | 8403                   |
    | 3          | 60000 burst    | 11806                  |
    | 6          | 60000 burst    | 8186                   |
    +------------+----------------+------------------------+

Latency under live paced load: p50 about 0.77s and p95 about 1.2s with a
maximum around 1.8s. The median time from a camera crossing to a stored
violation is under one second.

See benchmarks/comparison.png for the throughput chart.

### What the results mean

Going from 1 to 3 partitions raised sustained throughput by about 40
percent. That is the central claim of the proposal (section 4.1) shown
working: with one high cardinality keyed topic the level of parallelism
is a configuration choice. In the A2 design this number was locked at 3
by the topology itself (section 3 limitation L1) and no configuration
could change it.

At 6 partitions throughput drops back to roughly the 1 partition level.
That is not the design failing. It is the single machine running out of
parallel capacity. The evaluation runs Spark as local[*] with 4 shuffle
partitions so all work shares one set of CPU cores. Six input partitions
oversubscribe those cores and the extra scheduling and coordination cost
outweighs the extra parallelism. On a real cluster the additional
partitions would map onto additional executor cores instead. The design
removes the structural ceiling and the hardware decides where the
practical ceiling sits which is the position the proposal takes in
section 6.

### Honest caveats

- Single host only. Multi node behaviour is argued by extrapolation as
  the proposal allows
- One run per configuration so the numbers are indicative rather than
  averaged over repeats
- Latency from fast mode runs is undefined by construction as explained
  above
- Join state size was not directly instrumented. The bounded-state
  property is enforced by construction (the time-range join predicate
  plus the watermark, proposal sections 4.2.4 and 4.5) and is verified
  indirectly: the pipeline processed repeated 60000 event bursts and long
  live runs with stable memory and no state growth between runs
- The pipeline detects fewer violations than the simulator predicts
  because deduplication on (car_plate, violation_type) collapses repeat
  offences inside the dedup window. That is intended behaviour (D4)

### Reproducing the sweep

For each N in 1, 3, 6:

    docker exec kafka /opt/kafka_2.13-2.8.2/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic camera-events
    docker exec kafka /opt/kafka_2.13-2.8.2/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic camera-events --partitions N --replication-factor 1

    deployment/scripts/stack.sh pipeline                  (terminal 1)
    deployment/scripts/stack.sh sim --fast --total 20000  (terminal 2, run once)

    wait for the pipeline to report idle twice then:

    python benchmarks/measure_run.py --label "pN-fast20k"
    python benchmarks/plot_results.py

On Git Bash prefix the docker exec lines with MSYS_NO_PATHCONV=1 so the
/opt path is not rewritten.