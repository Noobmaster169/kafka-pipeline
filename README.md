# AWAS A3 — Real-Time Average-Speed Enforcement Platform

Assignment 3 extends the Assignment 2 speed-camera prototype into a scalable, reproducible,
real-time analytics platform. See **`A3_PROPOSAL.md`** for the architecture and its
research-backed justification.

> This README grows with each build phase. Currently documents Phase 1 (scaffold + seed).

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
│   └── backend/              # Phase 4
│       └── frontend/         # Phase 5 (React + Vite)
├── deployment/
│   ├── config/docker-compose.yml   # full stack: zookeeper + kafka + mongo + spark (kafka-net)
│   ├── Dockerfile                  # baked Spark image (fit3182/pyspark + A3 deps)
│   └── scripts/
│       ├── stack.sh                # single entry point: up/down/restart/seed/verify/pipeline
│       ├── seed.sh                 # convenience wrapper for seed_db.py
│       └── spark.sh                # backwards-compatible alias -> stack.sh
└── benchmarks/               # Phase 6 (performance harness)
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

```bash
cd src
python -m simulator.run                          # live, default mix (70/20/10), 0.5 trips/s
python -m simulator.run --rate 2 --total 200     # 2 trips/s, stop after 200 trips
python -m simulator.run --normal 0.5 --speeder 0.3 --sneaky 0.2   # custom behaviour mix
python -m simulator.run --fast --total 100000    # load test: emit as fast as possible
python -m simulator.run --source csv --scale 5   # replay the A2 camera_event_*.csv files
```

Camera config is re-read every `--refresh` seconds, so cameras an admin adds at runtime
start receiving traffic with no restart. Requires the broker up and `MONGO_HOST` reachable.

## Spark pipeline (Phase 3)

Reads the single `camera-events` topic, detects INSTANTANEOUS + AVERAGE violations (the
generalised self-join of proposal §4.2), de-duplicates per `(car_plate, violation_type)`,
and writes one document per violation to MongoDB while republishing each to the
`violations` Kafka topic for the live dashboard. Requires **Java + Spark** (use the
`fit3182/pyspark` image).

```bash
cd src
python -m pipeline.verify_detect     # OFFLINE batch test of the join — no broker needed
python -m pipeline.run --reset       # start the streaming query (needs broker + mongo)
```

`verify_detect` feeds one normal, one speeder, and one sneaky driver through the real
transforms and asserts the classification — the offline proof the self-join works.

## Configuration

Everything is env-overridable (defaults in `src/common/config.py`): `KAFKA_BOOTSTRAP_SERVERS`,
`KAFKA_TOPIC`, `KAFKA_TOPIC_PARTITIONS`, `MONGO_HOST`, `MONGO_PORT`, `MONGO_DB`,
`WATERMARK_DURATION`, `JOIN_WINDOW`, `DEDUP_WINDOW`, `CAMERA_SPACING_KM`.
