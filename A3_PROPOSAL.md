# Scaling an Average-Speed Enforcement Pipeline: A Partitioned-Key, Generalised Stream-Join Architecture

**FIT3182 Big Data Management and Processing — Assignment 3 Proposal**
Team 34328041 / 34423680

---

## Abstract

Assignment 2 delivered a functional average-speed enforcement system in which roadside camera
events are ingested through Apache Kafka, correlated by Apache Spark Structured Streaming, and
persisted to MongoDB. While correct on the supplied dataset, the prototype's topology embeds a
fixed parallelism ceiling, a quadratic growth in hand-written join operators, and a
non-idempotent persistence model for violations, rendering it unsuitable for the
national-scale, continuously-running deployment the problem domain implies. This proposal
specifies a re-architected streaming pipeline built around two research-supported principles:
(i) a **single Kafka topic partitioned on a high-cardinality key** (`car_plate`) to decouple
parallelism from the number of cameras, and (ii) a **single generalised stream–stream
self-join** that subsumes all (consecutive) camera pairs and scales to an arbitrary number of
cameras and lanes without code change. The redesign is embedded in a reproducible, containerised admin
platform comprising a synthetic traffic simulator, the streaming pipeline, and a real-time
operations dashboard. We further specify an empirical evaluation of throughput, latency, and
operator-state growth that demonstrates the removal of the prototype's scaling ceiling.

---

## 1. Introduction and Problem Statement

Average-speed enforcement systems detect speeding by timing a vehicle between two cameras a
known distance apart, defeating the common evasion of braking only at the camera location. The
core computational task is a **temporal correlation over an unbounded event stream**: for each
vehicle, pairs of detections at distinct cameras must be matched within a bounded time window,
the mean speed over the intervening segment computed, and the result compared against the
segment's legal limit.

The operational context is demanding. A national camera network produces a high-volume,
24/7, never-ending event stream; the system must tolerate component failure without data loss,
absorb load spikes, and surface violations in near-real-time. These requirements — unbounded
input, fault tolerance, horizontal scalability, and stateful temporal correlation — are the
established motivation for a distributed log (Kafka) coupled to a stateful stream processor
(Spark Structured Streaming) [1], [4]. We emphasise at the outset that this machinery is
justified by the **shape** of the problem (an unbounded, high-volume, fault-intolerant stream
requiring windowed joins), not by the size of the assignment dataset; the small dataset is a
stand-in for the production workload.

This proposal critically evaluates the Assignment 2 (hereafter *A2*) architecture, identifies
the structural barriers to scale, and specifies a re-architected solution with research-based
justification, in line with Assignment 3's emphasis on stream-join design, scalability, and
research-supported technical decisions.

---

## 2. Background

### 2.1 Apache Kafka — the partitioned commit log

Kafka is a distributed, append-only, durable commit log designed for high-throughput log
processing [1]. Producers publish records to **topics**; each topic is divided into
**partitions**, which are the unit of parallelism, ordering, and storage. A record's
**partition** is determined by its **key** via `hash(key) mod #partitions`, guaranteeing that
records sharing a key are delivered to the same partition in publication order. Consumers track
a per-partition **offset**, enabling exactly-once-style resumption after failure. Two
properties are central to this proposal: (i) **the number of partitions bounds consumer-side
parallelism** — a partition is consumed by at most one consumer in a group — and (ii) **key
cardinality governs load balance** across partitions.

### 2.2 Apache Spark Structured Streaming

Spark provides data-parallel computation by partitioning data and executing tasks across
executor cores [2]. Structured Streaming models an unbounded stream as an incrementally
executed series of **micro-batches**, exposing a declarative DataFrame API while the engine
manages incremental execution, state, and fault tolerance via checkpointing [3], [4]. The
degree of parallelism for a stateful operator is set by the number of post-shuffle partitions;
the Kafka source contributes one Spark input partition per topic-partition, directly tying
ingest parallelism to Kafka's partition count.

### 2.3 Stream–stream joins, event time, and watermarks

Correlating two unbounded streams requires the engine to **buffer** records as operator
**state** until a matching record arrives or can no longer arrive. To bound this state, Spark
combines a **time-range join predicate** with **watermarks** — a heuristic, monotonically
advancing bound on event-time completeness that declares when sufficiently old records may be
dropped and their state evicted [4], [5]. The watermark/event-time model formalised by the
Dataflow Model [5] is what makes windowed correlation feasible on an infinite stream with
**bounded memory**: operator state is approximately `arrival_rate × (window + watermark
delay)`. Two parallel stream-join papers frame the design space our join sits in. The
handshake join [6] distributes the windowed join's tuple-pair comparison space across cores
*because* arbitrary θ-predicates cannot be key-partitioned — implying that a predicate led by
a high-cardinality equality (ours: `car_plate`) **can** be, with no such machinery needed.
ScaleJoin [7] shows that key-partitioned joins load-balance only as well as their key
distribution (a skewed key hot-spots one thread), which is the criterion by which we select
the partition key (§4.1) and the skew failure mode our design must consider (§6).

---

## 3. Limitations of the Assignment 2 Architecture

A2 ingests three camera streams via **three distinct topics** (`camera-events-A/B/C`), read by
three Spark readers, and detects average-speed violations through **six hand-written pairwise
joins** over all ordered camera pairs. Analysis of the implementation
(`src/spark_pipeline.py`, `src/run_all_producers.py`) reveals four structural limitations:

**L1 — Parallelism is fixed at the number of cameras.** Because each camera maps to its own
topic, and a topic-partition is the unit of consumer parallelism [1], ingest parallelism is
capped at three regardless of available compute. A single high-traffic camera constitutes one
indivisible partition and cannot be parallelised at all. The ceiling is structural, not a
tunable.

**L2 — Join topology grows quadratically and requires recompilation to scale.** Monitoring all
camera pairs with explicit per-pair joins yields `O(n²)` operators in the number of cameras `n`
(six for three cameras; ninety for ten). Adding a camera requires authoring new topics,
readers, and join operators — the system cannot grow without being rewritten.

**L3 — Redundant and wasted operator state.** A2 instantiates joins for both directions of
every pair; for unidirectional traffic, roughly half maintain windowed state that can never
produce output, inflating memory and shuffle cost.

**L4 — A non-idempotent persistence model.** A2 stores violations as **one document per
`(car_plate, date)`** with a `$push` array (`spark_pipeline.py`), so a day's distinct
violations are conflated into a single array and reprocessing a micro-batch re-pushes the same
events. This both complicates per-violation querying, filtering and export, and makes the write
path non-idempotent — a vehicle's record cannot be reconciled cleanly after a restart. (The
de-duplication itself, keyed on `car_plate` over a window, is the *correct* enforcement grain —
one flag per car per window — and is retained in A3; see §4.4.)

A secondary observation is that A2's feeder pre-sorts events into global time order, so the
watermark and late-data machinery — the crux of correct stream-join semantics [5] — is never
exercised under realistic out-of-order conditions.

---

## 4. Proposed Architecture

We retain the Kafka → Spark Structured Streaming → MongoDB substrate but re-architect the
streaming core around four decisions, each addressing a limitation above.

### 4.1 D1 — A single topic partitioned on `car_plate`

The three per-camera topics are replaced by one topic, `camera-events`, with a configurable
partition count, keyed on `car_plate`. The justification is twofold and grounded in Kafka's
partitioning semantics [1]:

- **Parallelism decoupled from camera count.** Ingest and downstream parallelism are now
  governed by the partition count, a deployment parameter, rather than fixed at three (resolves
  **L1**).
- **Balanced load via high key cardinality.** `car_plate` has cardinality in the thousands, so
  records distribute evenly across partitions; co-location of a vehicle's events on one
  partition additionally preserves per-vehicle event ordering, which the join relies upon.
  Keying on `camera_id` (cardinality three) would reproduce the A2 ceiling and induce hot
  partitions — exactly the key-skew failure mode for which ScaleJoin [7] abandons key-based
  placement in favour of key-free round-robin distribution. The high-cardinality key keeps the
  partitioned join inside the safe regime that analysis defines.

### 4.2 D2 — A single generalised stream–stream self-join

This is the central change of the architecture and warrants a detailed treatment. A2 detected
average-speed violations with six hand-written joins, one per ordered camera pair. We replace
all of them with **one** self-join of the single enriched stream with itself. The remainder of
this section specifies the join, justifies each predicate clause, and works a concrete numeric
example end-to-end.

#### 4.2.1 The mechanism: joining one stream to itself

After §4.1, every camera's detections arrive on the same stream, `events`. A *self-join* uses
that same stream under two aliases — `a` (a candidate **start** detection) and `b` (a candidate
**end** detection) — and asks the engine to find, for every vehicle, pairs of detections at two
different cameras that are close enough in time to be the same journey. Spark maintains two
keyed state stores (one per side) and matches arriving records against the opposite side's
buffered state [4]. Listing 1 gives the core code.

```python
# Listing 1 — the generalised average-speed self-join (PySpark)

# (1) ONE enriched input stream, watermarked on event time so join state can be evicted.
events = (
    spark.readStream.format("kafka").option("subscribe", "camera-events").load()
        .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")                                   # car_plate, lane_id, camera_id, camera_index,
                                                         # position_km, speed_limit, speed_reading, timestamp
        .withColumn("event_time", to_timestamp("timestamp"))
        .withWatermark("event_time", "10 minutes")       # late bound; enables state cleanup
)

# (2) The SAME stream under two aliases: a = "start" detection, b = "end" detection.
a = events.alias("a")
b = events.alias("b")

# (3) The single self-join that replaces all six A2 pairwise joins.
paired = a.join(
    b,
    on=expr("""
        a.car_plate  =  b.car_plate                       AND   -- (i)   same vehicle
        a.lane_id    =  b.lane_id                         AND   -- (ii)  same lane
        abs(b.camera_index - a.camera_index) = 1          AND   -- (iii) ADJACENT cameras only
        b.event_time >  a.event_time                      AND   -- (iv)  b is the later crossing
        b.event_time <= a.event_time + interval 10 minutes      -- (v)   within the journey window
    """),
    how="inner",                                          # emit only when a partner exists
)

# (4) Average speed computed from the TWO joined events alone — no camera config in Spark (D3).
segments = paired.select(
    col("a.car_plate").alias("car_plate"),
    col("a.lane_id").alias("lane_id"),
    col("a.camera_id").alias("camera_id_start"),
    col("b.camera_id").alias("camera_id_end"),
    col("a.event_time").alias("timestamp_start"),
    col("b.event_time").alias("timestamp_end"),
    col("b.speed_limit").alias("speed_limit"),            # end-camera limit governs the segment
    abs(col("b.position_km") - col("a.position_km")).alias("distance_km"),
    (unix_timestamp("b.event_time") - unix_timestamp("a.event_time")).alias("dt_seconds"),
).withColumn(
    "avg_speed", col("distance_km") * 3600.0 / col("dt_seconds")   # km / hours = km/h
)

# (5) A segment is a violation iff its average speed exceeds the end-camera limit.
violations_avg = segments.where(col("avg_speed") > col("speed_limit"))
```

#### 4.2.2 Why each predicate clause is present

| Clause | Predicate | Purpose |
|---|---|---|
| (i) | `a.car_plate = b.car_plate` | The join key. Only detections of the **same vehicle** can form a journey; co-located on one Kafka partition by §4.1, so matching is local. |
| (ii) | `a.lane_id = b.lane_id` | A segment exists only **within one lane**; cross-lane pairs are physically meaningless and are excluded. |
| (iii) | `abs(b.camera_index − a.camera_index) = 1` | The two endpoints must be **adjacent cameras** on the lane. `camera_index` is each camera's ordinal along the road (source-enriched, D3). Pairing only neighbours computes the average over **consecutive segments** (X-1 → X) and never across a skipped camera (X-2), reducing per-vehicle output from `O(k²)` to `O(k)`. The `|Δindex| = 1` form (rather than `index+1`) keeps **both travel directions**; clause (iv) then fixes which endpoint is the start. |
| (iv) | `b.event_time > a.event_time` | Forces `b` to be the **later** crossing, so `a` is unambiguously the segment **start**. Combined with (iii) this makes each adjacent camera pair produce **exactly one** row, not two mirror-image rows — there is no `(b before a)` duplicate because it would violate this clause. Physical direction is irrelevant: distance is `|position_b − position_a|`, valid either way. |
| (v) | `b.event_time <= a.event_time + 10 min` | The **journey window**: two detections more than ten minutes apart are not one trip. This is also the bound Spark uses, together with the watermark, to **evict join state** (§4.2.4). |

A single operator with these five clauses therefore subsumes **every adjacent camera pair, in
both travel directions, for any number of cameras and lanes** — resolving **L2** (no per-pair
code) and **L3** (no wasted reverse-direction joins). The predicate is deliberately *led by a
high-cardinality equality* (clause i): the handshake join [6] exists because general
θ-predicates cannot be key-partitioned, so keeping our join equi-keyed is what licenses the
simple key-partitioned execution — the literature's applicability boundary used as a design
rule.

Restricting clause (iii) to *adjacent* cameras (`|Δindex| = 1`) rather than any distinct pair
(`camera_id <> camera_id`) is a deliberate refinement: average-speed enforcement is intrinsically
a **consecutive-segment** computation, so a vehicle that passes `k` cameras need only be checked
on its `k−1` adjacent segments, not all `O(k²)` pairs. A non-adjacent pair such as `(cam1, cam3)`
adds no enforcement power — if a driver speeds across `(1,3)` with a constant between-camera speed,
the adjacent segments `(1,2)` and `(2,3)` already exceed the limit and flag. The only case it
forgoes is a **missed** camera reading (`cam1` then `cam3` with no `cam2` event), which the
all-pairs form would still bridge; the ideal remedy — holding each vehicle's *last* crossing in
operator state via `applyInPandasWithState` for `O(active vehicles)` rather than `O(events ×
window)` memory — requires Spark ≥ 3.4 and is noted as future work (§6) under the unit's Spark 3.3
runtime.

#### 4.2.3 Worked example — catching a "sneaky" driver

Consider vehicle `SNK 1` on lane 1, whose three cameras sit at positions 1.0, 2.0 and 3.0 km,
each with a 90 km/h limit. The driver brakes hard *at* each camera but accelerates between them:

| camera | position_km | event_time | speed_reading |
|---|---|---|---|
| 1 | 1.0 | 08:00:00 | 88 km/h |
| 2 | 2.0 | 08:00:30 | 85 km/h |
| 3 | 3.0 | 08:01:00 | 89 km/h |

**Instantaneous detector (stateless filter `speed_reading > speed_limit`):** 88, 85, 89 are all
≤ 90. **No instantaneous violation** — the driver appears legal at every individual camera. This
is exactly the evasion average-speed enforcement exists to defeat.

**The self-join** emits one row per qualifying `(a, b)` pair. With three crossings of the same
plate on the same lane within the window, clauses (i)–(v) — pairing only **adjacent** cameras —
yield two rows:

| `a.cam` | `b.cam` | `dt_seconds` | `distance_km` | `avg_speed = dist·3600/dt` |
|---|---|---|---|---|
| 1 | 2 | 30 | 1.0 | **120.0 km/h** |
| 2 | 3 | 30 | 1.0 | **120.0 km/h** |

(The non-adjacent pair `(cam1, cam3)` is **not** emitted — clause (iii) requires
`|Δcamera_index| = 1` — and the reverse pairs such as `a.cam=2, b.cam=1` never appear because
clause (iv) requires the `b` crossing to be later in time.) Both rows' `avg_speed` (120) exceed
the 90 limit, so clause (5) flags both as AVERAGE candidates. Downstream de-duplication on
`car_plate` (one flag per car per window, D4) then collapses them to **one** recorded AVERAGE
violation for `SNK 1`. The driver is caught despite never exceeding the limit at any camera — and
with `k−1 = 2` segment checks rather than the `O(k²) = 3` of an all-pairs join.

**Runtime extensibility, concretely.** If an administrator adds a fourth camera D at 4.0 km
(§4.3), `SNK 1`'s later trips simply produce a fourth crossing (`camera_index = 3`); the *same*
join in Listing 1 then additionally yields the one **adjacent** pair `(3, 4)` with **no code
change and no restart**. By contrast, A2 would require new hand-written joins.

#### 4.2.4 How the join's memory stays bounded

State growth is the failure mode of a naïve stream join. Two cooperating mechanisms bound it
here. Clause (v) tells Spark that an `a` detection can only ever match `b` detections within ten
minutes after it. The **watermark** (Listing 1, step 1) advances as `max(event_time seen) − 10
min` and tells Spark when it has seen enough later data that an `a` row can never gain a new
partner. For `SNK 1`'s 08:00:00 crossing, once the watermark passes 08:10:00 the row is evicted
from the state store and its memory reclaimed [4], [5]. Aggregate join state is therefore
bounded at approximately `arrival_rate × (window + watermark delay)` regardless of how long the
stream runs, and is checkpointed for fault-tolerant recovery (§4.5).

### 4.3 D3 — Source-side event enrichment for a config-free pipeline

Each event is enriched at the producer (the simulator, which already holds camera
configuration) with `lane_id`, `position_km`, and `speed_limit`. The join then computes segment
distance directly from the two joined records (`|b.position_km − a.position_km|`) and reads the
limit from the event, eliminating any broadcast lookup or camera-configuration load inside
Spark. A consequential operational benefit is that **cameras may be added at runtime** — new
cameras simply begin emitting enriched events — with **no pipeline restart and no code change**,
a property directly exercised by the admin platform's camera-management feature.

### 4.4 D4 — Idempotent one-document-per-violation persistence

The enforcement requirement is to **flag each vehicle once per detection window**, irrespective
of how many cameras or segments fired, or of the violation type. De-duplication therefore keys on
`car_plate` over an explicit watermark window (`dropDuplicates(["car_plate"])`) — the correct
enforcement grain, carried over from A2. The contribution of D4 is the **persistence model**: in
place of A2's `(car_plate, date)` `$push` array (L4), each flag is written as **its own document**
and upserted on the idempotent unique key `(car_plate, window_start)`, where `window_start` is the
start time floored to the dedup window. A replayed micro-batch upserts onto the same document
rather than inserting a duplicate — tolerating pipeline restarts — and the per-violation document
simplifies downstream querying, filtering, and export for the operations dashboard.

### 4.5 State bounding

The self-join carries an explicit time-range predicate and per-stream watermark, so operator
state remains bounded at approximately `arrival_rate × (window + watermark delay)` and is
checkpointed for fault-tolerant recovery [3], [4], [5]. We note that the instantaneous detector
is a stateless per-record filter requiring no join, isolating the entirety of the stateful,
Spark-justifying computation in the single self-join.

---

## 5. Evaluation Methodology

To substantiate the scalability claim empirically (rather than by assertion), a synthetic
**traffic simulator** will generate load at controlled event rates and behaviour mixes, and the
pipeline will be measured under systematic sweeps of:

- **Kafka partition count** and **Spark shuffle-partition / core count** — against sustained
  **throughput** (events s⁻¹). We hypothesise near-linear scaling for the proposed design
  versus a plateau at three for the A2 topology.
- **Increasing load** — against **end-to-end latency**, measured as the interval between an
  event's timestamp and the violation document's `detected_at`.
- **Concurrent vehicles and window length** — against **join-state size**, demonstrating the
  bounded-memory property of §4.5.

Results will be emitted as data files and charts generated entirely from executed runs, with no
static substitutes, consistent with the assignment's reproducibility requirements.

The executed evaluation, including the partition sweep results and latency
distributions, is in the repository under benchmarks/ with the full write-up in
the project README (section "Empirical evaluation (Phase 6)").

---

## 6. Trade-offs and Limitations

- **Key skew.** Partitioning on `car_plate` exchanges A2's structural camera-skew for a
  potential, rare per-plate skew (a small number of hyperactive plates). At the cardinality of
  a real plate population this is negligible; the worst case is precisely what ScaleJoin's [7]
  key-free round-robin tuple distribution addresses, and is acknowledged as a bounded risk.
- **Micro-batch latency.** Structured Streaming's micro-batch model introduces seconds-scale
  detection latency [3], acceptable for an enforcement-review use case but not for hard
  real-time control.
- **Single-node demonstration.** Evaluation runs on a single host (`local[*]`); scalability is
  demonstrated through partition/core sweeps and argued by extrapolation to a multi-node
  cluster, consistent with the cloud-simulated deployment option in the specification.
- **Join state vs. arbitrary stateful processing.** The consecutive-segment self-join (§4.2.2)
  bounds *output* to `O(k)` per vehicle, but its *state* is still the stream-join buffer of every
  event for the watermark window — `O(arrival_rate × window)`. The asymptotically stronger design
  keeps only each vehicle's **last crossing per lane** in keyed state via `applyInPandasWithState`,
  reducing state to `O(active vehicles)` and matching consecutive crossings directly (also closing
  the missed-camera gap of clause (iii)). That API requires **Spark ≥ 3.4**; the unit runtime is
  the `fit3182/pyspark` **3.3** image, where PySpark exposes no arbitrary-stateful operator
  (`flatMapGroupsWithState` is JVM-only), so it is documented as the primary future-work upgrade
  rather than implemented.

---

## 7. Conclusion

The Assignment 2 prototype validated the average-speed detection concept but encoded a
parallelism ceiling, a quadratic and recompilation-bound join topology, and a non-idempotent
persistence model. The proposed architecture removes these through a single high-cardinality-partitioned
topic, one generalised key-partitioned self-join, source-side enrichment for a config-free and
runtime-extensible pipeline, and idempotent one-document-per-violation persistence — each decision supported by
the partitioned-log, structured-streaming, and parallel-stream-join literature. Embedded in a
reproducible containerised admin platform and accompanied by an empirical scalability
evaluation, the redesign advances the prototype toward a production-shaped, operationally
defensible system.

---

## References

> *Citation details should be verified against the original publications before final
> presentation.*

[1] J. Kreps, N. Narkhede, and J. Rao, "Kafka: a Distributed Messaging System for Log
Processing," in *Proc. NetDB*, 2011.

[2] M. Zaharia et al., "Resilient Distributed Datasets: A Fault-Tolerant Abstraction for
In-Memory Cluster Computing," in *Proc. USENIX NSDI*, 2012.

[3] M. Zaharia, T. Das, H. Li, T. Hunter, S. Shenker, and I. Stoica, "Discretized Streams:
Fault-Tolerant Streaming Computation at Scale," in *Proc. ACM SOSP*, 2013.

[4] M. Armbrust et al., "Structured Streaming: A Declarative API for Real-Time Applications in
Apache Spark," in *Proc. ACM SIGMOD*, 2018.

[5] T. Akidau et al., "The Dataflow Model: A Practical Approach to Balancing Correctness,
Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing," in *Proc. VLDB
Endowment*, vol. 8, no. 12, 2015.

[6] J. Teubner and R. Mueller, "How Soccer Players Would Do Stream Joins," in *Proc. ACM
SIGMOD*, 2011.

[7] V. Gulisano, Y. Nikolakopoulos, M. Papatriantafilou, and P. Tsigas, "ScaleJoin: A
Deterministic, Disjoint-Parallel and Skew-Resilient Stream Join," *IEEE Transactions on Big
Data*, 2016.
