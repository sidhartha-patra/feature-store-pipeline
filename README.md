# Feature Store Pipeline

A compact, production-grade reference implementation of the three things that
separate a **correct** ML feature pipeline from a subtly broken one:

1. **Point-in-time correctness** — as-of joins that make future data leakage
   *structurally impossible*, not just unlikely.
2. **Feature versioning** — evolving a feature's definition without silently
   rewriting the history that older labels were trained against.
3. **A data-quality gate** — validation rules that **fail the run loudly** with
   a precise diagnostic instead of emitting corrupt training data.

It is written in Scala (sbt, ScalaTest) using plain immutable collections. The
pipeline logic is deliberately structured to mirror how the same job would be
expressed in Apache Spark — see [Mapping to a real Spark
job](#mapping-to-a-real-spark-job).

---

## Problem statement: why this is one of the costliest ML bugs

Most "the model was great offline but died in production" incidents are not
modeling problems. They are **data-time** problems:

- **Label leakage / lookahead bias.** When you assemble a training example for a
  label observed at time `T`, every feature attached to it must have been
  *knowable at or before `T`*. If any feature reflects information that only
  existed *after* `T`, the model learns to "cheat" using the future. Offline
  metrics look excellent; at serving time that future value does not exist yet,
  so the model silently underperforms. This is **train/serve skew** in its most
  expensive form.
- **Silent feature drift on redefinition.** Teams routinely redefine a feature
  (a bug fix, a new data source, a units change). If you recompute the *new*
  definition over *old* timestamps, you retroactively train on values that never
  existed historically — another flavor of leakage that is nearly invisible in
  code review.
- **Silent data corruption.** A feature suddenly goes 90% null, or a value
  jumps outside its physical range, or the freshest available value is weeks
  stale. Without a gate, the pipeline happily produces garbage and the model
  quietly degrades.

This project encodes the guardrails for all three.

---

## Domain model

| Type | Meaning | Spark analogue |
|------|---------|----------------|
| `FeatureEvent(entityId, featureName, value, eventTimestamp, schemaVersion)` | One observation of a feature for an entity, valid as of `eventTimestamp` | A row in a feature table |
| `LabelEvent(entityId, labelValue, labelTimestamp)` | A training label anchored to an as-of instant | A row in the label/spine table |
| `TrainingRow(entityId, labelTimestamp, labelValue, features)` | A label plus its point-in-time-correct features | One row of the training `DataFrame` |
| `FeatureValue` = `NumericValue` \| `CategoricalValue` | Typed feature value (sealed ADT) | A typed column in a `StructType` |
| `FeatureJoinResult(featureName, value, asOfTimestamp, version)` | The chosen value for one feature, plus provenance | The joined columns + audit columns |

Missing values are represented explicitly (`value = None`) rather than dropped,
so the quality gate can reason about them.

---

## The as-of join algorithm

For every `(label, feature)` pair the joiner selects the feature event with the
**greatest `eventTimestamp` such that `eventTimestamp <= labelTimestamp`**,
scoped to the entity and (optionally) the resolved feature version.

```
value(label, feature) =
    argmax_{e in events}  e.eventTimestamp
    subject to  e.entityId       == label.entityId
            and e.featureName     == feature
            and e.schemaVersion   == versionAsOf(feature, label.labelTimestamp)   // if registered
            and e.eventTimestamp  <= label.labelTimestamp                          // anti-leakage
```

The `<=` is the entire ballgame: an event observed *strictly after* the label
can never be selected. The boundary case (`eventTimestamp == labelTimestamp`) is
eligible — a value known exactly at the anchor instant is legitimately usable.

### Complexity

Feature events are grouped by `(entityId, featureName, schemaVersion)` and each
group is **sorted once** ascending by `eventTimestamp`. Each lookup is a
**binary search** for the rightmost element `<= labelTimestamp`.

| Phase | Cost |
|-------|------|
| Build index | `O(F log F)` |
| Single feature lookup | `O(log V)` |
| Full run | `O(F log F + L · K · log V)` |

where `F` = feature events, `L` = labels, `K` = features per label, `V` = events
in the matched group. This deliberately avoids the naive `O(L · F)` nested scan.
The tie-break is documented and deterministic: for equal timestamps, the stable
sort keeps input order and the search returns the **last** event provided at that
instant.

---

## Feature versioning strategy

A feature definition evolves. We model this with a `FeatureVersionRegistry`
mapping each feature to a timeline of `FeatureVersion(version, effectiveFrom)`.

- `versionAsOf(feature, t)` returns the version whose `effectiveFrom <= t` is
  latest — i.e. the definition **in force at the label's anchor time**.
- The joiner then restricts candidate feature events to that version before the
  as-of search.

This gives two correctness properties simultaneously:

- **Historical fidelity.** A label anchored *before* a v1→v2 cut-over joins
  against v1 data only. We never retroactively apply v2 to old history.
- **Correct routing.** A label anchored *after* the cut-over is routed to v2,
  and — importantly — will report a **missing value** rather than silently
  falling back to a stale v1 value if no v2 value exists yet.

Unregistered features fall back to plain latest-as-of semantics across all
versions, so versioning is strictly opt-in per feature.

---

## Data-quality gate design

The gate runs configurable rules over the produced `TrainingRow`s. Each rule
returns a `RuleResult` (pass/fail + message + **the exact offending entities**),
and the run aborts with a `DataQualityException` — carrying the full rendered
report — if any rule fails.

| Rule | Fails when… |
|------|-------------|
| `NullRateThreshold(feature, maxNullRate)` | the fraction of rows with a missing value for `feature` exceeds the threshold |
| `NumericRange(feature, min, max)` | any present value is outside `[min, max]`, **or** is non-numeric where numeric is expected (type violation) |
| `FreshnessThreshold(feature, maxStaleness)` | the as-of value chosen for a row is older than `maxStaleness` relative to the label timestamp |

The philosophy: **a pipeline that crashes is better than one that silently ships
bad training data.** The diagnostic names the rule, the measured value vs. the
threshold, and the affected entities, so an on-call engineer can triage in
seconds.

Example rendered failure:

```
DATA QUALITY: FAILED (2 of 2 rules violated)
  [FAIL] numeric_range(balance in [0.0, 10.0]): 1 value(s) out of range: e1=999.0 affected=[e1]
  [FAIL] freshness(balance <= 172800s): 1 stale value(s): e1=777600s affected=[e1]
```

---

## Mapping to a real Spark job

**This implementation intentionally uses pure Scala collections instead of a
`org.apache.spark` dependency.** In this constrained/offline CI-like
environment a full local Spark session is slow and flaky (large dependency
tree, native Hadoop `winutils` issues on Windows). Using plain `Seq`/`Vector`
keeps the project fast, deterministic, and trivially testable — while the logic
is written so it translates almost 1:1 to Spark:

| This project | Spark equivalent |
|--------------|------------------|
| `Seq[FeatureEvent]`, `Seq[LabelEvent]` | `Dataset[FeatureEvent]`, `Dataset[LabelEvent]` (or `DataFrame`) |
| `groupBy((entityId, featureName, version))` + sort | `repartition`/`Window.partitionBy(entity, feature).orderBy(event_ts)` |
| binary search for rightmost `eventTimestamp <= labelTimestamp` | as-of join: `labels.join(features, entity && feature && feature.event_ts <= label.ts)` then `Window` `row_number()`/`last(...)` to pick the latest, **or** Spark 3.5+ `Dataset` as-of join semantics |
| `FeatureVersionRegistry.versionAsOf` filter | a broadcast-joined version dimension filtered by `effective_from <= label_ts` |
| `DataQualityGate` rules | a validation stage (e.g. Deequ / custom aggregations) that raises before the write |
| throwing `DataQualityException` | failing the Spark job / Airflow task before `df.write` |

The signatures were chosen to make this mapping obvious, e.g.:

```scala
def join(featureEvents: Seq[FeatureEvent],
         labelEvents:   Seq[LabelEvent],
         featureNames:  Seq[String]): Seq[TrainingRow]
```

reads directly as "as-of join the label spine against each feature table."

---

## Usage

```scala
import com.sidhartha.featurestore._
import com.sidhartha.featurestore.FeatureVersioning.FeatureVersion
import com.sidhartha.featurestore.DataQualityGate._
import com.sidhartha.featurestore.domain._
import java.time.{Duration, Instant}

val registry = FeatureVersionRegistry.of(
  "balance" -> Seq(
    FeatureVersion(1, Instant.parse("2026-01-01T00:00:00Z")),
    FeatureVersion(2, Instant.parse("2026-01-11T00:00:00Z"))
  )
)

val config = PipelineConfig(
  featureNames = Seq("balance"),
  registry     = registry,
  qualityRules = Seq(
    NullRateThreshold("balance", maxNullRate = 0.0),
    NumericRange("balance", 0.0, 1e6),
    FreshnessThreshold("balance", Duration.ofDays(30))
  )
)

// Returns training rows on success; throws DataQualityException on violation.
val rows = new Pipeline(config).run(featureEvents, labelEvents)
```

---

## Build & test

Requires JDK 21 and sbt.

```bash
sbt test        # compile + run the full ScalaTest suite
sbt compile     # compile only
```

CI (`.github/workflows/ci.yml`) runs `sbt test` on Temurin JDK 21 for every
push and PR to `main`.

### Project layout

```
feature-store-pipeline/
├── build.sbt
├── project/build.properties          # sbt 1.10.7
├── src/main/scala/com/sidhartha/featurestore/
│   ├── domain/                        # FeatureEvent, LabelEvent, TrainingRow, FeatureValue
│   ├── PointInTimeJoiner.scala        # as-of join (anti-leakage core)
│   ├── FeatureVersioning.scala        # version registry & routing
│   ├── DataQualityGate.scala          # rules, report, DataQualityException
│   └── Pipeline.scala                 # orchestration
└── src/test/scala/com/sidhartha/featurestore/
    ├── PointInTimeJoinerSpec.scala
    ├── FeatureVersioningSpec.scala
    ├── DataQualityGateSpec.scala
    └── PipelineSpec.scala
```

---

## Limitations & scope

This is a focused reference implementation, not a full feature platform.
Explicit non-goals / simplifications:

- **In-memory only.** Everything operates over Scala collections. There is no
  storage layer, no offline/online store, and no serving path. The
  [Spark mapping](#mapping-to-a-real-spark-job) describes the intended scale-out.
- **No distributed execution.** Single-JVM, single-thread. The complexity
  analysis holds; the constants would change under partitioning/shuffle.
- **Point-in-time semantics are per-feature latest-as-of.** Windowed aggregate
  features (e.g. "count of events in the last 7 days as-of `T`") are not
  implemented, though they compose naturally on top of the same as-of primitive.
- **Label values are numeric** for simplicity; multi-class/structured labels
  would generalize `LabelEvent.labelValue`.
- **Time is event-time only.** There is no separate "arrival/processing time"
  dimension (bitemporal modeling), which some feature stores track to handle
  late-arriving data.
- **Quality rules are row/column-level.** Distributional checks (PSI, KS drift
  vs. a reference window) are out of scope but would slot in as additional
  `DataQualityRule`s.

## License

MIT — see [LICENSE](LICENSE). Copyright (c) 2026 Sidhartha Patra.
