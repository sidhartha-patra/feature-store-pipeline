package com.sidhartha.featurestore

import com.sidhartha.featurestore.domain._

import java.time.Instant

/** Point-in-time ("as-of") join between feature events and label events.
  *
  * ==Why this exists==
  * Point-in-time correctness is one of the most common and most costly bugs in
  * production ML. If, when building a training row for a label observed at time
  * `T`, you join a feature value that was only observed *after* `T`, you leak
  * the future into the model. The model looks great offline and collapses in
  * production, because at serving time that future value does not yet exist.
  * This is a form of label leakage / train-serve skew.
  *
  * ==Guarantee==
  * For every (label, feature) pair the joiner selects the feature event with the
  * greatest `eventTimestamp` such that `eventTimestamp <= labelTimestamp`, scoped
  * to the entity and (optionally) the feature version resolved for that label.
  * A value observed strictly after the label timestamp can never be selected.
  *
  * ==Algorithm & complexity==
  * Feature events are grouped by `(entityId, featureName, schemaVersion)` and
  * each group is sorted ascending by `eventTimestamp` once, up front. For each
  * requested feature of each label we binary-search that group for the rightmost
  * element with `eventTimestamp <= labelTimestamp`.
  *
  *   - Indexing:      O(F log F)      (F = number of feature events)
  *   - Per lookup:    O(log V)        (V = events in the matching group)
  *   - Total joins:   O(F log F + L * K * log V)
  *
  * where L = number of labels and K = number of requested features. This avoids
  * the naive O(L * F) nested scan. The equivalent Spark job would sort/partition
  * by `(entity_id, feature_name)` and use a windowed as-of join instead.
  *
  * ==Missing values==
  * If no feature event satisfies the as-of condition for a (label, feature) the
  * result is an explicit [[FeatureJoinResult]] with `value = None` (rather than a
  * silent drop). Downstream the [[DataQualityGate]] can enforce a null-rate
  * threshold on these.
  */
final class PointInTimeJoiner(registry: FeatureVersionRegistry = FeatureVersionRegistry.empty) {

  import PointInTimeJoiner._

  /** Join the requested `featureNames` onto every label, as-of each label's
    * timestamp.
    *
    * @param featureEvents the full set of feature observations
    * @param labelEvents   the labels to build training rows for
    * @param featureNames  which features to attach to every training row
    * @return one [[TrainingRow]] per label, in the input label order
    */
  def join(
      featureEvents: Seq[FeatureEvent],
      labelEvents: Seq[LabelEvent],
      featureNames: Seq[String]
  ): Seq[TrainingRow] = {
    val index = buildIndex(featureEvents)
    labelEvents.map { label =>
      val joined = featureNames.map { fname =>
        fname -> joinOne(index, label, fname)
      }.toMap
      TrainingRow(label.entityId, label.labelTimestamp, label.labelValue, joined)
    }
  }

  /** Resolve a single (label, feature) as-of value. */
  private def joinOne(index: Index, label: LabelEvent, featureName: String): FeatureJoinResult = {
    val asOf = label.labelTimestamp
    val resolvedVersion = registry.versionAsOf(featureName, asOf)

    // Candidate groups: if a version is resolved, restrict to it; otherwise
    // (unregistered feature) consider every version this entity/feature has.
    val candidateGroups: Iterable[Vector[FeatureEvent]] = resolvedVersion match {
      case Some(v) =>
        index.get((label.entityId, featureName, v)).toList
      case None =>
        index.collect {
          case ((eid, fn, _), events) if eid == label.entityId && fn == featureName => events
        }
    }

    // Best as-of event across candidate groups (max eventTimestamp <= asOf).
    val best: Option[FeatureEvent] = candidateGroups.iterator
      .flatMap(group => latestAsOf(group, asOf).iterator)
      .reduceOption((a, b) => if (a.eventTimestamp.isBefore(b.eventTimestamp)) b else a)

    best match {
      case Some(ev) =>
        FeatureJoinResult(featureName, Some(ev.value), Some(ev.eventTimestamp), Some(ev.schemaVersion))
      case None =>
        FeatureJoinResult(featureName, None, None, resolvedVersion)
    }
  }

  /** Build the sorted, grouped index over all feature events. */
  private def buildIndex(featureEvents: Seq[FeatureEvent]): Index =
    featureEvents
      .groupBy(e => (e.entityId, e.featureName, e.schemaVersion))
      .map { case (key, events) =>
        // Stable sort ascending by eventTimestamp; ties keep input order, so a
        // binary search for the rightmost `<= asOf` returns the last event
        // provided at that instant (documented, deterministic tie-break).
        key -> events.toVector.sortBy(_.eventTimestamp)
      }
}

object PointInTimeJoiner {

  private type Key = (String, String, Int)
  private type Index = Map[Key, Vector[FeatureEvent]]

  /** Binary search: rightmost event in an ascending-by-timestamp group whose
    * `eventTimestamp <= asOf`. Returns `None` if every event is after `asOf`.
    *
    * This is the core anti-leakage primitive: it can never return an event with
    * `eventTimestamp > asOf`.
    */
  private[featurestore] def latestAsOf(
      sortedAscending: Vector[FeatureEvent],
      asOf: Instant
  ): Option[FeatureEvent] = {
    var lo = 0
    var hi = sortedAscending.length - 1
    var result = -1
    while (lo <= hi) {
      val mid = (lo + hi) >>> 1
      // eventTimestamp <= asOf  <=>  !(eventTimestamp isAfter asOf)
      if (!sortedAscending(mid).eventTimestamp.isAfter(asOf)) {
        result = mid
        lo = mid + 1
      } else {
        hi = mid - 1
      }
    }
    if (result >= 0) Some(sortedAscending(result)) else None
  }
}
