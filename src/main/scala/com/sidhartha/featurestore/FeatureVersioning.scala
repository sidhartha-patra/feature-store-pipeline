package com.sidhartha.featurestore

import java.time.Instant

/** Feature versioning.
  *
  * A feature definition evolves over time. For example `account_balance` may be
  * redefined at a cut-over instant (v1 -> v2) because the upstream computation
  * changed. Two correctness requirements follow:
  *
  *   1. Labels anchored *before* the cut-over must be joined against the value
  *      definition that was in effect back then (v1), otherwise we retroactively
  *      rewrite history and train on values that never existed at that time.
  *   2. Labels anchored *after* the cut-over must be routed to the new
  *      definition (v2).
  *
  * The registry stores, per feature, the list of versions and the instant each
  * became effective. [[FeatureVersionRegistry.versionAsOf]] resolves the version
  * in force at a given as-of timestamp. The [[PointInTimeJoiner]] then filters
  * feature events to that resolved version before performing the as-of join.
  */
object FeatureVersioning {

  /** A feature-definition version and the instant it became effective. */
  final case class FeatureVersion(version: Int, effectiveFrom: Instant)
}

import FeatureVersioning.FeatureVersion

/** Immutable registry mapping feature name -> effective version timeline.
  *
  * @param versions per-feature list of [[FeatureVersion]] entries (order-agnostic;
  *                 the registry sorts internally by `effectiveFrom`).
  */
final class FeatureVersionRegistry private (
    private val versions: Map[String, Vector[FeatureVersion]]
) {

  /** Resolve the feature version in force as-of `asOf`.
    *
    * Returns the version with the latest `effectiveFrom <= asOf`. If the feature
    * is not registered, returns `None`, which the joiner treats as "no version
    * routing" (join across all versions using plain latest-as-of semantics).
    *
    * @return `Some(version)` when the feature is registered and at least one
    *         version was effective as-of `asOf`; `None` otherwise.
    */
  def versionAsOf(featureName: String, asOf: Instant): Option[Int] =
    versions.get(featureName).flatMap { vs =>
      vs.iterator
        .filter(v => !v.effectiveFrom.isAfter(asOf))
        .reduceOption((a, b) => if (a.effectiveFrom.isAfter(b.effectiveFrom)) a else b)
        .map(_.version)
    }

  /** True if the feature has an explicit version timeline registered. */
  def isRegistered(featureName: String): Boolean = versions.contains(featureName)

  /** All versions ever defined for a feature (sorted by effectiveFrom). */
  def timeline(featureName: String): Vector[FeatureVersion] =
    versions.getOrElse(featureName, Vector.empty).sortBy(_.effectiveFrom)
}

object FeatureVersionRegistry {

  /** An empty registry: no feature is version-routed. */
  val empty: FeatureVersionRegistry = new FeatureVersionRegistry(Map.empty)

  /** Build a registry from `featureName -> versions`. */
  def apply(entries: Map[String, Seq[FeatureVersion]]): FeatureVersionRegistry =
    new FeatureVersionRegistry(entries.map { case (k, v) => k -> v.toVector }.toMap)

  /** Convenience builder from tuples. */
  def of(entries: (String, Seq[FeatureVersion])*): FeatureVersionRegistry =
    apply(entries.toMap)
}
