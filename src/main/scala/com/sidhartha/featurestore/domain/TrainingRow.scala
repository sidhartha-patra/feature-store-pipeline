package com.sidhartha.featurestore.domain

import java.time.Instant

/** The point-in-time-correct value chosen for one feature of one label.
  *
  * @param featureName    the feature this result is for
  * @param value          the chosen value, or `None` if no valid value existed
  *                       as-of the label timestamp (documented missing handling)
  * @param asOfTimestamp  the [[FeatureEvent.eventTimestamp]] of the chosen value;
  *                       `None` when the value is missing
  * @param version        the resolved feature schema version used for the join;
  *                       `None` when missing
  */
final case class FeatureJoinResult(
    featureName: String,
    value: Option[FeatureValue],
    asOfTimestamp: Option[Instant],
    version: Option[Int]
) {

  /** True when no value could be joined for this feature as-of the label. */
  def isMissing: Boolean = value.isEmpty

  /** Staleness of the chosen value relative to a label timestamp, if present. */
  def stalenessSeconds(labelTimestamp: Instant): Option[Long] =
    asOfTimestamp.map(ts => java.time.Duration.between(ts, labelTimestamp).getSeconds)
}

/** A fully assembled training example: a label plus its point-in-time features.
  *
  * This is the analogue of one row of a Spark training `DataFrame` produced by
  * an as-of join of the label table against each feature table.
  *
  * @param entityId       the entity id
  * @param labelTimestamp the as-of point the row is anchored to
  * @param labelValue     the target value
  * @param features       resolved feature values keyed by feature name
  */
final case class TrainingRow(
    entityId: String,
    labelTimestamp: Instant,
    labelValue: Double,
    features: Map[String, FeatureJoinResult]
) {

  /** Convenience lookup for a feature's join result. */
  def feature(name: String): Option[FeatureJoinResult] = features.get(name)

  /** Convenience lookup for a feature's raw value. */
  def valueOf(name: String): Option[FeatureValue] = features.get(name).flatMap(_.value)
}
