package com.sidhartha.featurestore.domain

import java.time.Instant

/** A supervised-learning label observed for an entity at a point in time.
  *
  * The [[labelTimestamp]] is the "as-of" cut point: every feature joined onto
  * this label must have been known at or before this instant. Joining any
  * feature value observed *after* this timestamp would leak the future into
  * training data.
  *
  * @param entityId       the entity the label describes
  * @param labelValue     the target/label value (kept numeric for simplicity)
  * @param labelTimestamp the instant the label is anchored to (the as-of point)
  */
final case class LabelEvent(
    entityId: String,
    labelValue: Double,
    labelTimestamp: Instant
)
