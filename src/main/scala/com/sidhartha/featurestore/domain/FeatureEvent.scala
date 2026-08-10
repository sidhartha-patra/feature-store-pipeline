package com.sidhartha.featurestore.domain

import java.time.Instant

/** A single observation of a feature for an entity at a point in time.
  *
  * This is the analogue of one row in a Spark feature table:
  * {{{
  *   (entity_id, feature_name, value, event_timestamp, schema_version)
  * }}}
  *
  * @param entityId        the entity the feature describes (e.g. a customer id)
  * @param featureName     the logical feature name (e.g. `"account_balance"`)
  * @param value           the observed value at [[eventTimestamp]]
  * @param eventTimestamp  when the value became true in the real world; this is
  *                        the timestamp used for point-in-time correctness
  * @param schemaVersion   the feature-definition version that produced this value
  */
final case class FeatureEvent(
    entityId: String,
    featureName: String,
    value: FeatureValue,
    eventTimestamp: Instant,
    schemaVersion: Int = 1
)
