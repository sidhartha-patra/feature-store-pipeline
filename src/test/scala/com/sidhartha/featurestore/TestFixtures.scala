package com.sidhartha.featurestore

import java.time.Instant

/** Small helpers to keep test scenarios readable. Timestamps are expressed as
  * days offset from a fixed epoch so the temporal ordering is obvious.
  */
object TestFixtures {

  /** Fixed reference instant: 2026-01-01T00:00:00Z. */
  val T0: Instant = Instant.parse("2026-01-01T00:00:00Z")

  /** `T0` plus `days` (fractional days allowed via [[hours]]). */
  def day(days: Long): Instant = T0.plusSeconds(days * 86400L)

  /** `T0` plus `hours`. */
  def hour(hours: Long): Instant = T0.plusSeconds(hours * 3600L)
}
