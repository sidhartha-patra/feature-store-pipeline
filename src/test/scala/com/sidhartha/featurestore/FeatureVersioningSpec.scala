package com.sidhartha.featurestore

import com.sidhartha.featurestore.FeatureVersioning.FeatureVersion
import com.sidhartha.featurestore.domain._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Feature versioning: labels are routed to the feature-definition version that
  * was in force as-of the label timestamp, and the as-of join respects it.
  */
class FeatureVersioningSpec extends AnyFunSuite with Matchers {

  import TestFixtures._

  // `balance` v1 is effective from T0; v2 takes over from day(10).
  private val registry = FeatureVersionRegistry.of(
    "balance" -> Seq(
      FeatureVersion(1, day(0)),
      FeatureVersion(2, day(10))
    )
  )

  private val joiner = new PointInTimeJoiner(registry)

  // Two parallel value streams: v1 values are small, v2 values are large.
  private val events = Seq(
    FeatureEvent("e1", "balance", FeatureValue.num(100.0), day(1), schemaVersion = 1),
    FeatureEvent("e1", "balance", FeatureValue.num(110.0), day(8), schemaVersion = 1),
    FeatureEvent("e1", "balance", FeatureValue.num(9000.0), day(11), schemaVersion = 2),
    FeatureEvent("e1", "balance", FeatureValue.num(9500.0), day(15), schemaVersion = 2)
  )

  test("registry resolves the version in force at a given as-of timestamp") {
    registry.versionAsOf("balance", day(5)) shouldBe Some(1)
    registry.versionAsOf("balance", day(10)) shouldBe Some(2) // effective boundary is inclusive
    registry.versionAsOf("balance", day(20)) shouldBe Some(2)
  }

  test("registry returns None before any version was effective") {
    val late = FeatureVersionRegistry.of("f" -> Seq(FeatureVersion(1, day(10))))
    late.versionAsOf("f", day(5)) shouldBe None
  }

  test("older label joins against v1 history only") {
    val labels = Seq(LabelEvent("e1", 1.0, day(9)))
    val row = joiner.join(events, labels, Seq("balance")).head
    // v1 in force at day(9); latest v1 value as-of day(9) is the day(8)=110.0.
    // The v2 stream (>= day 11) must be invisible here.
    row.valueOf("balance") shouldBe Some(NumericValue(110.0))
    row.feature("balance").flatMap(_.version) shouldBe Some(1)
  }

  test("newer label is routed to v2 and never sees stale v1 values") {
    val labels = Seq(LabelEvent("e1", 1.0, day(12)))
    val row = joiner.join(events, labels, Seq("balance")).head
    // v2 in force at day(12); latest v2 value as-of day(12) is day(11)=9000.0.
    row.valueOf("balance") shouldBe Some(NumericValue(9000.0))
    row.feature("balance").flatMap(_.version) shouldBe Some(2)
  }

  test("newer label with no v2 value yet yields missing rather than falling back to v1") {
    // Label at day(10): v2 is now in force, but the first v2 event is at day(11).
    val labels = Seq(LabelEvent("e1", 1.0, day(10)))
    val row = joiner.join(events, labels, Seq("balance")).head
    row.feature("balance").map(_.isMissing) shouldBe Some(true)
    row.feature("balance").flatMap(_.version) shouldBe Some(2) // resolved version retained
  }

  test("unregistered feature falls back to plain latest-as-of across all versions") {
    val plainJoiner = new PointInTimeJoiner(FeatureVersionRegistry.empty)
    val labels = Seq(LabelEvent("e1", 1.0, day(12)))
    val row = plainJoiner.join(events, labels, Seq("balance")).head
    // With no version routing, the globally latest as-of day(12) is day(11)=9000.0.
    row.valueOf("balance") shouldBe Some(NumericValue(9000.0))
  }
}
