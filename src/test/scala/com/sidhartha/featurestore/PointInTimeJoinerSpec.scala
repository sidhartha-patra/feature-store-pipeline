package com.sidhartha.featurestore

import com.sidhartha.featurestore.domain._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Correctness of the point-in-time (as-of) join, with an emphasis on the
  * anti-leakage guarantee.
  */
class PointInTimeJoinerSpec extends AnyFunSuite with Matchers {

  import TestFixtures._

  private val joiner = new PointInTimeJoiner()

  test("selects the latest feature value at or before the label timestamp") {
    val events = Seq(
      FeatureEvent("e1", "balance", FeatureValue.num(100.0), day(1)),
      FeatureEvent("e1", "balance", FeatureValue.num(200.0), day(3)),
      FeatureEvent("e1", "balance", FeatureValue.num(300.0), day(5))
    )
    val labels = Seq(LabelEvent("e1", 1.0, day(4)))

    val row = joiner.join(events, labels, Seq("balance")).head
    // Latest value with event_timestamp <= day(4) is the day(3) value = 200.0
    row.valueOf("balance") shouldBe Some(NumericValue(200.0))
    row.feature("balance").flatMap(_.asOfTimestamp) shouldBe Some(day(3))
  }

  test("NEVER leaks a value observed strictly after the label timestamp") {
    // The only feature update happens the day AFTER the label. It must not leak.
    val events = Seq(
      FeatureEvent("e1", "risk_score", FeatureValue.num(0.9), day(10))
    )
    val labels = Seq(LabelEvent("e1", 1.0, day(9)))

    val row = joiner.join(events, labels, Seq("risk_score")).head
    row.valueOf("risk_score") shouldBe None
    row.feature("risk_score").map(_.isMissing) shouldBe Some(true)
  }

  test("boundary: an event exactly at the label timestamp IS eligible (<=)") {
    val events = Seq(FeatureEvent("e1", "f", FeatureValue.num(42.0), day(7)))
    val labels = Seq(LabelEvent("e1", 1.0, day(7)))

    val row = joiner.join(events, labels, Seq("f")).head
    row.valueOf("f") shouldBe Some(NumericValue(42.0))
  }

  test("handles multiple features per entity independently") {
    val events = Seq(
      FeatureEvent("e1", "balance", FeatureValue.num(100.0), day(1)),
      FeatureEvent("e1", "balance", FeatureValue.num(150.0), day(4)),
      FeatureEvent("e1", "tenure", FeatureValue.num(12.0), day(2)),
      FeatureEvent("e1", "tenure", FeatureValue.num(24.0), day(6))
    )
    val labels = Seq(LabelEvent("e1", 1.0, day(5)))

    val row = joiner.join(events, labels, Seq("balance", "tenure")).head
    row.valueOf("balance") shouldBe Some(NumericValue(150.0)) // day(4) <= day(5)
    row.valueOf("tenure") shouldBe Some(NumericValue(12.0))   // day(6) is future -> day(2)
  }

  test("isolates entities: one entity's events never bleed into another") {
    val events = Seq(
      FeatureEvent("e1", "balance", FeatureValue.num(100.0), day(1)),
      FeatureEvent("e2", "balance", FeatureValue.num(999.0), day(1))
    )
    val labels = Seq(
      LabelEvent("e1", 1.0, day(2)),
      LabelEvent("e2", 0.0, day(2))
    )
    val rows = joiner.join(events, labels, Seq("balance"))
    rows.find(_.entityId == "e1").flatMap(_.valueOf("balance")) shouldBe Some(NumericValue(100.0))
    rows.find(_.entityId == "e2").flatMap(_.valueOf("balance")) shouldBe Some(NumericValue(999.0))
  }

  test("entity with no matching feature event yields an explicit missing value") {
    val events = Seq(FeatureEvent("e1", "balance", FeatureValue.num(100.0), day(1)))
    val labels = Seq(LabelEvent("ghost", 1.0, day(2)))

    val row = joiner.join(events, labels, Seq("balance")).head
    row.entityId shouldBe "ghost"
    row.feature("balance").map(_.isMissing) shouldBe Some(true)
    row.feature("balance").flatMap(_.asOfTimestamp) shouldBe None
  }

  test("multiple historical versions of the same feature: picks correct as-of one") {
    // Five daily updates; verify the correct one is chosen for several labels.
    val events = (1 to 5).map(d => FeatureEvent("e1", "f", FeatureValue.num(d * 10.0), day(d)))
    val labels = Seq(
      LabelEvent("e1", 1.0, day(1)), // -> 10
      LabelEvent("e1", 1.0, day(3)), // -> 30
      LabelEvent("e1", 1.0, day(5))  // -> 50
    )
    val rows = joiner.join(events, labels, Seq("f"))
    rows.map(_.valueOf("f")) shouldBe Seq(
      Some(NumericValue(10.0)),
      Some(NumericValue(30.0)),
      Some(NumericValue(50.0))
    )
  }

  test("preserves label input order in the output rows") {
    val events = Seq(FeatureEvent("e1", "f", FeatureValue.num(1.0), day(0)))
    val labels = Seq(
      LabelEvent("b", 1.0, day(1)),
      LabelEvent("a", 1.0, day(1)),
      LabelEvent("c", 1.0, day(1))
    )
    joiner.join(events, labels, Seq("f")).map(_.entityId) shouldBe Seq("b", "a", "c")
  }

  test("latestAsOf binary-search primitive returns None when all events are future") {
    val group = Vector(
      FeatureEvent("e1", "f", FeatureValue.num(1.0), day(5)),
      FeatureEvent("e1", "f", FeatureValue.num(2.0), day(6))
    )
    PointInTimeJoiner.latestAsOf(group, day(4)) shouldBe None
    PointInTimeJoiner.latestAsOf(group, day(5)).map(_.value) shouldBe Some(NumericValue(1.0))
  }
}
