package com.sidhartha.featurestore

import com.sidhartha.featurestore.DataQualityGate._
import com.sidhartha.featurestore.domain._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.{Duration, Instant}

/** Data-quality gate: each rule type passes and fails as designed, and the
  * failure diagnostics name the offending entities.
  */
class DataQualityGateSpec extends AnyFunSuite with Matchers {

  import TestFixtures._

  private def row(
      entity: String,
      label: Instant,
      value: Option[FeatureValue],
      asOf: Option[Instant],
      feature: String = "f"
  ): TrainingRow =
    TrainingRow(
      entity,
      label,
      1.0,
      Map(feature -> FeatureJoinResult(feature, value, asOf, value.map(_ => 1)))
    )

  // --- NullRateThreshold ------------------------------------------------------

  test("null-rate rule passes when missing fraction is within threshold") {
    val rows = Seq(
      row("e1", day(1), Some(FeatureValue.num(1.0)), Some(day(1))),
      row("e2", day(1), Some(FeatureValue.num(2.0)), Some(day(1))),
      row("e3", day(1), None, None) // 1/3 missing
    )
    val res = NullRateThreshold("f", maxNullRate = 0.5).evaluate(rows)
    res.passed shouldBe true
    res.affectedEntities shouldBe empty
  }

  test("null-rate rule fails and names affected entities when threshold exceeded") {
    val rows = Seq(
      row("e1", day(1), None, None),
      row("e2", day(1), None, None),
      row("e3", day(1), Some(FeatureValue.num(2.0)), Some(day(1))) // 2/3 missing
    )
    val res = NullRateThreshold("f", maxNullRate = 0.1).evaluate(rows)
    res.passed shouldBe false
    res.affectedEntities should contain("e1")
    res.affectedEntities should contain("e2")
    res.message should include("null-rate")
  }

  // --- NumericRange -----------------------------------------------------------

  test("numeric-range rule passes when all present values are within range") {
    val rows = Seq(
      row("e1", day(1), Some(FeatureValue.num(5.0)), Some(day(1))),
      row("e2", day(1), Some(FeatureValue.num(9.0)), Some(day(1)))
    )
    NumericRange("f", 0.0, 10.0).evaluate(rows).passed shouldBe true
  }

  test("numeric-range rule fails for out-of-range values and reports them") {
    val rows = Seq(
      row("e1", day(1), Some(FeatureValue.num(-1.0)), Some(day(1))),
      row("e2", day(1), Some(FeatureValue.num(5.0)), Some(day(1))),
      row("e3", day(1), Some(FeatureValue.num(42.0)), Some(day(1)))
    )
    val res = NumericRange("f", 0.0, 10.0).evaluate(rows)
    res.passed shouldBe false
    res.affectedEntities should contain("e1")
    res.affectedEntities should contain("e3")
    res.affectedEntities should not contain "e2"
  }

  test("numeric-range rule flags a categorical value as a type violation") {
    val rows = Seq(row("e1", day(1), Some(FeatureValue.cat("gold")), Some(day(1))))
    val res = NumericRange("f", 0.0, 10.0).evaluate(rows)
    res.passed shouldBe false
    res.message should include("non-numeric")
  }

  // --- FreshnessThreshold -----------------------------------------------------

  test("freshness rule passes when the as-of value is recent enough") {
    val rows = Seq(
      // label at day 5, value as-of day 4 -> 1 day stale, within 2-day budget
      row("e1", day(5), Some(FeatureValue.num(1.0)), Some(day(4)))
    )
    FreshnessThreshold("f", Duration.ofDays(2)).evaluate(rows).passed shouldBe true
  }

  test("freshness rule fails when the as-of value is too stale") {
    val rows = Seq(
      // label at day 10, value as-of day 1 -> 9 days stale, budget is 2 days
      row("e1", day(10), Some(FeatureValue.num(1.0)), Some(day(1)))
    )
    val res = FreshnessThreshold("f", Duration.ofDays(2)).evaluate(rows)
    res.passed shouldBe false
    res.affectedEntities shouldBe Seq("e1")
    res.message should include("stale")
  }

  test("freshness rule ignores missing values (that is null-rate's concern)") {
    val rows = Seq(row("e1", day(10), None, None))
    FreshnessThreshold("f", Duration.ofDays(2)).evaluate(rows).passed shouldBe true
  }

  // --- Aggregate report -------------------------------------------------------

  test("report passes only when all rules pass and renders a readable summary") {
    val rows = Seq(row("e1", day(2), Some(FeatureValue.num(5.0)), Some(day(1))))
    val report = DataQualityGate.evaluate(
      rows,
      Seq(
        NullRateThreshold("f", 0.0),
        NumericRange("f", 0.0, 10.0),
        FreshnessThreshold("f", Duration.ofDays(30))
      )
    )
    report.passed shouldBe true
    report.render should include("DATA QUALITY: PASSED")
  }

  test("report aggregates multiple failures with per-rule diagnostics") {
    val rows = Seq(
      row("e1", day(10), Some(FeatureValue.num(999.0)), Some(day(1))) // out of range AND stale
    )
    val report = DataQualityGate.evaluate(
      rows,
      Seq(
        NumericRange("f", 0.0, 10.0),
        FreshnessThreshold("f", Duration.ofDays(2))
      )
    )
    report.passed shouldBe false
    report.failures.size shouldBe 2
    report.render should include("FAILED")
    report.render should include("numeric_range")
    report.render should include("freshness")
  }
}
