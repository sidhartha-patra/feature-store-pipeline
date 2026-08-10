package com.sidhartha.featurestore

import com.sidhartha.featurestore.DataQualityGate._
import com.sidhartha.featurestore.FeatureVersioning.FeatureVersion
import com.sidhartha.featurestore.domain._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Duration

/** End-to-end pipeline behaviour: join + versioning + gate, including the
  * loud-failure contract.
  */
class PipelineSpec extends AnyFunSuite with Matchers {

  import TestFixtures._

  private val registry = FeatureVersionRegistry.of(
    "balance" -> Seq(FeatureVersion(1, day(0)), FeatureVersion(2, day(10)))
  )

  private val events = Seq(
    FeatureEvent("e1", "balance", FeatureValue.num(100.0), day(1), schemaVersion = 1),
    FeatureEvent("e1", "balance", FeatureValue.num(9000.0), day(11), schemaVersion = 2),
    FeatureEvent("e2", "balance", FeatureValue.num(250.0), day(2), schemaVersion = 1)
  )

  test("happy path: produces point-in-time-correct rows and passes the gate") {
    val config = PipelineConfig(
      featureNames = Seq("balance"),
      registry = registry,
      qualityRules = Seq(
        NullRateThreshold("balance", 0.0),
        NumericRange("balance", 0.0, 100000.0),
        FreshnessThreshold("balance", Duration.ofDays(365))
      )
    )
    val labels = Seq(
      LabelEvent("e1", 1.0, day(5)),  // v1 -> 100.0
      LabelEvent("e2", 0.0, day(5))   // v1 -> 250.0
    )
    val rows = new Pipeline(config).run(events, labels)
    rows.map(_.valueOf("balance")) shouldBe Seq(
      Some(NumericValue(100.0)),
      Some(NumericValue(250.0))
    )
  }

  test("run throws DataQualityException with a diagnostic report when the gate fails") {
    val config = PipelineConfig(
      featureNames = Seq("balance"),
      registry = registry,
      qualityRules = Seq(NullRateThreshold("balance", maxNullRate = 0.0))
    )
    // "ghost" has no feature events -> a missing value -> null-rate 0.5 > 0.0.
    val labels = Seq(
      LabelEvent("e1", 1.0, day(5)),
      LabelEvent("ghost", 1.0, day(5))
    )
    val ex = intercept[DataQualityException] {
      new Pipeline(config).run(events, labels)
    }
    ex.report.passed shouldBe false
    ex.report.failures.head.affectedEntities should contain("ghost")
    ex.getMessage should include("DATA QUALITY: FAILED")
  }

  test("runWithReport surfaces the report without throwing") {
    val config = PipelineConfig(
      featureNames = Seq("balance"),
      qualityRules = Seq(NumericRange("balance", 0.0, 1.0)) // 100.0 is out of range
    )
    val labels = Seq(LabelEvent("e1", 1.0, day(5)))
    val (rows, report) = new Pipeline(config).runWithReport(events, labels)
    rows should have size 1
    report.passed shouldBe false
  }

  test("versioned run: a newer label is routed to v2 through the full pipeline") {
    val config = PipelineConfig(featureNames = Seq("balance"), registry = registry)
    val labels = Seq(LabelEvent("e1", 1.0, day(12)))
    val row = new Pipeline(config).run(events, labels).head
    row.valueOf("balance") shouldBe Some(NumericValue(9000.0))
    row.feature("balance").flatMap(_.version) shouldBe Some(2)
  }

  test("end-to-end anti-leakage: a future feature update never enters the training row") {
    val config = PipelineConfig(featureNames = Seq("balance"))
    // Label at day(5); e1's v2 update at day(11) is the future and must not leak.
    val labels = Seq(LabelEvent("e1", 1.0, day(5)))
    val row = new Pipeline(config).run(events, labels).head
    row.feature("balance").flatMap(_.asOfTimestamp).exists(_.isAfter(day(5))) shouldBe false
  }
}
