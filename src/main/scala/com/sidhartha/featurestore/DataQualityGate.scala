package com.sidhartha.featurestore

import com.sidhartha.featurestore.domain._

import java.time.Duration

/** Configurable data-quality gate that runs over produced [[TrainingRow]]s and
  * fails the pipeline loudly on violation.
  *
  * A feature pipeline that silently emits bad training data is worse than one
  * that crashes: the model trains, ships, and quietly underperforms. This gate
  * turns silent data corruption into a loud, diagnosable failure. Each rule
  * produces a [[RuleResult]] naming exactly which entities/rows it flagged, and
  * the [[Pipeline]] aborts with a [[DataQualityException]] whose message is the
  * rendered report.
  */
object DataQualityGate {

  /** A single validation rule over the assembled training rows. */
  sealed trait DataQualityRule {

    /** Short, stable identifier used in diagnostics. */
    def name: String

    /** Evaluate the rule against all training rows. */
    def evaluate(rows: Seq[TrainingRow]): RuleResult
  }

  /** Outcome of evaluating one [[DataQualityRule]]. */
  final case class RuleResult(
      rule: String,
      passed: Boolean,
      message: String,
      affectedEntities: Seq[String] = Seq.empty
  )

  /** Aggregate report across all rules. */
  final case class DataQualityReport(results: Seq[RuleResult]) {

    /** The gate passes only if every rule passed. */
    val passed: Boolean = results.forall(_.passed)

    def failures: Seq[RuleResult] = results.filterNot(_.passed)

    /** Human-readable, multi-line diagnostic suitable for logs / exceptions. */
    def render: String = {
      val header =
        if (passed) "DATA QUALITY: PASSED"
        else s"DATA QUALITY: FAILED (${failures.size} of ${results.size} rules violated)"
      val lines = results.map { r =>
        val status = if (r.passed) "PASS" else "FAIL"
        val affected =
          if (r.affectedEntities.isEmpty) ""
          else s" affected=[${r.affectedEntities.mkString(", ")}]"
        s"  [$status] ${r.rule}: ${r.message}$affected"
      }
      (header +: lines).mkString("\n")
    }
  }

  /** Thrown by the pipeline when the gate fails. Carries the full report. */
  final class DataQualityException(val report: DataQualityReport)
      extends RuntimeException(report.render)

  // ---------------------------------------------------------------------------
  // Rules
  // ---------------------------------------------------------------------------

  /** Fail if the fraction of rows with a missing value for `featureName`
    * exceeds `maxNullRate` (in `[0.0, 1.0]`).
    */
  final case class NullRateThreshold(featureName: String, maxNullRate: Double)
      extends DataQualityRule {
    val name = s"null_rate($featureName <= $maxNullRate)"

    def evaluate(rows: Seq[TrainingRow]): RuleResult = {
      if (rows.isEmpty)
        return RuleResult(name, passed = true, "no rows to evaluate")
      val missing = rows.filter(_.feature(featureName).forall(_.isMissing))
      val rate = missing.size.toDouble / rows.size.toDouble
      val ok = rate <= maxNullRate + Epsilon
      val msg =
        f"null-rate=${rate}%.4f (threshold ${maxNullRate}%.4f) over ${rows.size} rows"
      RuleResult(name, ok, msg, if (ok) Nil else missing.map(_.entityId).distinct)
    }
  }

  /** Fail if any present value of `featureName` is non-numeric or falls outside
    * the inclusive `[min, max]` range.
    */
  final case class NumericRange(featureName: String, min: Double, max: Double)
      extends DataQualityRule {
    val name = s"numeric_range($featureName in [$min, $max])"

    def evaluate(rows: Seq[TrainingRow]): RuleResult = {
      val offenders = rows.flatMap { row =>
        row.valueOf(featureName) match {
          case Some(NumericValue(v)) if v < min || v > max => Some(row.entityId -> v.toString)
          case Some(CategoricalValue(v))                   => Some(row.entityId -> s"non-numeric:$v")
          case _                                           => None
        }
      }
      val ok = offenders.isEmpty
      val msg =
        if (ok) s"all present values within [$min, $max]"
        else s"${offenders.size} value(s) out of range: " +
          offenders.take(MaxSamples).map { case (e, v) => s"$e=$v" }.mkString(", ")
      RuleResult(name, ok, msg, offenders.map(_._1).distinct)
    }
  }

  /** Fail if the value joined for `featureName` is staler than `maxStaleness`
    * relative to the label timestamp (i.e. the freshest known value at the
    * as-of point is too old to be trustworthy). Missing values are ignored here
    * (that concern belongs to [[NullRateThreshold]]).
    */
  final case class FreshnessThreshold(featureName: String, maxStaleness: Duration)
      extends DataQualityRule {
    val name = s"freshness($featureName <= ${maxStaleness.getSeconds}s)"

    def evaluate(rows: Seq[TrainingRow]): RuleResult = {
      val maxSeconds = maxStaleness.getSeconds
      val offenders = rows.flatMap { row =>
        row.feature(featureName).flatMap { fr =>
          fr.stalenessSeconds(row.labelTimestamp) match {
            case Some(s) if s > maxSeconds => Some(row.entityId -> s)
            case _                         => None
          }
        }
      }
      val ok = offenders.isEmpty
      val msg =
        if (ok) s"all values within ${maxSeconds}s of the label"
        else s"${offenders.size} stale value(s): " +
          offenders.take(MaxSamples).map { case (e, s) => s"$e=${s}s" }.mkString(", ")
      RuleResult(name, ok, msg, offenders.map(_._1).distinct)
    }
  }

  // ---------------------------------------------------------------------------
  // Runner
  // ---------------------------------------------------------------------------

  private val Epsilon = 1e-9
  private val MaxSamples = 10

  /** Evaluate every rule and return the aggregate report (does not throw). */
  def evaluate(rows: Seq[TrainingRow], rules: Seq[DataQualityRule]): DataQualityReport =
    DataQualityReport(rules.map(_.evaluate(rows)))
}
