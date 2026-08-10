package com.sidhartha.featurestore

import com.sidhartha.featurestore.DataQualityGate.{DataQualityException, DataQualityReport, DataQualityRule}
import com.sidhartha.featurestore.domain._

/** Configuration for a single pipeline run.
  *
  * @param featureNames  the features to attach to every training row
  * @param registry      feature-version routing (use `FeatureVersionRegistry.empty`
  *                      to disable version routing)
  * @param qualityRules  data-quality rules the output must satisfy
  */
final case class PipelineConfig(
    featureNames: Seq[String],
    registry: FeatureVersionRegistry = FeatureVersionRegistry.empty,
    qualityRules: Seq[DataQualityRule] = Seq.empty
)

/** End-to-end feature pipeline: point-in-time join -> feature versioning ->
  * data-quality gate.
  *
  * This is the orchestration layer that a Spark job's `main` would call: read
  * feature/label tables, perform the as-of join, then gate the result before it
  * is written to the training store.
  */
final class Pipeline(config: PipelineConfig) {

  private val joiner = new PointInTimeJoiner(config.registry)

  /** Build training rows without evaluating the quality gate. Useful for
    * inspection and testing.
    */
  def buildTrainingRows(
      featureEvents: Seq[FeatureEvent],
      labelEvents: Seq[LabelEvent]
  ): Seq[TrainingRow] =
    joiner.join(featureEvents, labelEvents, config.featureNames)

  /** Build rows and evaluate the gate, returning both without throwing. */
  def runWithReport(
      featureEvents: Seq[FeatureEvent],
      labelEvents: Seq[LabelEvent]
  ): (Seq[TrainingRow], DataQualityReport) = {
    val rows = buildTrainingRows(featureEvents, labelEvents)
    val report = DataQualityGate.evaluate(rows, config.qualityRules)
    (rows, report)
  }

  /** Run the full pipeline. Returns the training rows on success; throws
    * [[DataQualityException]] (carrying the diagnostic report) if the gate fails,
    * so a bad run aborts loudly instead of emitting corrupt training data.
    */
  def run(
      featureEvents: Seq[FeatureEvent],
      labelEvents: Seq[LabelEvent]
  ): Seq[TrainingRow] = {
    val (rows, report) = runWithReport(featureEvents, labelEvents)
    if (!report.passed) throw new DataQualityException(report)
    rows
  }
}
