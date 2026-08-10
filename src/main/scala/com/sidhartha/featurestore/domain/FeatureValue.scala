package com.sidhartha.featurestore.domain

/** A typed feature value.
  *
  * Modeling values as a sealed ADT (rather than `Any`) keeps the pipeline
  * type-safe and lets the data-quality gate reason about numeric ranges vs.
  * categorical values explicitly. In a Spark job this maps to strongly typed
  * columns in a `Dataset[Row]` / `StructType` schema.
  */
sealed trait FeatureValue extends Product with Serializable {

  /** The numeric payload if this is a [[NumericValue]], otherwise `None`. */
  def asNumeric: Option[Double] = this match {
    case NumericValue(v) => Some(v)
    case _               => None
  }

  /** A human-readable rendering used in diagnostic reports. */
  def render: String = this match {
    case NumericValue(v)     => v.toString
    case CategoricalValue(v) => v
  }
}

/** A continuous / numeric feature value (e.g. `account_balance = 1234.56`). */
final case class NumericValue(value: Double) extends FeatureValue

/** A discrete / categorical feature value (e.g. `plan_tier = "gold"`). */
final case class CategoricalValue(value: String) extends FeatureValue

object FeatureValue {
  def num(v: Double): FeatureValue = NumericValue(v)
  def cat(v: String): FeatureValue = CategoricalValue(v)
}
