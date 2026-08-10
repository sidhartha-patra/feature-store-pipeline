ThisBuild / organization := "com.sidhartha"
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "feature-store-pipeline",
    // Pure-Scala implementation on purpose: the pipeline models Spark
    // Dataset/DataFrame operations with plain Scala collections so that it
    // stays fast, deterministic and free of native Hadoop/winutils flakiness.
    // See README.md ("Mapping to a real Spark job") for the intended mapping.
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint",
      "-Wunused:imports"
    ),
    Test / parallelExecution := false
  )
