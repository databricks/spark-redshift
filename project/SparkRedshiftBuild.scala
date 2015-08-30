/*
 * Copyright 2015 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.scalastyle.sbt.ScalastylePlugin.rawScalastyleSettings
import sbt._
import sbt.Keys._
import sbtsparkpackage.SparkPackagePlugin.autoImport._
import scoverage.ScoverageSbtPlugin

object SparkRedshiftBuild extends Build {
  val testSparkVersion = settingKey[String]("Spark version to test against")
  val testHadoopVersion = settingKey[String]("Hadoop version to test against")

  // Define a custom test configuration so that unit test helper classes can be re-used under
  // the integration tests configuration; see http://stackoverflow.com/a/20635808.
  lazy val IntegrationTest = config("it") extend Test

  lazy val root = Project("spark-redshift", file("."))
    .configs(IntegrationTest)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(Project.inConfig(IntegrationTest)(rawScalastyleSettings()): _*)
    .settings(Defaults.itSettings: _*)
    .settings(
      name := "spark-redshift",
      organization := "com.databricks",
      version := "0.4.1-SNAPSHOT",
      scalaVersion := "2.10.4",
      sparkVersion := "1.4.1",
      testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value),
      testHadoopVersion := sys.props.get("hadoop.testVersion").getOrElse("2.2.0"),
      spName := "databricks/spark-redshift",
      sparkComponents ++= Seq("sql", "hive"),
      spIgnoreProvided := true,
      licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"),
      credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
      resolvers +=
        "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      resolvers +=
        "Spark 1.5.0 RC2 Staging" at "https://repository.apache.org/content/repositories/orgapachespark-1141",
      libraryDependencies ++= Seq(
        "com.amazonaws" % "aws-java-sdk-core" % "1.9.40" % "provided",
        "com.amazonaws" % "aws-java-sdk-s3" % "1.9.40" % "provided",
        // We require spark-avro, but avro-mapred must be provided to match Hadoop version:
        "com.databricks" %% "spark-avro" % "1.0.0",
        "org.apache.avro" % "avro-mapred" % "1.7.6" % "provided" exclude("org.mortbay.jetty", "servlet-api"),
        // A Redshift-compatible JDBC driver must be present on the classpath for spark-redshift to work.
        // For testing, we use an Amazon driver, which is available from
        // http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html
        "com.amazon.redshift" % "jdbc4" % "1.1.7.1007" % "test" from "https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC4-1.1.7.1007.jar",
        "com.google.guava" % "guava" % "14.0.1" % "test",
        "org.scalatest" %% "scalatest" % "2.2.1" % "test",
        "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % "test"
      ),
      libraryDependencies ++= Seq(
        "org.apache.hadoop" % "hadoop-client" % testHadoopVersion.value % "test",
        "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client"),
        "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client"),
        "org.apache.spark" %% "spark-hive" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client")
      ),
      ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
        if (scalaBinaryVersion.value == "2.10") false
        else false
      },
      logBuffered := false,
      // Display full-length stacktraces from ScalaTest:
      testOptions in Test += Tests.Argument("-oF")
    )
}
