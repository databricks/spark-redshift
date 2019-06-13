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

import scala.math.Ordering.Implicits._
import org.scalastyle.sbt.ScalastylePlugin.rawScalastyleSettings
import sbt._
import sbt.Keys._
import sbtsparkpackage.SparkPackagePlugin.autoImport._
import scoverage.ScoverageKeys
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import com.typesafe.sbt.pgp._
import bintray.BintrayPlugin.autoImport._

object SparkRedshiftBuild extends Build {
  val testSparkVersion = settingKey[String]("Spark version to test against")
  val testHadoopVersion = settingKey[String]("Hadoop version to test against")
  val testAWSJavaSDKVersion = settingKey[String]("AWS Java SDK version to test against")

  // Define a custom test configuration so that unit test helper classes can be re-used under
  // the integration tests configuration; see http://stackoverflow.com/a/20635808.
  lazy val IntegrationTest = config("it") extend Test

  lazy val root = Project("spark-redshift", file("."))
    .configs(IntegrationTest)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(Project.inConfig(IntegrationTest)(rawScalastyleSettings()): _*)
    .settings(Defaults.coreDefaultSettings: _*)
    .settings(Defaults.itSettings: _*)
    .settings(
      name := "spark-redshift",
      organization := "com.databricks",
      scalaVersion := "2.11.12",
      sparkVersion := "2.4.3",
      testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value),

      // Spark 2.4.x should be compatible with hadoop >= 2.7.x
      // https://spark.apache.org/downloads.html
      testHadoopVersion := sys.props.get("hadoop.testVersion").getOrElse("2.7.7"),

      // DON't UPGRADE AWS-SDK-JAVA if not compatible with hadoop version
      // https://stackoverflow.com/a/49510602/2544874 
      // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/2.7.7
      testAWSJavaSDKVersion := sys.props.get("aws.testVersion").getOrElse("1.7.4"),
      
      spName := "databricks/spark-redshift",
      sparkComponents ++= Seq("sql", "hive"),
      spIgnoreProvided := true,
      licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"),
      credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
      scalacOptions ++= Seq("-target:jvm-1.8"),
      javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
      libraryDependencies ++= Seq(
        "org.slf4j" % "slf4j-api" % "1.7.5",
        "com.eclipsesource.minimal-json" % "minimal-json" % "0.9.4",
        
        // A Redshift-compatible JDBC driver must be present on the classpath for spark-redshift to work.
        // For testing, we use an Amazon driver, which is available from
        // http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html
        "com.amazon.redshift" % "jdbc41" % "1.2.27.1051" % "test" from "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.27.1051/RedshiftJDBC41-no-awssdk-1.2.27.1051.jar",

        "com.google.guava" % "guava" % "14.0.1" % "test",
        "org.scalatest" %% "scalatest" % "3.0.5" % "test",
        "org.mockito" % "mockito-core" % "1.10.19" % "test",

        "com.amazonaws" % "aws-java-sdk" % testAWSJavaSDKVersion.value % "provided" excludeAll  
          (ExclusionRule(organization = "com.fasterxml.jackson.core")),

        "org.apache.hadoop" % "hadoop-client" % testHadoopVersion.value % "test" exclude("javax.servlet", "servlet-api") force(),
        "org.apache.hadoop" % "hadoop-common" % testHadoopVersion.value % "test" exclude("javax.servlet", "servlet-api") force(),
        "org.apache.hadoop" % "hadoop-common" % testHadoopVersion.value % "test" classifier "tests" force(),

        "org.apache.hadoop" % "hadoop-aws" % testHadoopVersion.value excludeAll
          (ExclusionRule(organization = "com.fasterxml.jackson.core"))
          exclude("org.apache.hadoop", "hadoop-common")
          exclude("com.amazonaws", "aws-java-sdk-s3")  force(),

        "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client") force(),
        "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client") force(),
        "org.apache.spark" %% "spark-hive" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client") force(),
        "org.apache.spark" %% "spark-avro" % testSparkVersion.value % "test" exclude("org.apache.avro", "avro-mapred") force()
      ),
      ScoverageKeys.coverageHighlighting := true,
      logBuffered := false,
      // Display full-length stacktraces from ScalaTest:
      testOptions in Test += Tests.Argument("-oF"),
      fork in Test := true,
      javaOptions in Test ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M"),

      /********************
       * Release settings *
       ********************/

      publishMavenStyle := true,
      releaseCrossBuild := true,
      licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
      releasePublishArtifactsAction := PgpKeys.publishSigned.value,

      pomExtra :=
        <url>https://github.com/databricks/spark-redshift</url>
        <scm>
          <url>git@github.com:databricks/spark-redshift.git</url>
          <connection>scm:git:git@github.com:databricks/spark-redshift.git</connection>
        </scm>
        <developers>
          <developer>
            <id>meng</id>
            <name>Xiangrui Meng</name>
            <url>https://github.com/mengxr</url>
          </developer>
          <developer>
            <id>JoshRosen</id>
            <name>Josh Rosen</name>
            <url>https://github.com/JoshRosen</url>
          </developer>
          <developer>
            <id>marmbrus</id>
            <name>Michael Armbrust</name>
            <url>https://github.com/marmbrus</url>
          </developer>
        </developers>,

      bintrayReleaseOnPublish in ThisBuild := false,

      // Add publishing to spark packages as another step.
      releaseProcess := Seq[ReleaseStep](
        checkSnapshotDependencies,
        inquireVersions,
        runTest,
        setReleaseVersion,
        commitReleaseVersion,
        tagRelease,
        publishArtifacts,
        setNextVersion,
        commitNextVersion,
        pushChanges
      )
    )
}
