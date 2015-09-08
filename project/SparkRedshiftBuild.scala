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
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import com.typesafe.sbt.pgp._
import bintray.BintrayPlugin.autoImport._

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
      scalaVersion := "2.10.4",
      crossScalaVersions := Seq("2.10.5", "2.11.7"),
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
        "Spark 1.5.0 RC3 Staging" at "https://repository.apache.org/content/repositories/orgapachespark-1143",
      libraryDependencies ++= Seq(
        "org.slf4j" % "slf4j-api" % "1.7.5",
        // These Amazon SDK depdencies are marked as 'provided' in order to reduce the risk of
        // dependency conflicts with other user libraries. In many environments, such as EMR and
        // Databricks, the Amazon SDK will already be on the classpath. In other cases, the SDK is
        // likely to be provided via a dependency on the S3NativeFileSystem. If this was not marked
        // as provided, then we would have to worry about the SDK's own dependencies evicting
        // earlier versions of those dependencies that are required by the end user's own code.
        // There's a trade-off here and we've chosen to err on the side of minimizing dependency
        // conflicts for a majority of users while adding a minor inconvienece (adding one extra
        // depenendecy by hand) for a smaller set of users.
        "com.amazonaws" % "aws-java-sdk-core" % "1.9.40" % "provided",
        "com.amazonaws" % "aws-java-sdk-s3" % "1.9.40" % "provided",
        "com.amazonaws" % "aws-java-sdk-sts" % "1.9.40" % "test",
        // We require spark-avro, but avro-mapred must be provided to match Hadoop version.
        // In most cases, avro-mapred will be provided as part of the Spark assembly JAR.
        "com.databricks" %% "spark-avro" % "2.0.1",
        if (testHadoopVersion.value.startsWith("1")) {
          "org.apache.avro" % "avro-mapred" % "1.7.7" % "provided" classifier "hadoop1" exclude("org.mortbay.jetty", "servlet-api")
        } else {
          "org.apache.avro" % "avro-mapred" % "1.7.7" % "provided" classifier "hadoop2" exclude("org.mortbay.jetty", "servlet-api")
        },
        // A Redshift-compatible JDBC driver must be present on the classpath for spark-redshift to work.
        // For testing, we use an Amazon driver, which is available from
        // http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html
        "com.amazon.redshift" % "jdbc4" % "1.1.7.1007" % "test" from "https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC4-1.1.7.1007.jar",
        "com.google.guava" % "guava" % "14.0.1" % "test",
        "org.scalatest" %% "scalatest" % "2.2.1" % "test",
        "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % "test",
        "org.mockito" % "mockito-core" % "1.10.19" % "test"
      ),
      libraryDependencies ++= (if (testHadoopVersion.value.startsWith("1")) {
        Seq(
          "org.apache.hadoop" % "hadoop-client" % testHadoopVersion.value % "test" force(),
          "org.apache.hadoop" % "hadoop-test" % testHadoopVersion.value % "test" force()
        )
      } else {
        Seq(
          "org.apache.hadoop" % "hadoop-client" % testHadoopVersion.value % "test" exclude("javax.servlet", "servlet-api") force(),
          "org.apache.hadoop" % "hadoop-common" % testHadoopVersion.value % "test" exclude("javax.servlet", "servlet-api") force(),
          "org.apache.hadoop" % "hadoop-common" % testHadoopVersion.value % "test" classifier "tests" force()
        )
      }),
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client") force(),
        "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client") force(),
        "org.apache.spark" %% "spark-hive" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client") force()
      ),
      // Although spark-avro declares its avro-mapred dependency as `provided`, its version of the
      // dependency can still end up on the classpath during tests, which breaks the tests for
      // Hadoop 1.x. To work around this, we filter out the incompatible JARs here:
      (fullClasspath in Test) := (if (testHadoopVersion.value.startsWith("1")) {
        (fullClasspath in Test).value.filterNot {
          x => x.data.getName.contains("hadoop2") && x.data.getName.contains("avro")
        }
      } else {
        (fullClasspath in Test).value.filterNot {
          x => x.data.getName.contains("hadoop1") && x.data.getName.contains("avro")
        }
      }),
      ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
        if (scalaBinaryVersion.value == "2.10") false
        else true
      },
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
        <licenses>
          <license>
            <name>Apache License, Verision 2.0</name>
            <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
            <distribution>repo</distribution>
          </license>
        </licenses>
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
            <id>joshrosen</id>
            <name>Josh Rosen</name>
            <url>https://github.com/joshrosen</url>
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
        releaseStepTask(spPublish),
        setNextVersion,
        commitNextVersion,
        pushChanges
      )
    )
}
