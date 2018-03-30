
retrieveManaged := true

lazy val root = Project("spark-redshift", file("."))
  //.settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
  .settings(Defaults.coreDefaultSettings: _*)
  .settings(
    name := "spark-redshift",
    organization := "com.databricks",
    scalaVersion := "2.11.12",
    licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"),
    // scalacOptions ++= Seq("-target:jvm-1.6"),
    // javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % "1.7.5",
      "com.eclipsesource.minimal-json" % "minimal-json" % "0.9.4",
      // We require spark-avro, but avro-mapred must be provided to match Hadoop version.
      // In most cases, avro-mapred will be provided as part of the Spark assembly JAR.
      "com.databricks" %% "spark-avro" % "4.0.0",

      "org.apache.avro" % "avro-mapred" % "1.7.7" % "provided" classifier "hadoop2" exclude("org.mortbay.jetty", "servlet-api"),

      // Kryo is provided by Spark, but we need this here in order to be able to import KryoSerializable
      "com.esotericsoftware" % "kryo-shaded" % "3.0.3" % "provided",
      // A Redshift-compatible JDBC driver must be present on the classpath for spark-redshift to work.
      // For testing, we use an Amazon driver, which is available from
      // http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html
      "com.amazon.redshift" % "jdbc4" % "1.2.10.1009" from "https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC4-1.2.10.1009.jar",

      // Although support for the postgres driver is lower priority than support for Amazon's
      // official Redshift driver, we still run basic tests with it.
      "org.postgresql" % "postgresql" % "42.1.4",

      "com.amazonaws" % "aws-java-sdk" % "1.7.4" exclude("com.fasterxml.jackson.core", "jackson-databind"),

      "org.apache.spark" %% "spark-core" % "2.2.0",
      "org.apache.spark" %% "spark-sql" % "2.2.0",
      "org.apache.spark" %% "spark-hive" % "2.2.0",

      "com.google.guava" % "guava" % "14.0.1" % "test",
      "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      "org.mockito" % "mockito-core" % "1.10.19" % "test"
    ),

    logBuffered := false)

