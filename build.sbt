net.virtualvoid.sbt.graph.Plugin.graphSettings

val hadoopVersion = settingKey[String]("Hadoop version")

organization := "com.databricks"

name := "spark-redshift"

version := "0.4.1-SNAPSHOT"

scalaVersion := "2.10.4"

sparkVersion := sys.props.get("spark.version").getOrElse("1.4.1")

hadoopVersion := sys.props.get("hadoop.version").getOrElse("2.2.0")

spName := "databricks/spark-redshift"

sparkComponents += "sql"

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-core" % "1.9.40" % "provided"

// We require spark-avro, but avro-mapred must be provided to match Hadoop version
libraryDependencies += "com.databricks" %% "spark-avro" % "1.0.0"

libraryDependencies += "org.apache.avro" % "avro-mapred" % "1.7.6" % "provided" exclude("org.mortbay.jetty", "servlet-api")

// A Redshift-compatible JDBC driver must be present on the classpath for spark-redshift to work.
// The Amazon driver is recommended for production use; it can be obtained from
// http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html

// A Redshift-compatible JDBC driver must be present on the classpath for spark-redshift to work.
// For testing, we use an Amazon driver, which is available from
// http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html
libraryDependencies += "com.amazon.redshift" % "jdbc4" % "1.1.7.1007" % "test" from "https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC4-1.1.7.1007.jar"

libraryDependencies += "com.google.guava" % "guava" % "14.0.1" % Test

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.5" % Test

libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % Test

ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else false
}
