net.virtualvoid.sbt.graph.Plugin.graphSettings

val hadoopVersion = settingKey[String]("Hadoop version")

organization := "com.databricks"

name := "spark-redshift"

version := "0.4.1-SNAPSHOT"

scalaVersion := "2.10.4"

sparkVersion := sys.props.get("spark.version").getOrElse("1.4.0")

hadoopVersion := sys.props.get("hadoop.version").getOrElse("2.2.0")

spName := "databricks/spark-redshift"

sparkComponents += "sql"

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion.value % "provided" exclude("org.mortbay.jetty", "javax.servlet")

libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.6.0" % "provided"

libraryDependencies += "com.databricks" % "spark-avro_2.10" % "1.0.0"

// TODO: Should be provided? This seems to be needed to make spark-avro work.
libraryDependencies += "org.apache.avro" % "avro-mapred" % "1.7.6" classifier "hadoop2" exclude("org.mortbay.jetty", "servlet-api")

// TODO: Need a better fix for dependency hell here
libraryDependencies += "net.java.dev.jets3t" % "jets3t" % "0.9.0"

// A Redshift-compatible JDBC driver must be present on the classpath for spark-redshift to work.
// For testing, we using a Postgres driver, but it is recommended that the Amazon driver is used
// in production. See http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html
libraryDependencies += "postgresql" % "postgresql" % "8.3-606.jdbc4" % "provided"

libraryDependencies += "com.google.guava" % "guava" % "14.0.1" % Test

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.5" % Test

libraryDependencies := libraryDependencies.value.map { module =>
  if (module.name.indexOf("spark-sql") >= 0) {
    module.exclude("org.apache.hadoop", "hadoop-client")
  } else {
    module
  }
}

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion.value

libraryDependencies += "com.google.guava" % "guava" % "14.0.1" % Test

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.5" % Test

ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else false
}
