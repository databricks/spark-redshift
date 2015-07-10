import sbt.ExclusionRule

net.virtualvoid.sbt.graph.Plugin.graphSettings

val hadoopVersion = settingKey[String]("Hadoop version")

organization := "com.databricks"

name := "spark-redshift"

version := "0.4.1-SNAPSHOT"

scalaVersion := "2.10.4"

sparkVersion := sys.props.get("spark.version").getOrElse("1.4.0")

hadoopVersion := sys.props.get("hadoop.version").getOrElse("2.6.0")

spName := "databricks/spark-redshift"

sparkComponents += "sql"

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion.value excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % hadoopVersion.value

libraryDependencies += "postgresql" % "postgresql" % "8.3-606.jdbc4"

libraryDependencies += "com.databricks" % "spark-avro_2.10" % "1.0.0"

libraryDependencies += "org.apache.avro" % "avro-mapred" % "1.7.6" classifier "hadoop2" exclude("org.mortbay.jetty", "servlet-api")

// TODO: Need a better fix for dependency hell here
libraryDependencies += "net.java.dev.jets3t" % "jets3t" % "0.9.0"

libraryDependencies += "com.google.guava" % "guava" % "14.0.1" % Test

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.5" % Test

libraryDependencies := libraryDependencies.value.map { module =>
  if (module.name.indexOf("spark-sql") >= 0) {
    module.exclude("org.apache.hadoop", "hadoop-client")
  }  else if (module.name.indexOf("spark-core") >= 0) {
    module.exclude("net.java.dev.jets3t", "jets3t")
  } else {
    module
  }
}
