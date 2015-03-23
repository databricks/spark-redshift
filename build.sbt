net.virtualvoid.sbt.graph.Plugin.graphSettings

organization := "com.databricks.examples.redshift"

name := "redshift-input-format"

version := "0.1"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.0.4"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.2.0" exclude ("org.apache.hadoop", "hadoop-client")

libraryDependencies += "com.google.guava" % "guava" % "14.0.1" % Test

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.5" % Test
