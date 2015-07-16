RedshiftInputFormat
===

[![Build Status](https://travis-ci.org/databricks/spark-redshift.svg?branch=master)](https://travis-ci.org/databricks/spark-redshift)
[![codecov.io](http://codecov.io/github/databricks/spark-redshift/coverage.svg?branch=master)](http://codecov.io/github/databricks/spark-redshift?branch=master)

Install
-------

You may use this library in your applications through:

### spark-shell, pyspark, or spark-submit (with Spark 1.3)

```
> $SPARK_HOME/bin/spark-shell --packages databricks:spark-redshift:0.3
```

### sbt

If you use the sbt-spark-package plugin, in your sbt build file, add:

```
spDependencies += "databricks/spark-redshift:0.3"
```

Otherwise,
```
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "databricks" % "spark-redshift" % "0.3"
```

### Maven

In your pom.xml, add:
```
<dependencies>
  <!-- list of dependencies -->
  <dependency>
    <groupId>databricks</groupId>
    <artifactId>spark-redshift</artifactId>
    <version>0.3</version>
  </dependency>
</dependencies>
<repositories>
  <!-- list of other repositories -->
  <repository>
    <id>SparkPackagesRepo</id>
    <url>http://dl.bintray.com/spark-packages/maven</url>
  </repository>
</repositories>
```

Usage
-----

Hadoop input format for Redshift tables unloaded with the ESCAPE option.

Usage in Spark Core:
```scala
import com.databricks.spark.redshift.RedshiftInputFormat

val records = sc.newAPIHadoopFile(
  path,
  classOf[RedshiftInputFormat],
  classOf[java.lang.Long],
  classOf[Array[String]])
```

Usage in Spark SQL:
```scala
import com.databricks.spark.redshift._

// Call redshiftFile() that returns a SchemaRDD with all string columns.
val records: DataFrame = sqlContext.redshiftFile(path, Seq("name", "age"))

// Call redshiftFile() with the table schema.
val records: DataFrame = sqlContext.redshiftFile(path, "name varchar(10) age integer")
```

Migration Guide
---------------

Some breaking changes were made in version 0.3. Users should make the following changes in their 
code if they would like to use the 0.3 version:

 * `com.databricks.examples.redshift.input` -> `com.databricks.spark.redshift`
 * `SchemaRDD` -> `DataFrame`
 * `import com.databricks.examples.redshift.input.RedshiftInputFormat._` -> `import com.databricks.spark.redshift._`
