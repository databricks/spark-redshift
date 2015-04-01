RedshiftInputFormat
===

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
