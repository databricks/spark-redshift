# Tutorial #

The [Spark Data Sources API](https://databricks.com/blog/2015/01/09/spark-sql-data-sources-api-unified-data-access-for-the-spark-platform.html) introduced in Spark 1.2 supports a pluggable mechanism for integration with structured data-sources. It is a unified API designed to support two major operations:

1. Loading structured data from an external data source into Spark
2. Storing structured data from Spark into an external data source.

The Data Sources API has built in integration for several data sources such as Hive, Avro, JSON, JDBC and Parquet. Third party integration is added through spark-packages. One such integration is for Amazon Redshift which is provided by the `spark-redshift` package. 

Prior to the introduction of `spark-redshift`, JDBC was the only way for Spark users to read data from Redshift. While this method is adequate when running queries returning a small number of rows (order of 100’s), it is too slow when handling large scale data. This is because, JDBC provides a ResultSet based approach where rows are retrieved in a single thread in small batches. Furthermore, the use of JDBC to store large datasets in Redshift is only practical when data needs to be moved between tables inside a Redshift database. The JDBC based INSERT/UPDATE queries are only practical for small updates to Redshift tables. For users hoping to load or store large volumes of data from/to Redshift, JDBC leaves much to be desired in terms of performance and throughput.

This tutorial will provide a hand-on experience in using the `spark-redshift` package from your local development environment. It will also provide a deep dive into the implementation details of `spark-redshift` which will enable you to gain a deeper understanding of why `spark-redshift` provides a high performance alternative to a plain JDBC based approach to interacting with Redshift from Spark.

## Prepare the Redshift database ##

Before we delve into specific examples of how `spark-redshift` works, let us configure the Redshift database which we will be using.

In this tutorial, we will use the sample [TICKT](http://docs.aws.amazon.com/redshift/latest/dg/c_sampledb.html) database on Redshift. This database enables the tracking of  sales activity for the fictional TICKIT web site, where users buy and sell tickets online for various types of events. The database allows analysis of sales over time, performance of sellers, venues and correlation of sales with seasons. This information can be used to drive advertising and promotions campaigns.

When you start the Redshift service you will first need to create the TICKT database and load it. Follow the instructions [here](http://docs.aws.amazon.com/redshift/latest/dg/cm-dev-t-load-sample-data.html) to create/load the TICKT database.

In the examples below, we used a Redshift database running on a 2 node cluster. Each node manages 2 slices for a total of 4 slices. This *usually* (read Redshift [documentation](http://docs.aws.amazon.com/redshift/latest/dg/c_high_level_system_architecture.html) for a more nuanced discussion) means that each table will be stored in 4 separate partitions, one for each slice.

## Usage ##

We are ready to interact with Redshift using the `spark-redshift` library. The skeleton of the program we will be using is shown below. The entire `SparkRedshiftTutorial.scala` program can be accessed from [here](SparkRedshiftTutorial.scala). You can also use the Spark REPL to run the lines listed in the program below.

```scala
object SparkRedshiftTutorial {
  def main(args:Array[String]): Unit = {

    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)
    val redshiftDBName = args(2)
    val redshiftUserId = args(3)
    val redshiftPassword = args(4)
    val redshifturl = args(5)
    val jdbcURL = s"jdbc:redshift://$redshifturl/$redshiftDBName?user=$redshiftUserId&password=$redshiftPassword"

    val sc = new SparkContext(new SparkConf().setAppName("SparkSQL").setMaster("local"))

	// Configure SparkContext to communicate with AWS
	val tempS3Dir = "s3n://redshift-spark/temp/"
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)

	// Create the SQL Context
    val sqlContext = new SQLContext(sc)
  }
}
```

We need the following set of user provided parameters to communicate with AWS in general and Redshift in particular -

- **AWS Access Key and AWS Secret Access Key** - This key pair will be used to communicate with AWS services. This information is passed by the AWS Client libraries in every interaction with AWS.
- **Redshift Database Name** - When you provision the Redshift service you have to provide a name for your database. This is similar to a schema in Oracle. The name of our Redshift database was `sparkredshift`
- **Redshift UserId/Password combination** - You will need to provide this information when the Redshift service is provisioned.
- **Redshift URL** - You will need to obtain this from your Redshift Console. A sample Redshift URL is `swredshift.czac2vcs84ci.us-east-1.redshift.amazonaws.com:5439`

Your Redshift console will provide the JDBC URL to use. It follows the pattern

```
jdbc:redshift://$redshifturl/$redshiftDBName?user=$redshiftUserId&password=$redshiftPassword
```

A sample JDBC URL is

```
jdbc:redshift://swredshift.czac2vcs84ci.us-east-1.redshift.amazonaws.com:5439/sparkredshift?user=spark&password=mysecretpass
```

`spark-redshift` reads and writes data to S3 when transferring data from/to Redshift, so you'll need to specify a path in S3 where the library should write these temporary files. `spark-redshift` cannot automatically clean up the temporary files it creates in S3. As a result, we recommend that you use a dedicated temporary S3 bucket with an [object lifecycle configuration ](http://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html) to ensure that temporary files are automatically deleted after a specified expiration period. For this example we create a S3 bucket `redshift-spark`. We tell `spark-redshift` that we will use the following temporary location in S3 to store temporary files generated by `spark-redshift` `s3n://redshift-spark/temp/`

Next enable the communication with AWS by configuring the following properties in the `SparkContext` instance:

- fs.s3n.awsAccessKeyId
- fs.s3n.awsSecretAccessKey

Lastly we create the `SQLContext` to use the Data Sources API to communicate with Redshift.

```scala
val sqlContext = new SQLContext(sc)
```

### Load Function - Reading from a Redshift table ###

Let us fetch data from the Redshift table `event`. Add the following lines of code to the skeleton listed above:

```scala
import sqlContext.implicits._
val eventsDF = sqlContext.read
	.format("com.databricks.spark.redshift")
	.option("url",jdbcURL )
	.option("tempdir", tempS3Dir)
	.option("dbtable", "event")
	.load()
eventsDF.show()
```


`.format("com.databricks.spark.redshift")` line tells the Data Sources API that we are using the `spark-redshift` package. It uses this information to identify the class `DefaultSource` in the package specified by the `format` invocation. This class contains the entry points for the functionality provided by the Data Sources API implementation.

Next we provide the parameters necessary to read the `event` table from Redshift. We provide the JDBC URL, the temporary S3 folder where the table data will be copied to, and the name of the table we want to read. A comprehensive list of parameters is listed on the `spark-redshift` documentation [page](https://github.com/databricks/spark-redshift).

Executing the above lines will produce the following output:

```{r, engine='bash'}
+-------+-------+-----+------+------------------+--------------------+
|eventid|venueid|catid|dateid|         eventname|           starttime|
+-------+-------+-----+------+------------------+--------------------+
|   1433|    248|    6|  1827|            Grease|2008-01-01 19:00:...|
|   2811|    207|    7|  1827|  Spring Awakening|2008-01-01 15:00:...|
|   4135|     16|    9|  1827|               Nas|2008-01-01 14:30:...|
|   5807|     45|    9|  1827| Return To Forever|2008-01-01 15:00:...|
|   1738|    260|    6|  1828|      Beatles LOVE|2008-01-02 20:00:...|
|   2131|    212|    7|  1828|           Macbeth|2008-01-02 15:00:...|
|   2494|    203|    7|  1828|     The Caretaker|2008-01-02 19:00:...|
|   2824|    248|    7|  1828|  Cirque du Soleil|2008-01-02 19:00:...|
|   2920|    209|    7|  1828|           Macbeth|2008-01-02 19:30:...|
|   4853|     39|    9|  1828|        Rick Braun|2008-01-02 19:00:...|
|    394|    300|    8|  1829|Adriana Lecouvreur|2008-01-03 15:00:...|
|   2043|    217|    7|  1829|       The Bacchae|2008-01-03 19:00:...|
|   5508|     55|    9|  1829|        Ryan Adams|2008-01-03 19:30:...|
|   6071|     47|    9|  1829|      3 Doors Down|2008-01-03 15:00:...|
|   6120|    130|    9|  1829|         Bob Dylan|2008-01-03 19:30:...|
|   7468|     37|    9|  1829|        Commodores|2008-01-03 19:00:...|
|   1567|    257|    6|  1830|    Blue Man Group|2008-01-04 15:00:...|
|   1764|    262|    6|  1830|        Mamma Mia!|2008-01-04 20:00:...|
|   1981|    219|    7|  1830|         King Lear|2008-01-04 15:00:...|
|   2274|    222|    7|  1830|     The Caretaker|2008-01-04 20:00:...|
+-------+-------+-----+------+------------------+--------------------+
only showing top 20 rows
```

`spark-redshift` automatically reads the schema from the Redshift table and maps its types back to Spark SQL's types. The command `eventsDF.printSchema()` produces the following output:

```{r, engine='bash'}
root
 |-- eventid: integer (nullable = true)
 |-- venueid: integer (nullable = true)
 |-- catid: integer (nullable = true)
 |-- dateid: integer (nullable = true)
 |-- eventname: string (nullable = true)
 |-- starttime: timestamp (nullable = true)
```

We can even register the `DataFrame` as a temporary table in Spark and execute queries against it as follows:

```scala
eventsDF.registerTempTable("myevent")
val myEventDF = sqlContext.sql("SELECT * FROM myevent")
```

While the above examples used Scala we could have also used SQL as follows:

```sql
CREATE TEMPORARY TABLE myevent
USING com.databricks.spark.redshift
OPTIONS (
  dbtable 'event',
  tempdir 's3n://redshift-spark/temp/',
  url 'jdbc:redshift://swredshift.czac2vcs84ci.us-east-1.redshift.amazonaws.com:5439/sparkredshift?user=spark&password=mysecretpass'
);
SELECT * FROM myevent;
```

Note, we have registered a temporary table `myevent` in Spark and executed a query against it (`select * from myevent`) just like we did in our Scala example.

### Load Function - Reading from a Redshift query ###

We can also read from a Redshift query. If we need to read the most recent 10000 records from the `sales` table we will execute the following lines

```scala
val salesQuery = """
    SELECT salesid, listid, sellerid, buyerid,
           eventid, dateid, qtysold, pricepaid,
           commission 
    FROM sales 
    ORDER BY saletime DESC LIMIT 10000"""
val salesDF = sqlContext.read
    .format("com.databricks.spark.redshift")
    .option("url", jdbcURL) 
    .option("tempdir", tempS3Dir) 
    .option("query", salesQuery)
    .load()
salesDF.show()
```

Notice that the line `.option("dbtable", "event")` is replaced with the line `.option("query", salesQuery)`. This will result in the following output

```{r, engine='bash'}
+-------+------+--------+-------+-------+------+-------+---------+----------+
|salesid|listid|sellerid|buyerid|eventid|dateid|qtysold|pricepaid|commission|
+-------+------+--------+-------+-------+------+-------+---------+----------+
| 102373|117133|   26190|  35491|   2141|  2191|      4|  1008.00|    151.20|
|  75861| 86640|   43402|  39545|   8372|  2191|      2|   372.00|     55.80|
|  56302| 63546|    5760|   5797|   1489|  2191|      2|   372.00|     55.80|
|  83603| 95341|   27027|  27881|   6034|  2191|      1|   288.00|     43.20|
|  40652| 45468|   27557|  28366|   5099|  2191|      2|   510.00|     76.50|
| 157586|206655|   13213|  47394|   3665|  2191|      2|  4018.00|    602.70|
|  99761|114026|    7466|  21189|   5621|  2191|      2|   102.00|     15.30|
|  68467| 77797|   49538|  27029|   1489|  2191|      2|   188.00|     28.20|
|  66470| 75420|   22851|  37849|   7604|  2191|      2|   420.00|     63.00|
|  36700| 40838|   15099|  45514|   8027|  2191|      4|  1400.00|    210.00|
| 126896|145346|   18243|  10273|   6783|  2191|      2|   866.00|    129.90|
|  55416| 62529|   42223|  26273|   1489|  2191|      4|   532.00|     79.80|
|  48503| 54812|   42828|  12282|   8372|  2191|      2|    80.00|     12.00|
|   3737|  4035|   11666|  23692|   6034|  2191|      2|   302.00|     45.30|
| 154069|178359|    3897|   5815|   8168|  2191|      1|   290.00|     43.50|
|  37817| 42124|   24592|  26543|   6783|  2191|      4|  1300.00|    195.00|
|    924|   933|   48898|  25710|   1594|  2191|      3|   504.00|     75.60|
| 112232|128585|   30390|  44375|    914|  2191|      1|   126.00|     18.90|
|  97099|110826|   24730|  32758|   3331|  2190|      1|   319.00|     47.85|
|  74902| 85497|   45049|   2318|   7547|  2190|      2|   554.00|     83.10|
+-------+------+--------+-------+-------+------+-------+---------+----------+
only showing top 20 rows
```

### Under the hood - Load Function ###

In this section we will take a peek inside `spark-redshift` to understand how exactly the LOAD function works. Specifically we will look at how `event` table was read into a `DataFrame`. The LOAD is a two step process

1. [UNLOAD](http://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html) data from the Redshift table to S3
2. Consume the unloaded files in S3 via a custom InputFormat into an RDD which is then wrapped as a `DataFrame` using the schema obtained from Redshift.

#### UNLOAD Redshift to S3 ####

The following diagram shows the steps that are performed when Spark Redshift UNLOADs data from Redshift to S3.

![](images/loadunloadstep.png)


First, the Spark Driver communicates with the Redshift Leader node to obtain the schema of the table (or query) requested. The attribute, `override lazy val schema: StructType` in the class, `com.databricks.spark.redshift.RedshiftRelation` obtains the schema on demand by invoking the method, `resolveTable` of the class, `com.databricks.spark.redshift.JDBCWrapper` which is responsible for fetching the schema from the Redshift Leader.

Next a Redshift [UNLOAD](http://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html) query is created using the schema information obtained. The UNLOAD command unloads each slice into a S3 folder (`22c365b4-13cb-40fd-b4d6-d0ac5d426551`) created in the temporary S3 location (`s3n://spark-redshift/temp/`) provided by the user. Each file contains a row per line and each column of the row is pipe (`|`) delimited. This process occurs in parallel for each slice. The `spark-redshift` library achieves its high performance through this mechanism.

#### Read UNLOAD'ed S3 files into a DataFrame instance ####

The diagram below shows how the files unloaded in S3 are consumed to form a `DataFrame`.

![](images/loadreadstep.png)

Once the files are written to S3, a custom InputFormat implemented in the class, `com.databricks.spark.redshift.RedshiftInputFormat` is used to consume the files in parallel. This class is similar to the standard and well known Hadoop class, `TextInputFormat` where the key is the byte offset of the start of each line in the file. The value class however, is of type `Array[String]` (unlike, `TextInputFormat`where its type is `Text`). The value instance is created by splitting the lines using the default delimiter `|`. The `RedshiftInputFormat` consumes the S3 files line by line to produce an `RDD`. The schema obtained earlier is applied on this `RDD` to generate a `DataFrame`.

### Save Function - Writing to a Redshift table ###

`spark-redshift` allows you to write data back to Redshift. The source data is of type, `DataFrame`. We will need to assume that we have a source table available in Spark. This table can be sourced from a variety of sources such as, a Hive table, CSV file, parquet file, or even, a delimited text file. We will source a `DataFrame` from Redshift table as a temporary table in Spark and write it back to Redshift to illustrate this feature. In practice the source would not be a Redshift table or query since, in such a case it is easiest to perform a "CREATE TABLE AS" query on Redshift without going through Spark.

Recall, we have registered the `Dataframe` `eventDF` representing the `event` table  as a temporary table `myevent` in Spark.

```scala
eventsDF.registerTempTable("myevent")
```

Let us write the contents of the temporary table `myevent` to a Redshift table `redshiftevent`.

```scala
// Create a new table, `redshiftevent`, after dropping any existing redshiftevent table,
// then write event records with event id less than 1000
sqlContext.sql("SELECT * FROM myevent WHERE eventid <= 1000").withColumnRenamed("eventid", "id")
    .write.format("com.databricks.spark.redshift")
    .option("url", jdbcURL)
    .option("tempdir", tempS3Dir)
    .option("dbtable", "redshiftevent")
    .mode(SaveMode.Overwrite)
    .save()

// Append to an existing table redshiftevent if it exists or create a new one if it does 
// not exist, then write event records with event id greater than 1000
sqlContext.sql("SELECT * FROM myevent WHERE eventid > 1000").withColumnRenamed("eventid", "id")
    .write.format("com.databricks.spark.redshift")
    .option("url", jdbcURL)
    .option("tempdir", tempS3Dir)
    .option("dbtable", "redshiftevent")
    .mode(SaveMode.Append)
    .save()
```

`spark-redshift` automatically creates a Redshift table with the appropriate schema determined from the table/DataFrame being written. The default behavior is to create a new table and to throw an error message if a table with the same name already exists (corresponding to `SaveMode.ErrorIfExists`)

There are two key points to note:

1. Note the `.withColumnRenamed("eventid", "id")`. This feature if necessary if any of the source table columns names are key words in Redshift (ex. table) then `spark-redshift` will throw and error. This will be [fixed](https://github.com/databricks/spark-redshift/issues/80) in the release 0.5.1.

2. Note how we use the modes. The first write uses the mode `SaveMode.Overwrite` which means that the table will be dropped and recreated it exists. The second query uses the `SaveMode.Append` which will create the table if it does not exist but will append to the table if it already exists. The default mode is `SaveMode.ErrorIfExists` which creates the table if it does not exist and throws an error if it does. The last mode is `SaveMode.Ignore` which is same as `SaveMode.Overwrite` if the table does not exist but does nothing if the table exists.

We could have achieve similar results using SQL. The only thing to be aware of when using the SQL CLI is, all the SaveMode's are not available and only the default mode (`SaveMode.ErrorIfExists`) is applicable.

```sql
CREATE TABLE redshiftevent
USING com.databricks.spark.redshift
OPTIONS (
  dbtable 'redshiftevent',
  tempdir 's3n://redshift-spark/temp/',
  url 'jdbc:redshift://swredshift.czac2vcs84ci.us-east-1.redshift.amazonaws.com:5439/sparkredshift?user=spark&password=mysecretpass'
)
AS SELECT * FROM myevent;
```

By default the save operation uses the [key distribution style](http://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html) of `EVEN` in Redshift. This can be changed by using option parameter `diststyle` and `distkey`. See [spark-redshift documentation](https://github.com/databricks/spark-redshift) for details.

### Under the hood - Save Function ###

The implementation of the Save function is provided in the class, `com.databricks.spark.redshift.RedshiftWriter`. The following diagram shows how the `save` function is works.

![](images/savetoredshift.png)

The save function performs the following steps:

1. The partitions of the `DataFrame` are written out in parallel to the temporary S3 folder specified by the user. `spark-avro` is used to write the schema compliant data.
2. Next a Redshift [COPY](http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) command is created and invoked to load the files stored in the temporary S3 folder in the prior step, into the Redshift table.


## Integration with other Data Sources ##

Data read via `spark-redshift` is automatically converted to `DataFrame`'s, Spark’s primary abstraction for large datasets. This promotes interoperability between data sources, since types are automatically converted to Spark’s standard representations(for example `StringType`, `DecimalType`). A Redshift user can, for instance, join Redshift tables with data stored in S3, Hive tables, CSV or Parquet files stored on HDFS. This flexibility is important to users with complex data pipelines involving multiple sources.

The following code snippet illustrates these concepts-

```scala

val salesAGGQuery = """
    SELECT
        sales.eventid AS id,
        sum(qtysold) AS totalqty,
        sum(pricepaid) AS salesamt
    FROM sales
    GROUP BY sales.eventid"""
val salesAGGDF = sqlContext.read
    .format("com.databricks.spark.redshift")
    .option("url",jdbcURL)
    .option("tempdir", tempS3Dir)
    .option("query", salesAGGQuery)
    .load()


val salesAGGDF2 = salesAGGDF.join(eventsDF, salesAGGDF("id") === eventsDF("eventid"))
    .select("id", "eventname","totalqty","salesamt")
```

While both `eventDF` and `salesAGGDF` table are sourced from Redshift, assume for the sake of illustration that they are sourced from a non-Redshift data sources. This should not be too far-fetched since the DataFrame class is part of the Data Sources API and not the `spark-redshift` library. A `DataFrame` can be created from any data source compatible with the Data Sources API.

`salesAGGDF2` `DataFrame` is created by joining `eventsDF` and `salesAGGDF2`. We register it as a temporary table `redshift_sales_agg` before saving it to Redshift with the same name  `redshift_sales_agg`


```scala
salesAGGDF2.registerTempTable("redshift_sales_agg")

sqlContext.sql("SELECT * FROM redshift_sales_agg")
	.write.format("com.databricks.spark.redshift")
	.option("url", jdbcURL)
	.option("tempdir", tempS3Dir)
	.option("dbtable", "redshift_sales_agg")
	.mode(SaveMode.Overwrite)
	.save()
```


## Under the hood - Putting it all together ##

As we discussed earlier Spark SQL will introspect for a class called `DefaultSource` in the Data Sources API package, `com.databricks.spark.redshift`. The `DefaultSource` class implements the trait, `RelationProvider` which provides the default load functionality for the library. The interface provided by the `RelationProvider` trait consumes the parameters provided by the user and converts it to an instance of `BaseRelation` which is implemented by the class, `com.databricks.spark.redshift.RedshiftRelation`.

The `com.databricks.spark.redshift.RedshiftRelation` class is responsible for providing an `RDD` of `org.apache.spark.sql.Row` which backs the `org.apache.spark.sql.DataFrame` instance. This represents the underlying implementation for the load functionality for the `spark-redshift` package where the schema is inferred from the underlying Redshift table. The load function which supports the a user-defined schema is supported by the trait `org.apache.spark.sql.sources.SchemaRelationProvider` and implemented in the class `RedshiftRelation`.

The store functionality of the `spark-redshift` package is supported by the trait `org.apache.spark.sql.sources.CreatableRelationProvider` and implemented by the class `com.databricks.spark.redshift.RedshiftWriter`.


## Conclusion ###

The Data Sources API introduced in Spark 1.2 provides a unified interface for handling structured data. It unifies structured data sources under a common interface which is richer than the original RDD-based approach which used Hadoop's InputFormat API. In this article we explored how the `spark-redshift` package expands the Data Sources API to support Amazon Redshift which is becoming an increasingly popular choice in the Enterprise IT infrastructure.
