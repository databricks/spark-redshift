# # `spark-redshift`

[![Build Status](https://travis-ci.org/databricks/spark-redshift.svg?branch=master)](https://travis-ci.org/databricks/spark-redshift)
[![codecov.io](http://codecov.io/github/databricks/spark-redshift/coverage.svg?branch=master)](http://codecov.io/github/databricks/spark-redshift?branch=master)

A library to load data into Spark SQL DataFrames from Amazon Redshift, and write them back to
Redshift tables. Amazon S3 is used to efficiently transfer data in and out of Redshift, and
JDBC is used to automatically trigger the appropriate `COPY` and `UNLOAD` commands on Redshift.

- [Installation](#installation)
- Usage:
  - Data sources API: [Scala](#scala), [Python](#python), [SQL](#sql)
  - [Hadoop InputFormat](#hadoop-inputformat)
- [Configuration](#configuration)
  - [AWS Credentials](#aws-credentials)
  - [Parameters](#parameters)
  - [Configuring the maximum size of string columns](#configuring-the-maximum-size-of-string-columns)
- [Migration Guide](#migration-guide)

## Installation

`spark-redshift` requires Apache Spark 1.4+ and Amazon Redshift 1.0.963+.

You may use this library in your applications with the following dependency information:

```
groupId: com.databricks
artifactId: spark-redshift
version: 0.5.0-SNAPSHOT
```

You will also need to provide a JDBC driver that is compatible with Redshift. Amazon recommend that you use [their driver](http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html), which is distributed as a JAR that is hosted on Amazon's website. This library has also been successfully tested using the Postgres JDBC driver.

**Note on Hadoop versions**: This library depends on [`spark-avro`](https://github.com/databricks/spark-avro), which should automatically be downloaded because it is declared as a dependency. However, you may need to provide the corresponding `avro-mapred` dependency which matches your Hadoop distribution. In most deployments, however, this dependency will be automatically provided by your cluster's Spark assemblies and no additional action will be required.

## Usage

### Data Sources API

You can use `spark-redshift` via the Data Sources API in Scala, Python or SQL, as follows:

#### Scala

```scala
import org.apache.spark.sql._

val sc = // existing SparkContext
val sqlContext = new SQLContext(sc)


// Get some data from a Redshift table
val df: DataFrame = sqlContext.read
    .format("com.databricks.spark.redshift")
    .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass")
    .option("dbtable" -> "my_table")
    .option("tempdir" -> "s3://path/for/temp/data")
    .load()

// Can also load data from a Redshift query
val df: DataFrame = sqlContext.read
    .format("com.databricks.spark.redshift")
    .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass")
    .option("query" -> "select x, count(*) my_table group by x")
    .option("tempdir" -> "s3://path/for/temp/data")
    .load()

// Apply some transformations to the data as per normal, then you can use the
// Data Source API to write the data back to another table

df.write
  .format("com.databricks.spark.redshift")
    .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass")
    .option("dbtable" -> "my_table_copy")
    .option("tempdir" -> "s3://path/for/temp/data")
  .mode("error")
  .save()
```

#### Python

```python
from pyspark.sql import SQLContext

sc = # existing SparkContext
sql_context = SQLContext(sc)

# Read data from a table
df = sql_context.read \
    .format("com.databricks.spark.redshift") \
    .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass") \
    .option("dbtable" -> "my_table") \
    .option("tempdir" -> "s3://path/for/temp/data") \
    .load()

# Read data from a query
df = sql_context.read \
    .format("com.databricks.spark.redshift") \
    .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass") \
    .option("query" -> "select x, count(*) my_table group by x") \
    .option("tempdir" -> "s3://path/for/temp/data") \
    .load()

# Write back to a table
df.write \
  .format("com.databricks.spark.redshift")
  .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass") \
  .option("dbtable" -> "my_table_copy") \
  .option("tempdir" -> "s3://path/for/temp/data") \
  .mode("error")
  .save()
```

#### SQL

```sql
CREATE TABLE my_table
USING com.databricks.spark.redshift
OPTIONS (dbtable 'my_table',
         tempdir 's3://my_bucket/tmp',
         url 'jdbc:redshift://host:port/db?user=username&password=pass');
```

### Hadoop InputFormat

The library contains a Hadoop input format for Redshift tables unloaded with the ESCAPE option,
which you may make direct use of as follows:

```scala
import com.databricks.spark.redshift.RedshiftInputFormat

val records = sc.newAPIHadoopFile(
  path,
  classOf[RedshiftInputFormat],
  classOf[java.lang.Long],
  classOf[Array[String]])
```

## Configuration

### AWS Credentials

`spark-redshift` reads and writes data to S3 when transferring data to/from Redshift. As a result, it requires AWS credentials with read and write access to a S3 bucket (specified as `tempdir` in the configuration parameters described below).

You can provide AWS credentials via the parameters listed below, with Hadoop `fs.*` configuration settings, or by making them available via the usual environment variables, system properties or IAM roles.

**:warning: Note**: `spark-redshift` does not clean up the temporary files that it creates in S3. As a result, we recommend that you use a dedicated temporary S3 bucket with an [object lifecycle configuration](http://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html) to ensure that temporary files are automatically deleted after a specified expiration period.

### Parameters

The parameter map or <tt>OPTIONS</tt> provided in Spark SQL supports the following settings.

<table>
 <tr>
    <th>Parameter</th>
    <th>Required</th>
    <th>Default</th>
    <th>Notes</th>
 </tr>

 <tr>
    <td><tt>dbtable</tt></td>
    <td>Yes, unless <tt>query</tt> is specified</td>
    <td>No default</td>
    <td>The table to create or read from in Redshift. This parameter is required when saving data back to Redshift.</td>
 </tr>
 <tr>
    <td><tt>query</tt></td>
    <td>Yes, unless <tt>dbtable</tt> is specified</td>
    <td>No default</td>
    <td>The query to read from in Redshift</td>
 </tr>
 <tr>
    <td><tt>url</tt></td>
    <td>Yes</td>
    <td>No default</td>
    <td>
<p>A JDBC URL, of the format, <tt>jdbc:subprotocol://host:port/database?user=username&password=password</tt></p>

<ul>
 <li><tt>subprotocol</tt> can be <tt>postgresql</tt> or <tt>redshift</tt>, depending on which JDBC driver 
    you have loaded. Note however that one Redshift-compatible driver must be on the classpath and match 
    this URL.</li>
 <li><tt>host</tt> and <tt>port</tt> should point to the Redshift master node, so security groups and/or VPC will
need to be configured to allow access from your driver application.
 <li><tt>database</tt> identifies a Redshift database name</li>
 <li><tt>user</tt> and <tt>password</tt> are credentials to access the database, which must be embedded
    in this URL for JDBC, and your user account should have necessary privileges for the table being referenced. </li>
    </td>
 </tr>
 <tr>
    <td><tt>aws_access_key_id</tt></td>
    <td>No, unless also unavailable from environment</td>
    <td><a href="http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html">Default Provider Chain</a></td>
    <td>AWS access key, must have write permissions to the S3 bucket.</td>
 </tr>
 <tr>
    <td><tt>aws_secret_access_key</tt></td>
    <td>No, unless also unavailable from environment</td>
    <td><a href="http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html">Default Provider Chain</a></td>
    <td>AWS secret access key corresponding to provided access key.</td>
 </tr>
 <tr>
    <td><tt>aws_security_token</tt></td>
    <td>No, unless using AWS Security Token Service</td>
    <td>No default</td>
    <td>AWS security token corresponding to provided access key.</td>
 </tr>
 <tr>
    <td><tt>tempdir</tt></td>
    <td>Yes</td>
    <td>No default</td>
    <td>A writeable location in Amazon S3, to be used for unloaded data when reading and Avro data to be loaded into
Redshift when writing. If you're using `spark-redshift` as part of a regular ETL pipeline, it can be useful to
set a <a href="http://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html">Lifecycle Policy</a> on a bucket
and use that as a temp location for this data.
    </td>
 </tr>
 <tr>
    <td><tt>jdbcdriver</tt></td>
    <td>No</td>
    <td><tt>com.amazon.redshift.jdbc4.Driver</tt></td>
    <td>The class name of the JDBC driver to load before JDBC operations. Must be on classpath.</td>
 </tr>
 <tr>
    <td><tt>diststyle</tt></td>
    <td>No</td>
    <td><tt>EVEN</tt></td>
    <td>The Redshift <a href="http://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html">Distribution Style</a> to
be used when creating a table. Can be one of <tt>EVEN</tt>, <tt>KEY</tt> or <tt>ALL</tt> (see Redshift docs). When using <tt>KEY</tt>, you
must also set a distribution key with the <tt>distkey</tt> option.
    </td>
 </tr>
 <tr>
    <td><tt>distkey</tt></td>
    <td>No, unless using <tt>DISTSTYLE KEY</tt></td>
    <td>No default</td>
    <td>The name of a column in the table to use as the distribution key when creating a table.</td>
 </tr>
 <tr>
    <td><tt>sortkeyspec</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>
<p>A full Redshift <a href="http://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html">Sort Key</a> definition.</p>

<p>Examples include:</p>
<ul>
    <li><tt>SORTKEY my_sort_column</tt></li>
    <li><tt>COMPOUND SORTKEY sort_col_1, sort_col_2</tt></li>
    <li><tt>INTERLEAVED SORTKEY sort_col_1, sort_col_2</tt></li>
</ul>
    </td>
 </tr>
 <tr>
    <td><tt>usestagingtable</tt></td>
    <td>No</td>
    <td><tt>true</tt></td>
    <td>
<p>When performing an overwrite of existing data, this setting can be used to stage the new data in a temporary
table, such that we make sure the <tt>COPY</tt> finishes successfully before making any changes to the existing table.
This means that we minimize the amount of time that the target table will be unavailable and restore the old
data should the <tt>COPY</tt> fail.</p>

<p>You may wish to disable this by setting the parameter to <tt>false</tt> if you can't spare the disk space in your
Redshift cluster and/or don't have requirements to keep the table availability high.</p>
    </td>
 </tr>
 <tr>
    <td><tt>postactions</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>
<p>This can be a <tt>;</tt> separated list of SQL commands to be executed after a successful <tt>COPY</tt> when loading data.
It may be useful to have some <tt>GRANT</tt> commands or similar run here when loading new data. If the command contains
<tt>%s</tt>, the table name will be formatted in before execution (in case you're using a staging table).</p>

<p>Be warned that if this commands fail, it is treated as an error and you'll get an exception. If using a staging
table, the changes will be reverted and the backup table restored if post actions fail.</p>
    </td>
 </tr>
</table>

## Additional configuration options

### Configuring the maximum size of string columns

When creating Redshift tables, `spark-redshift`'s default behavior is to create `TEXT` columns for string columns. Redshift stores `TEXT` columns as `VARCHAR(256)`, so these columns have a maximum size of 256 characters ([source](http://docs.aws.amazon.com/redshift/latest/dg/r_Character_types.html)).

To support larger columns, you can use the `maxlength` column metadata field to specify the maximum length of individual string columns. This can also be done as a space-savings performance optimization in order to declare columns with a smaller maximum length than the default.

Here is an example of updating a column's metadata field in Scala:

```
import org.apache.spark.sql.types.MetadataBuilder
val metadata = new MetadataBuilder().putLong("maxlength", 10).build()
df.withColumn("colName", col("colName").as("colName", metadata)
```

Column metadata modification is unsupported in the Python, SQL, and R language APIs.

## Migration Guide


Some breaking changes were made in version 0.3 of the Hadoop InputFormat. Users should make the
following changes in their code if they would like to use the 0.3+ versions, when using the input format
directly:

 * <tt>com.databricks.examples.redshift.input</tt> -> <tt>com.databricks.spark.redshift</tt>
 * <tt>SchemaRDD</tt> -> <tt>DataFrame</tt>
 * `import com.databricks.examples.redshift.input.RedshiftInputFormat._` -> `import com.databricks.spark.redshift._`

Version 0.4+ adds the DataSource API and JDBC, which is an entirely new API, so although this won't break
code using the InputFormat directly, you may wish to make use of the new functionality to avoid performing
<tt>UNLOAD</tt> queries manually.
