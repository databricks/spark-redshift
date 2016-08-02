# Redshift Data Source for Apache Spark

[![Build Status](https://travis-ci.org/databricks/spark-redshift.svg?branch=master)](https://travis-ci.org/databricks/spark-redshift)
[![codecov.io](http://codecov.io/github/databricks/spark-redshift/coverage.svg?branch=master)](http://codecov.io/github/databricks/spark-redshift?branch=master)

A library to load data into Spark SQL DataFrames from Amazon Redshift, and write them back to
Redshift tables. Amazon S3 is used to efficiently transfer data in and out of Redshift, and
JDBC is used to automatically trigger the appropriate `COPY` and `UNLOAD` commands on Redshift.

This library is more suited to ETL than interactive queries, since large amounts of data could be extracted to S3 for each query execution. If you plan to perform many queries against the same Redshift tables then we recommend saving the extracted data in a format such as Parquet.

- [Installation](#installation)
- Usage:
  - Data sources API: [Scala](#scala), [Python](#python), [SQL](#sql)
  - [Hadoop InputFormat](#hadoop-inputformat)
- [Configuration](#configuration)
  - [AWS Credentials](#aws-credentials)
  - [Parameters](#parameters)
- [Additional configuration options](#additional-configuration-options)
    - [Configuring the maximum size of string columns](#configuring-the-maximum-size-of-string-columns)
    - [Setting a custom column type](#setting-a-custom-column-type)
    - [Configuring column encoding](#configuring-column-encoding)
    - [Setting descriptions on columns](#setting-descriptions-on-columns)
- [Transactional Guarantees](#transactional-guarantees)
- [Migration Guide](#migration-guide)

## Installation

This library requires Apache Spark 2.0+ and Amazon Redshift 1.0.963+.

For version that works with Spark 1.x, please check for the [1.x branch](https://github.com/databricks/spark-redshift/tree/branch-1.x).

You may use this library in your applications with the following dependency information:

**Scala 2.10**

```
groupId: com.databricks
artifactId: spark-redshift_2.10
version: 2.0.0
```

**Scala 2.11**
```
groupId: com.databricks
artifactId: spark-redshift_2.11
version: 2.0.0
```

You will also need to provide a JDBC driver that is compatible with Redshift. Amazon recommend that you use [their driver](http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html), which is distributed as a JAR that is hosted on Amazon's website. This library has also been successfully tested using the Postgres JDBC driver.

**Note on Hadoop versions**: This library depends on [`spark-avro`](https://github.com/databricks/spark-avro), which should automatically be downloaded because it is declared as a dependency. However, you may need to provide the corresponding `avro-mapred` dependency which matches your Hadoop distribution. In most deployments, however, this dependency will be automatically provided by your cluster's Spark assemblies and no additional action will be required.

**Note on Amazon SDK dependency**: This library declares a `provided` dependency on components of the AWS Java SDK. In most cases, these libraries will be provided by your deployment environment. However, if you get ClassNotFoundExceptions for Amazon SDK classes then you will need to add explicit dependencies on `com.amazonaws.aws-java-sdk-core` and `com.amazonaws.aws-java-sdk-s3` as part of your build / runtime configuration. See the comments in `project/SparkRedshiftBuild.scala` for more details.

## Usage

### Data Sources API

Once you have [configured your AWS credentials](#aws-credentials), you can use this library via the Data Sources API in Scala, Python or SQL, as follows:

#### Scala

```scala
import org.apache.spark.sql._

val sc = // existing SparkContext
val sqlContext = new SQLContext(sc)

// Get some data from a Redshift table
val df: DataFrame = sqlContext.read
    .format("com.databricks.spark.redshift")
    .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass")
    .option("dbtable", "my_table")
    .option("tempdir", "s3n://path/for/temp/data")
    .load()

// Can also load data from a Redshift query
val df: DataFrame = sqlContext.read
    .format("com.databricks.spark.redshift")
    .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass")
    .option("query", "select x, count(*) my_table group by x")
    .option("tempdir", "s3n://path/for/temp/data")
    .load()

// Apply some transformations to the data as per normal, then you can use the
// Data Source API to write the data back to another table

df.write
  .format("com.databricks.spark.redshift")
  .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass")
  .option("dbtable", "my_table_copy")
  .option("tempdir", "s3n://path/for/temp/data")
  .mode("error")
  .save()

// Using IAM Role based authentication
df.write
  .format("com.databricks.spark.redshift")
  .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass")
  .option("dbtable", "my_table_copy")
  .option("aws_iam_role", "arn:aws:iam::123456789000:role/redshift_iam_role")
  .option("tempdir", "s3n://path/for/temp/data")
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
    .option("dbtable", "my_table") \
    .option("tempdir", "s3n://path/for/temp/data") \
    .load()

# Read data from a query
df = sql_context.read \
    .format("com.databricks.spark.redshift") \
    .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass") \
    .option("query", "select x, count(*) my_table group by x") \
    .option("tempdir", "s3n://path/for/temp/data") \
    .load()

# Write back to a table
df.write \
  .format("com.databricks.spark.redshift") \
  .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass") \
  .option("dbtable", "my_table_copy") \
  .option("tempdir", "s3n://path/for/temp/data") \
  .mode("error") \
  .save()

# Using IAM Role based authentication
df.write \
  .format("com.databricks.spark.redshift") \
  .option("url", "jdbc:redshift://redshifthost:5439/database?user=username&password=pass") \
  .option("dbtable", "my_table_copy") \
  .option("tempdir", "s3n://path/for/temp/data") \
  .option("aws_iam_role", "arn:aws:iam::123456789000:role/redshift_iam_role")
  .mode("error") \
  .save()
```

#### SQL

Reading data using SQL:

```sql
CREATE TABLE my_table
USING com.databricks.spark.redshift
OPTIONS (
  dbtable 'my_table',
  tempdir 's3n://path/for/temp/data',
  url 'jdbc:redshift://redshifthost:5439/database?user=username&password=pass'
);
```

Writing data using SQL:

```sql
-- Create a new table, throwing an error if a table with the same name already exists:
CREATE TABLE my_table
USING com.databricks.spark.redshift
OPTIONS (
  dbtable 'my_table',
  tempdir 's3n://path/for/temp/data'
  url 'jdbc:redshift://redshifthost:5439/database?user=username&password=pass'
)
AS SELECT * FROM tabletosave;
```

Note that the SQL API only supports the creation of new tables and not overwriting or appending; this corresponds to the default save mode of the other language APIs.

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

This library reads and writes data to S3 when transferring data to/from Redshift. As a result, it requires AWS credentials with read and write access to a S3 bucket (specified using the `tempdir` configuration parameter). Assuming that Spark has been configured to access S3, it should automatically discover the proper credentials to pass to Redshift.

There are four ways of configuring AWS credentials for use by this library:

1. **Set keys in Hadoop conf (best option for most users):** You can specify AWS keys via [Hadoop configuration properties](https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/site/markdown/tools/hadoop-aws/index.md). For example, if your `tempdir` configuration points to a `s3n://` filesystem then you can set the `fs.s3n.awsAccessKeyId` and `fs.s3n.awsSecretAccessKey` properties in a Hadoop XML configuration file or call `sc.hadoopConfiguration.set()` to mutate Spark's global Hadoop configuration.

 For example, if you are using the `s3n` filesystem then add

 ```scala
 sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "YOUR_KEY_ID")
 sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "YOUR_SECRET_ACCESS_KEY")
 ```

 and for the `s3a` filesystem add

 ```scala
 sc.hadoopConfiguration.set("fs.s3a.access.key", "YOUR_KEY_ID")
 sc.hadoopConfiguration.set("fs.s3a.secret.key", "YOUR_SECRET_ACCESS_KEY")
 ```

 Python users will have to use a slightly different method to modify the `hadoopConfiguration`, since this field is not exposed in all versions of PySpark. Although the following command relies on some Spark internals, it should work with all PySpark versions and is unlikely to break or change in the future:

 ```python
 sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "YOUR_KEY_ID")
 sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "YOUR_SECRET_ACCESS_KEY")
 ```

2. **Encode keys in `tempdir` URI**: For example, the URI `s3n://ACCESSKEY:SECRETKEY@bucket/path/to/temp/dir` encodes the key pair (`ACCESSKEY`, `SECRETKEY`). Due to [Hadoop limitations](https://issues.apache.org/jira/browse/HADOOP-3733), this approach will not work for secret keys which contain forward slash (`/`) characters.
3. **Set the `aws_iam_role` parameter:** If set, this takes precedence over any other authentication option.  You will need to have this IAM role attached to the Redshift cluster which allows read/write access to your `tempdir` bucket.  More info [here](http://docs.aws.amazon.com/redshift/latest/mgmt/copy-unload-iam-role.html)
4. **IAM instance profiles:** If you are running on EC2 and authenticate to S3 using IAM and [instance profiles](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2_instance-profiles.html), then you must must configure the `temporary_aws_access_key_id`, `temporary_aws_secret_access_key`, and `temporary_aws_session_token` configuration properties to point to temporary keys created via the AWS [Security Token Service](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html). These temporary keys will then be passed to Redshift via `LOAD` and `UNLOAD` commands.

> **:warning: Note**: This library does not clean up the temporary files that it creates in S3. As a result, we recommend that you use a dedicated temporary S3 bucket with an [object lifecycle configuration](http://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html) to ensure that temporary files are automatically deleted after a specified expiration period.

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
    <td><tt>user</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>The Redshift username.  Must be used in tandem with <tt>password</tt> option.  May only be used if the user and password are not passed in the URL, passing both will result in an error.</td>
 </tr>
 <tr>
    <td><tt>password</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>The Redshift password.  Must be used in tandem with <tt>user</tt> option.  May only be used if the user and password are not passed in the URL; passing both will result in an error.</td>
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
   <td><tt>aws_iam_role</tt></td>
   <td>Only if using IAM roles to authorize Redshift COPY/UNLOAD operations</td>
   <td>No default</td>
   <td>Fully specified ARN of the <a href="http://docs.aws.amazon.com/redshift/latest/mgmt/copy-unload-iam-role.html">IAM Role</a> attached to the Redshift cluster, ex: arn:aws:iam::123456789000:role/redshift_iam_role</td>
 </tr>
 <tr>
    <td><tt>temporary_aws_access_key_id</tt></td>
    <td>No, unless using EC2 instance profile authentication</td>
    <td>No default</td>
    <td>AWS access key, must have write permissions to the S3 bucket.</td>
 </tr>
 <tr>
    <td><tt>temporary_aws_secret_access_key</tt></td>
    <td>No, unless using EC2 instance profile authentication</td>
    <td>No default</td>
    <td>AWS secret access key corresponding to provided access key.</td>
 </tr>
 <tr>
    <td><tt>temporary_aws_session_token</tt></td>
    <td>No, unless using EC2 instance profile authentication</td>
    <td>No default</td>
    <td>AWS session token corresponding to provided access key.</td>
 </tr>
 <tr>
    <td><tt>tempdir</tt></td>
    <td>Yes</td>
    <td>No default</td>
    <td>A writeable location in Amazon S3, to be used for unloaded data when reading and Avro data to be loaded into
Redshift when writing. If you're using Redshift data source for Spark as part of a regular ETL pipeline, it can be useful to
set a <a href="http://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html">Lifecycle Policy</a> on a bucket
and use that as a temp location for this data.
    </td>
 </tr>
 <tr>
    <td><tt>jdbcdriver</tt></td>
    <td>No</td>
    <td>Determined by the JDBC URL's subprotocol</td>
    <td>The class name of the JDBC driver to use. This class must be on the classpath. In most cases, it should not be necessary to specify this option, as the appropriate driver classname should automatically be determined by the JDBC URL's subprotocol.</td>
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
    <li><tt>SORTKEY(my_sort_column)</tt></li>
    <li><tt>COMPOUND SORTKEY(sort_col_1, sort_col_2)</tt></li>
    <li><tt>INTERLEAVED SORTKEY(sort_col_1, sort_col_2)</tt></li>
</ul>
    </td>
 </tr>
 <tr>
    <td><del><tt>usestagingtable</tt></del> (Deprecated)</td>
    <td>No</td>
    <td><tt>true</tt></td>
    <td>
    <p>
    Setting this deprecated option to <tt>false</tt> will cause an overwrite operation's destination table to be dropped immediately at the beginning of the write, making the overwrite operation non-atomic and reducing the availability of the destination table. This may reduce the temporary disk space requirements for overwrites.
    </p>

    <p>Since setting <tt>usestagingtable=false</tt> operation risks data loss / unavailability, we have chosen to deprecate it in favor of requiring users to manually drop the destination table themselves.</p>
    </td>
 </tr>
 <tr>
    <td><tt>description</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>
<p>A description for the table. Will be set using the SQL COMMENT command, and should show up in most query tools.
See also the <tt>description</tt> metadata to set descriptions on individual columns.
 </tr>
 <tr>
    <td><tt>preactions</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>
<p>This can be a <tt>;</tt> separated list of SQL commands to be executed before loading <tt>COPY</tt> command.
It may be useful to have some <tt>DELETE</tt> commands or similar run here before loading new data. If the command contains
<tt>%s</tt>, the table name will be formatted in before execution (in case you're using a staging table).</p>

<p>Be warned that if this commands fail, it is treated as an error and you'll get an exception. If using a staging
table, the changes will be reverted and the backup table restored if pre actions fail.</p>
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
 <tr>
    <td><tt>extracopyoptions</tt></td>
    <td>No</td>
    <td>No default</td>
    <td>
<p>A list extra options to append to the Redshift <tt>COPY</tt> command when loading data, e.g. <tt>TRUNCATECOLUMNS</tt>
or <TT>MAXERROR n</tt> (see the <a href="http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html#r_COPY-syntax-overview-optional-parameters">Redshift docs</a>
for other options).</p>

<p>Note that since these options are appended to the end of the <tt>COPY</tt> command, only options that make sense
at the end of the command can be used, but that should cover most possible use cases.</p>
    </td>
 </tr>
</table>

## Additional configuration options

### Configuring the maximum size of string columns

When creating Redshift tables, this library's default behavior is to create `TEXT` columns for string columns. Redshift stores `TEXT` columns as `VARCHAR(256)`, so these columns have a maximum size of 256 characters ([source](http://docs.aws.amazon.com/redshift/latest/dg/r_Character_types.html)).

To support larger columns, you can use the `maxlength` column metadata field to specify the maximum length of individual string columns. This can also be done as a space-savings performance optimization in order to declare columns with a smaller maximum length than the default.

> **:warning: Note**: Due to limitations in Spark, metadata modification is unsupported in the Python, SQL, and R language APIs.

Here is an example of updating multiple columns' metadata fields using Spark's Scala API:

```scala
import org.apache.spark.sql.types.MetadataBuilder

// Specify the custom width of each column
val columnLengthMap = Map(
  "language_code" -> 2,
  "country_code" -> 2,
  "url" -> 2083
)

var df = ... // the dataframe you'll want to write to Redshift

// Apply each column metadata customization
columnLengthMap.foreach { case (colName, length) =>
  val metadata = new MetadataBuilder().putLong("maxlength", length).build()
  df = df.withColumn(colName, df(colName).as(colName, metadata))
}

df.write
  .format("com.databricks.spark.redshift")
  .option("url", jdbcURL)
  .option("tempdir", s3TempDirectory)
  .option("dbtable", sessionTable)
  .save()
```

### Setting a custom column type

If you need to manually set a column type, you can use the `redshift_type` column metadata. For example, if you desire to override
the `Spark SQL Schema -> Redshift SQL` type matcher to assign a user-defined column type, you can do the following:

```scala
import org.apache.spark.sql.types.MetadataBuilder

// Specify the custom width of each column
val columnTypeMap = Map(
  "language_code" -> "CHAR(2)",
  "country_code" -> "CHAR(2)",
  "url" -> "BPCHAR(111)"
)

var df = ... // the dataframe you'll want to write to Redshift

// Apply each column metadata customization
columnTypeMap.foreach { case (colName, colType) =>
  val metadata = new MetadataBuilder().putString("redshift_type", colType).build()
  df = df.withColumn(colName, df(colName).as(colName, metadata))
}
```

### Configuring column encoding

When creating a table, this library can be configured to use a specific compression encoding on individual columns. You can use the `encoding` column metadata field to specify a compression encoding for each column (see [Amazon docs](http://docs.aws.amazon.com/redshift/latest/dg/c_Compression_encodings.html) for available encodings).

### Setting descriptions on columns

Redshift allows columns to have descriptions attached that should show up in most query tools (using the `COMMENT` command). You can set the `description` column metadata field to specify a description for individual columns.

## Transactional Guarantees

This section describes the transactional guarantees of the Redshift data source for Spark

### General background on Redshift and S3's properties

For general information on Redshift's transactional guarantees, see the [Managing Concurrent Write Operations](https://docs.aws.amazon.com/redshift/latest/dg/c_Concurrent_writes.html) chapter in the Redshift documentation. In a nutshell, Redshift provides [serializable isolation](https://docs.aws.amazon.com/redshift/latest/dg/c_serial_isolation.html) (according to the documentation for Redshift's [`BEGIN`](https://docs.aws.amazon.com/redshift/latest/dg/r_BEGIN.html) command, "[although] you can use any of the four transaction isolation levels, Amazon Redshift processes all isolation levels as serializable"). According to its [documentation](https://docs.aws.amazon.com/redshift/latest/dg/c_serial_isolation.html), "Amazon Redshift supports a default _automatic commit_ behavior in which each separately-executed SQL command commits individually." Thus, individual commands like `COPY` and `UNLOAD` are atomic and transactional, while explicit `BEGIN` and `END` should only be necessary to enforce the atomicity of multiple commands / queries.

When reading from / writing to Redshift, this library reads and writes data in S3. Both Spark and Redshift produce partitioned output which is stored in multiple files in S3. According to the [Amazon S3 Data Consistency Model](https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel) documentation, S3 bucket listing operations are eventually-consistent, so the files must to go to special lengths to avoid missing / incomplete data due to this source of eventual-consistency.

### Guarantees of the Redshift data source for Spark


**Appending to an existing table**: In the [`COPY`](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) command, this library uses [manifests](https://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html) to guard against certain eventually-consistent S3 operations. As a result, it appends to existing tables have the same atomic and transactional properties as regular Redshift `COPY` commands.

**Appending to an existing table**: When inserting rows into Redshift, this library uses the [`COPY`](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) command and specifies [manifests](https://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html) to guard against certain eventually-consistent S3 operations. As a result, `spark-redshift` appends to existing tables have the same atomic and transactional properties as regular Redshift `COPY` commands.


**Creating a new table (`SaveMode.CreateIfNotExists`)**: Creating a new table is a two-step process, consisting of a `CREATE TABLE` command followed by a [`COPY`](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) command to append the initial set of rows. Both of these operations are performed in a single transaction.

**Overwriting an existing table**: By default, this library uses transactions to perform overwrites, which are implemented by deleting the destination table, creating a new empty table, and appending rows to it.

If the deprecated `usestagingtable` setting is set to `false` then this library will commit the `DELETE TABLE` command before appending rows to the new table, sacrificing the atomicity of the overwrite operation but reducing the amount of staging space that Redshift needs during the overwrite.

**Querying Redshift tables**: Queries use Redshift's [`UNLOAD`](https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html) command to execute a query and save its results to S3 and use [manifests](https://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html) to guard against certain eventually-consistent S3 operations. As a result, queries from Redshift data source for Spark should have the same consistency properties as regular Redshift queries.

## Migration Guide

- Version 2.0 removed a number of deprecated APIs; for details, see https://github.com/databricks/spark-redshift/pull/239
