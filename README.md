# `spark-redshift`

[![Build Status](https://travis-ci.org/databricks/spark-redshift.svg?branch=master)](https://travis-ci.org/databricks/spark-redshift)
[![codecov.io](http://codecov.io/github/databricks/spark-redshift/coverage.svg?branch=master)](http://codecov.io/github/databricks/spark-redshift?branch=master)

A library to load data into Spark SQL DataFrames from Amazon Redshift, and write them back to
Redshift tables. Amazon S3 is used to transfer data efficiently into and out of Redshift, and
JDBC is used to trigger the appropriate <tt>COPY</tt> and <tt>UNLOAD</tt> commands on Redshift automatically.

## Install

**Note:** `spark-redshift` requires Apache Spark version 1.4+ and Amazon Redshift version 1.0.963+ for
writing with Avro data.

You may use this library in your applications with the following dependency information:

```
groupId: com.databricks
artifactId: spark-redshift
version: 0.4.1
```

The project makes use of [`spark-avro`](https://github.com/databricks/spark-avro), which is pulled
in as a dependency, however you'll need to provide the corresponding `avro-mapred` matching the Hadoop
distribution that you plan to deploy to.

Further, as Redshift is an AWS product, some AWS libraries will be required. This library expects that
your deployment environment will include `hadoop-aws`, or other things necessary to access S3, credentials,
etc. Check the dependencies with "provided" scope in <tt>build.sbt</tt> if you're at all unclear.

You're also going to need a JDBC driver that is compatible with Redshift. Amazon recommend that you
use [their driver](http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html),
although this library has also been successfully tested using the Postgres JDBC driver.

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
    .option("url", "jdbc:postgresql://redshifthost:5439/database?user=username&password=pass")
    .option("dbtable", "my_table")
    .option("tempdir", "s3://path/for/temp/data")
    .load()

// Apply some transformations to the data as per normal, then you can use the
// Data Source API to write the data back to another table

df.write
    .format("com.databricks.spark.redshift")
    .option("url", "jdbc:postgresql://redshifthost:5439/database?user=username&password=pass")
    .option("dbtable", "my_table_copy")
    .option("tempdir", "s3://path/for/temp/data")
    .option("avrocompression", "snappy")
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
    .option("url", "jdbc:postgresql://redshifthost:5439/database?user=username&password=pass") \
    .option("dbtable", "my_table") \
    .option("tempdir", "s3://path/for/temp/data") \
    .load()

# Write back to a table
df.write \
    .format("com.databricks.spark.redshift")
    .option("url", "jdbc:postgresql://redshifthost:5439/database?user=username&password=pass") \
    .option("dbtable", "my_table_copy") \
    .option("tempdir", "s3://path/for/temp/data") \
    .option("avrocompression", "snappy")
    .mode("error")
    .save()
```

#### SQL

```sql
CREATE TABLE my_table
USING com.databricks.spark.redshift
OPTIONS (dbtable 'my_table',
         tempdir 's3://my_bucket/tmp',
         avrocompression 'snappy',
         url 'jdbc:postgresql://host:port/db?user=username&password=pass');
```

### Scala helper functions

The <tt>com.databricks.spark.redshift</tt> package has some shortcuts if you're working directly
from a Scala application and don't want to use the Data Sources API:

```scala
import com.databricks.spark.redshift._

val sqlContext = new SQLContext(sc)

val dataFrame = sqlContext.redshiftTable( ... )
dataFrame.saveAsRedshiftTable( ... )
```

### Hadoop InputFormat

The library contains a Hadoop input format for Redshift tables unloaded with the ESCAPE option,
which you may make direct use of as follows:

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

## Parameters

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
    <td>Yes</td>
    <td>No default</td>
    <td>The table to create or read from in Redshift</td>
 </tr
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
    <td>No, unless using temporary IAM credentials</td>
    <td>None</td>
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
    <td><tt>overwrite</tt></td>
    <td>No</td>
    <td><tt>false</tt></td>
    <td>
If true, drop any existing data before writing new content. Only applies when using the Scala `saveAsRedshiftTable` function
directly, as `SaveMode` will be preferred when using the Data Source API. See also <tt>usestagingtable</tt></td>
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
 <tr>
    <td><tt>avrocompression</tt></td>
    <td>No</td>
    <td>No compression (unless set in Hadoop config)</td>
    <td>
<p>Sets the compression codec to use on the Avro data to be loaded into Redshift. This overwrites the <tt>avro.output.codec</tt>
key in the Hadoop configuration with the specified value and also sets <tt>mapred.output.compress = true</tt> and
<tt>mapred.output.compression.type = BLOCK</tt>. If left unset (or set to null or an empty string) it will leave
the Hadoop configuration unchanged.</p>
    </td>
 </tr>
</table>

## AWS Credentials

Note that you can provide AWS credentials in the parameters above, with Hadoop `fs.*` configuration settings, 
or you can make them available by the usual environment variables, system properties or IAM roles, etc. The credentials 
you provide will be used in Redshift <tt>COPY</tt> and <tt>UNLOAD</tt> commands, which means they need write access 
to the S3 bucket you reference in your <tt>tempdir</tt> setting.

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
