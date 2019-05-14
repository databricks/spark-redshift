/*
 * Copyright 2015 TouchType Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.redshift

import java.net.URI
import java.sql.{Connection, Date, SQLException, Timestamp}

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory
import scala.collection.mutable
import scala.util.control.NonFatal

import com.databricks.spark.redshift.Parameters.MergedParameters

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.types._

/**
 * Functions to write data to Redshift.
 *
 * At a high level, writing data back to Redshift involves the following steps:
 *
 *   - Use the spark-avro library to save the DataFrame to S3 using Avro serialization. Prior to
 *     saving the data, certain data type conversions are applied in order to work around
 *     limitations in Avro's data type support and Redshift's case-insensitive identifier handling.
 *
 *     While writing the Avro files, we use accumulators to keep track of which partitions were
 *     non-empty. After the write operation completes, we use this to construct a list of non-empty
 *     Avro partition files.
 *
 *   - If there is data to be written (i.e. not all partitions were empty), then use the list of
 *     non-empty Avro files to construct a JSON manifest file to tell Redshift to load those files.
 *     This manifest is written to S3 alongside the Avro files themselves. We need to use an
 *     explicit manifest, as opposed to simply passing the name of the directory containing the
 *     Avro files, in order to work around a bug related to parsing of empty Avro files (see #96).
 *
 *   - Start a new JDBC transaction and disable auto-commit. Depending on the SaveMode, issue
 *     DELETE TABLE or CREATE TABLE commands, then use the COPY command to instruct Redshift to load
 *     the Avro data into the appropriate table.
 */
private[redshift] class RedshiftWriter(
    jdbcWrapper: JDBCWrapper,
    s3ClientFactory: AWSCredentialsProvider => AmazonS3Client) {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Generate CREATE TABLE statement for Redshift
   */
  // Visible for testing.
  private[redshift] def createTableSql(data: DataFrame, params: MergedParameters): String = {
    val schemaSql = jdbcWrapper.schemaString(data.schema)
    val distStyleDef = params.distStyle match {
      case Some(style) => s"DISTSTYLE $style"
      case None => ""
    }
    val distKeyDef = params.distKey match {
      case Some(key) => s"DISTKEY ($key)"
      case None => ""
    }
    val sortKeyDef = params.sortKeySpec.getOrElse("")
    val table = params.table.get

    s"CREATE TABLE IF NOT EXISTS $table ($schemaSql) $distStyleDef $distKeyDef $sortKeyDef"
  }

  /**
   * Generate the COPY SQL command
   */
  private def copySql(
      sqlContext: SQLContext,
      params: MergedParameters,
      creds: AWSCredentialsProvider,
      manifestUrl: String): String = {
    val credsString: String =
      AWSCredentialsUtils.getRedshiftCredentialsString(params, creds.getCredentials)
    val fixedUrl = Utils.fixS3Url(manifestUrl)
    val format = params.tempFormat match {
      case "AVRO" => "AVRO 'auto'"
      case csv if csv == "CSV" || csv == "CSV GZIP" => csv + s" NULL AS '${params.nullString}'"
    }
    s"COPY ${params.table.get} FROM '$fixedUrl' CREDENTIALS '$credsString' FORMAT AS " +
      s"${format} manifest ${params.extraCopyOptions}"
  }

  /**
    * Generate COMMENT SQL statements for the table and columns.
    */
  private[redshift] def commentActions(tableComment: Option[String], schema: StructType):
      List[String] = {
    tableComment.toList.map(desc => s"COMMENT ON TABLE %s IS '${desc.replace("'", "''")}'") ++
    schema.fields
      .withFilter(f => f.metadata.contains("description"))
      .map(f => s"""COMMENT ON COLUMN %s."${f.name.replace("\"", "\\\"")}""""
              + s" IS '${f.metadata.getString("description").replace("'", "''")}'")
  }

  /**
   * Perform the Redshift load by issuing a COPY statement.
   */
  private def doRedshiftLoad(
      conn: Connection,
      data: DataFrame,
      params: MergedParameters,
      creds: AWSCredentialsProvider,
      manifestUrl: Option[String]): Unit = {

    // If the table doesn't exist, we need to create it first, using JDBC to infer column types
    val createStatement = createTableSql(data, params)
    log.info(createStatement)
    jdbcWrapper.executeInterruptibly(conn.prepareStatement(createStatement))

    val preActions = commentActions(params.description, data.schema) ++ params.preActions

    // Execute preActions
    preActions.foreach { action =>
      val actionSql = if (action.contains("%s")) action.format(params.table.get) else action
      log.info("Executing preAction: " + actionSql)
      jdbcWrapper.executeInterruptibly(conn.prepareStatement(actionSql))
    }

    manifestUrl.foreach { manifestUrl =>
      // Load the temporary data into the new file
      val copyStatement = copySql(data.sqlContext, params, creds, manifestUrl)
      log.info(copyStatement)
      try {
        jdbcWrapper.executeInterruptibly(conn.prepareStatement(copyStatement))
      } catch {
        case e: SQLException =>
          log.error("SQLException thrown while running COPY query; will attempt to retrieve " +
            "more information by querying the STL_LOAD_ERRORS table", e)
          // Try to query Redshift's STL_LOAD_ERRORS table to figure out why the load failed.
          // See http://docs.aws.amazon.com/redshift/latest/dg/r_STL_LOAD_ERRORS.html for details.
          conn.rollback()
          val errorLookupQuery =
            """
              | SELECT *
              | FROM stl_load_errors
              | WHERE query = pg_last_query_id()
            """.stripMargin
          val detailedException: Option[SQLException] = try {
            val results =
              jdbcWrapper.executeQueryInterruptibly(conn.prepareStatement(errorLookupQuery))
            if (results.next()) {
              val errCode = results.getInt("err_code")
              val errReason = results.getString("err_reason").trim
              val columnLength: String =
                Option(results.getString("col_length"))
                  .map(_.trim)
                  .filter(_.nonEmpty)
                  .map(n => s"($n)")
                  .getOrElse("")
              val exceptionMessage =
                s"""
                   |Error (code $errCode) while loading data into Redshift: "$errReason"
                   |Table name: ${params.table.get}
                   |Column name: ${results.getString("colname").trim}
                   |Column type: ${results.getString("type").trim}$columnLength
                   |Raw line: ${results.getString("raw_line")}
                   |Raw field value: ${results.getString("raw_field_value")}
                  """.stripMargin
              Some(new SQLException(exceptionMessage, e))
            } else {
              None
            }
          } catch {
            case NonFatal(e2) =>
              log.error("Error occurred while querying STL_LOAD_ERRORS", e2)
              None
          }
          throw detailedException.getOrElse(e)
      }
    }

    // Execute postActions
    params.postActions.foreach { action =>
      val actionSql = if (action.contains("%s")) action.format(params.table.get) else action
      log.info("Executing postAction: " + actionSql)
      jdbcWrapper.executeInterruptibly(conn.prepareStatement(actionSql))
    }
  }

  /**
   * Serialize temporary data to S3, ready for Redshift COPY, and create a manifest file which can
   * be used to instruct Redshift to load the non-empty temporary data partitions.
   *
   * @return the URL of the manifest file in S3, in `s3://path/to/file/manifest.json` format, if
   *         at least one record was written, and None otherwise.
   */
  private def unloadData(
      sqlContext: SQLContext,
      data: DataFrame,
      tempDir: String,
      tempFormat: String,
      nullString: String): Option[String] = {
    // spark-avro does not support Date types. In addition, it converts Timestamps into longs
    // (milliseconds since the Unix epoch). Redshift is capable of loading timestamps in
    // 'epochmillisecs' format but there's no equivalent format for dates. To work around this, we
    // choose to write out both dates and timestamps as strings.
    // For additional background and discussion, see #39.

    // Convert the rows so that timestamps and dates become formatted strings.
    // Formatters are not thread-safe, and thus these functions are not thread-safe.
    // However, each task gets its own deserialized copy, making this safe.
    val conversionFunctions: Array[Any => Any] = data.schema.fields.map { field =>
      field.dataType match {
        case _: DecimalType => (v: Any) => if (v == null) null else v.toString
        case DateType =>
          val dateFormat = Conversions.createRedshiftDateFormat()
          (v: Any) => {
            if (v == null) null else dateFormat.format(v.asInstanceOf[Date])
          }
        case TimestampType =>
          val timestampFormat = Conversions.createRedshiftTimestampFormat()
          (v: Any) => {
            if (v == null) null else timestampFormat.format(v.asInstanceOf[Timestamp])
          }
        case _ => (v: Any) => v
      }
    }

    // Use Spark accumulators to determine which partitions were non-empty.
    val nonEmptyPartitions =
      sqlContext.sparkContext.accumulableCollection(mutable.HashSet.empty[Int])

    val convertedRows: RDD[Row] = data.rdd.mapPartitions { iter: Iterator[Row] =>
      if (iter.hasNext) {
        nonEmptyPartitions += TaskContext.get.partitionId()
      }
      iter.map { row =>
        val convertedValues: Array[Any] = new Array(conversionFunctions.length)
        var i = 0
        while (i < conversionFunctions.length) {
          convertedValues(i) = conversionFunctions(i)(row(i))
          i += 1
        }
        Row.fromSeq(convertedValues)
      }
    }

    // Convert all column names to lowercase, which is necessary for Redshift to be able to load
    // those columns (see #51).
    val schemaWithLowercaseColumnNames: StructType =
      StructType(data.schema.map(f => f.copy(name = f.name.toLowerCase)))

    if (schemaWithLowercaseColumnNames.map(_.name).toSet.size != data.schema.size) {
      throw new IllegalArgumentException(
        "Cannot save table to Redshift because two or more column names would be identical" +
        " after conversion to lowercase: " + data.schema.map(_.name).mkString(", "))
    }

    // Update the schema so that Avro writes date and timestamp columns as formatted timestamp
    // strings. This is necessary for Redshift to be able to load these columns (see #39).
    val convertedSchema: StructType = StructType(
      schemaWithLowercaseColumnNames.map {
        case StructField(name, _: DecimalType, nullable, meta) =>
          StructField(name, StringType, nullable, meta)
        case StructField(name, DateType, nullable, meta) =>
          StructField(name, StringType, nullable, meta)
        case StructField(name, TimestampType, nullable, meta) =>
          StructField(name, StringType, nullable, meta)
        case other => other
      }
    )

    val writer = sqlContext.createDataFrame(convertedRows, convertedSchema).write
    (tempFormat match {
      case "AVRO" =>
        writer.format("avro")
      case "CSV" =>
        writer.format("csv")
          .option("escape", "\"")
          .option("nullValue", nullString)
      case "CSV GZIP" =>
        writer.format("csv")
          .option("escape", "\"")
          .option("nullValue", nullString)
          .option("compression", "gzip")
    }).save(tempDir)

    if (nonEmptyPartitions.value.isEmpty) {
      None
    } else {
      // See https://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html
      // for a description of the manifest file format. The URLs in this manifest must be absolute
      // and complete.

      // The partition filenames are of the form part-r-XXXXX-UUID.fileExtension.
      val fs = FileSystem.get(URI.create(tempDir), sqlContext.sparkContext.hadoopConfiguration)
      val partitionIdRegex = "^part-(?:r-)?(\\d+)[^\\d+].*$".r
      val filesToLoad: Seq[String] = {
        val nonEmptyPartitionIds = nonEmptyPartitions.value.toSet
        fs.listStatus(new Path(tempDir)).map(_.getPath.getName).collect {
          case file @ partitionIdRegex(id) if nonEmptyPartitionIds.contains(id.toInt) => file
        }
      }
      // It's possible that tempDir contains AWS access keys. We shouldn't save those credentials to
      // S3, so let's first sanitize `tempdir` and make sure that it uses the s3:// scheme:
      val sanitizedTempDir = Utils.fixS3Url(
        Utils.removeCredentialsFromURI(URI.create(tempDir)).toString).stripSuffix("/")
      val manifestEntries = filesToLoad.map { file =>
        s"""{"url":"$sanitizedTempDir/$file", "mandatory":true}"""
      }
      val manifest = s"""{"entries": [${manifestEntries.mkString(",\n")}]}"""
      val manifestPath = sanitizedTempDir + "/manifest.json"
      val fsDataOut = fs.create(new Path(manifestPath))
      try {
        fsDataOut.write(manifest.getBytes("utf-8"))
      } finally {
        fsDataOut.close()
      }
      Some(manifestPath)
    }
  }

  /**
   * Write a DataFrame to a Redshift table, using S3 and Avro or CSV serialization
   */
  def saveToRedshift(
      sqlContext: SQLContext,
      data: DataFrame,
      saveMode: SaveMode,
      params: MergedParameters) : Unit = {
    if (params.table.isEmpty) {
      throw new IllegalArgumentException(
        "For save operations you must specify a Redshift table name with the 'dbtable' parameter")
    }

    if (!params.useStagingTable) {
      log.warn("Setting useStagingTable=false is deprecated; instead, we recommend that you " +
        "drop the target table yourself. For more details on this deprecation, see" +
        "https://github.com/databricks/spark-redshift/pull/157")
    }

    val creds: AWSCredentialsProvider =
      AWSCredentialsUtils.load(params, sqlContext.sparkContext.hadoopConfiguration)

    for (
      redshiftRegion <- Utils.getRegionForRedshiftCluster(params.jdbcUrl);
      s3Region <- Utils.getRegionForS3Bucket(params.rootTempDir, s3ClientFactory(creds))
    ) {
     val regionIsSetInExtraCopyOptions =
       params.extraCopyOptions.contains(s3Region) && params.extraCopyOptions.contains("region")
     if (redshiftRegion != s3Region && !regionIsSetInExtraCopyOptions) {
       log.error("The Redshift cluster and S3 bucket are in different regions " +
         s"($redshiftRegion and $s3Region, respectively). In order to perform this cross-region " +
         s"""write, you must add "region '$s3Region'" to the extracopyoptions parameter. """ +
         "For more details on cross-region usage, see the README.")
     }
    }

    // When using the Avro tempformat, log an informative error message in case any column names
    // are unsupported by Avro's schema validation:
    if (params.tempFormat == "AVRO") {
      for (fieldName <- data.schema.fieldNames) {
        // The following logic is based on Avro's Schema.validateName() method:
        val firstChar = fieldName.charAt(0)
        val isValid = (firstChar.isLetter || firstChar == '_') && fieldName.tail.forall { c =>
          c.isLetterOrDigit || c == '_'
        }
        if (!isValid) {
          throw new IllegalArgumentException(
            s"The field name '$fieldName' is not supported when using the Avro tempformat. " +
              "Try using the CSV tempformat  instead. For more details, see " +
              "https://github.com/databricks/spark-redshift/issues/84")
        }
      }
    }

    Utils.assertThatFileSystemIsNotS3BlockFileSystem(
      new URI(params.rootTempDir), sqlContext.sparkContext.hadoopConfiguration)

    Utils.checkThatBucketHasObjectLifecycleConfiguration(params.rootTempDir, s3ClientFactory(creds))

    // Save the table's rows to S3:
    val manifestUrl = unloadData(
      sqlContext,
      data,
      tempDir = params.createPerQueryTempDir(),
      tempFormat = params.tempFormat,
      nullString = params.nullString)
    val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
    conn.setAutoCommit(false)
    try {
      val table: TableName = params.table.get
      if (saveMode == SaveMode.Overwrite) {
        // Overwrites must drop the table in case there has been a schema update
        jdbcWrapper.executeInterruptibly(conn.prepareStatement(s"DROP TABLE IF EXISTS $table;"))
        if (!params.useStagingTable) {
          // If we're not using a staging table, commit now so that Redshift doesn't have to
          // maintain a snapshot of the old table during the COPY; this sacrifices atomicity for
          // performance.
          conn.commit()
        }
      }
      log.info(s"Loading new Redshift data to: $table")
      doRedshiftLoad(conn, data, params, creds, manifestUrl)
      conn.commit()
    } catch {
      case NonFatal(e) =>
        try {
          log.error("Exception thrown during Redshift load; will roll back transaction", e)
          conn.rollback()
        } catch {
          case NonFatal(e2) =>
            log.error("Exception while rolling back transaction", e2)
        }
        throw e
    } finally {
      conn.close()
    }
  }
}

object DefaultRedshiftWriter extends RedshiftWriter(
  DefaultJDBCWrapper,
  awsCredentials => new AmazonS3Client(awsCredentials))
