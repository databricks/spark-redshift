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

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Random
import scala.util.control.NonFatal

import com.databricks.spark.redshift.Parameters.MergedParameters

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SQLContext}
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
 *   - Use JDBC to issue any CREATE TABLE commands, if required.
 *
 *   - If there is data to be written (i.e. not all partitions were empty), then use the list of
 *     non-empty Avro files to construct a JSON manifest file to tell Redshift to load those files.
 *     This manifest is written to S3 alongside the Avro files themselves. We need to use an
 *     explicit manifest, as opposed to simply passing the name of the directory containing the
 *     Avro files, in order to work around a bug related to parsing of empty Avro files (see #96).
 *
 *   - Use JDBC to issue a COPY command in order to instruct Redshift to load the Avro data into
 *     the appropriate table. If the Overwrite SaveMode is being used, then by default the data
 *     will be loaded into a temporary staging table, which later will atomically replace the
 *     original table via a transaction.
 */
private[redshift] class RedshiftWriter(
    jdbcWrapper: JDBCWrapper,
    s3ClientFactory: AWSCredentials => AmazonS3Client) {

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
      creds: AWSCredentials,
      manifestUrl: String): String = {
    val credsString: String = AWSCredentialsUtils.getRedshiftCredentialsString(creds)
    val fixedUrl = Utils.fixS3Url(manifestUrl)
    s"COPY ${params.table.get} FROM '$fixedUrl' CREDENTIALS '$credsString' FORMAT AS " +
      s"AVRO 'auto' manifest ${params.extraCopyOptions}"
  }

  /**
   * Sets up a staging table then runs the given action, passing the temporary table name
   * as a parameter.
   */
  private def withStagingTable(
      conn: Connection,
      table: TableName,
      action: (String) => Unit) {
    val randomSuffix = Math.abs(Random.nextInt()).toString
    val tempTable =
      table.copy(unescapedTableName = s"${table.unescapedTableName}_staging_$randomSuffix")
    val backupTable =
      table.copy(unescapedTableName = s"${table.unescapedTableName}_backup_$randomSuffix")
    log.info("Loading new Redshift data to: " + tempTable)
    log.info("Existing data will be backed up in: " + backupTable)

    try {
      action(tempTable.toString)

      if (jdbcWrapper.tableExists(conn, table.toString)) {
        jdbcWrapper.executeInterruptibly(conn.prepareStatement(
          s"""
             | BEGIN;
             | ALTER TABLE $table RENAME TO ${backupTable.escapedTableName};
             | ALTER TABLE $tempTable RENAME TO ${table.escapedTableName};
             | DROP TABLE $backupTable;
             | END;
           """.stripMargin.trim))
      } else {
        jdbcWrapper.executeInterruptibly(conn.prepareStatement(
          s"ALTER TABLE $tempTable RENAME TO ${table.escapedTableName}"))
      }
    } finally {
      jdbcWrapper.executeInterruptibly(conn.prepareStatement(s"DROP TABLE IF EXISTS $tempTable"))
    }
  }

  /**
    * Generate COMMENT SQL statements for the table and columns.
    */
  private def commentActions(tableComment: Option[String], schema: StructType): List[String] = {
    tableComment.toList.map(desc => s"COMMENT ON TABLE %s IS '$desc'") ++
    schema.fields
      .withFilter(f => f.metadata.contains("description"))
      .map(f => s"""COMMENT ON COLUMN %s.${f.name} IS '${f.metadata.getString("description")}'""")
  }

  /**
   * Perform the Redshift load, including deletion of existing data in the case of an overwrite,
   * and creating the table if it doesn't already exist.
   */
  private def doRedshiftLoad(
      conn: Connection,
      data: DataFrame,
      saveMode: SaveMode,
      params: MergedParameters,
      creds: AWSCredentials,
      manifestUrl: Option[String]): Unit = {

    // Overwrites must drop the table, in case there has been a schema update
    if (saveMode == SaveMode.Overwrite) {
      jdbcWrapper.executeInterruptibly(
        conn.prepareStatement(s"DROP TABLE IF EXISTS ${params.table.get}"))
    }

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
      try {
        jdbcWrapper.executeInterruptibly(conn.prepareStatement(copyStatement))
      } catch {
        case e: SQLException =>
          // Try to query Redshift's STL_LOAD_ERRORS table to figure out why the load failed.
          // See http://docs.aws.amazon.com/redshift/latest/dg/r_STL_LOAD_ERRORS.html for details.
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
      tempDir: String): Option[String] = {
    // spark-avro does not support Date types. In addition, it converts Timestamps into longs
    // (milliseconds since the Unix epoch). Redshift is capable of loading timestamps in
    // 'epochmillisecs' format but there's no equivalent format for dates. To work around this, we
    // choose to write out both dates and timestamps as strings using the same timestamp format.
    // For additional background and discussion, see #39.

    // Convert the rows so that timestamps and dates become formatted strings.
    // Formatters are not thread-safe, and thus these functions are not thread-safe.
    // However, each task gets its own deserialized copy, making this safe.
    val conversionFunctions: Array[Any => Any] = data.schema.fields.map { field =>
      field.dataType match {
        case DateType =>
          val dateFormat = new RedshiftDateFormat()
          (v: Any) => {
            if (v == null) null else dateFormat.format(v.asInstanceOf[Date])
          }
        case TimestampType =>
          val timestampFormat = new RedshiftTimestampFormat()
          (v: Any) => {
            if (v == null) null else timestampFormat.format(v.asInstanceOf[Timestamp])
          }
        case _ => (v: Any) => v
      }
    }

    // Use Spark accumulators to determine which partitions were non-empty.
    val nonEmptyPartitions =
      sqlContext.sparkContext.accumulableCollection(mutable.HashSet.empty[Int])

    val convertedRows: RDD[Row] = data.mapPartitions { iter =>
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
        case StructField(name, DateType, nullable, meta) =>
          StructField(name, StringType, nullable, meta)
        case StructField(name, TimestampType, nullable, meta) =>
          StructField(name, StringType, nullable, meta)
        case other => other
      }
    )

    sqlContext.createDataFrame(convertedRows, convertedSchema)
      .write
      .format("com.databricks.spark.avro")
      .save(tempDir)

    if (nonEmptyPartitions.value.isEmpty) {
      None
    } else {
      // See https://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html
      // for a description of the manifest file format. The URLs in this manifest must be absolute
      // and complete.

      // The saved filenames depend on the spark-avro version. In spark-avro 1.0.0, the write
      // path uses SparkContext.saveAsHadoopFile(), which produces filenames of the form
      // part-XXXXX.avro. In spark-avro 2.0.0+, the partition filenames are of the form
      // part-r-XXXXX-UUID.avro.
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
   * Write a DataFrame to a Redshift table, using S3 and Avro serialization
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

    val creds: AWSCredentials =
      AWSCredentialsUtils.load(params, sqlContext.sparkContext.hadoopConfiguration)

    Utils.assertThatFileSystemIsNotS3BlockFileSystem(
      new URI(params.rootTempDir), sqlContext.sparkContext.hadoopConfiguration)

    Utils.checkThatBucketHasObjectLifecycleConfiguration(params.rootTempDir, s3ClientFactory(creds))

    val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)

    try {
      val tempDir = params.createPerQueryTempDir()
      val manifestUrl = unloadData(sqlContext, data, tempDir)
      if (saveMode == SaveMode.Overwrite && params.useStagingTable) {
        withStagingTable(conn, params.table.get, stagingTable => {
          val updatedParams = MergedParameters(params.parameters.updated("dbtable", stagingTable))
          doRedshiftLoad(conn, data, saveMode, updatedParams, creds, manifestUrl)
        })
      } else {
        doRedshiftLoad(conn, data, saveMode, params, creds, manifestUrl)
      }
    } finally {
      conn.close()
    }
  }
}

object DefaultRedshiftWriter extends RedshiftWriter(
  DefaultJDBCWrapper,
  awsCredentials => new AmazonS3Client(awsCredentials))
