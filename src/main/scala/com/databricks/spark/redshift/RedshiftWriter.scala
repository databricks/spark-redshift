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

import java.sql.{Connection, Date, SQLException, Timestamp}
import java.util.Properties

import scala.util.Random
import scala.util.control.NonFatal

import com.databricks.spark.redshift.Parameters.MergedParameters

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._

/**
 * Functions to write data to Redshift with intermediate Avro serialisation into S3.
 */
class RedshiftWriter(jdbcWrapper: JDBCWrapper) extends Logging {

  /**
   * Generate CREATE TABLE statement for Redshift
   */
  private def createTableSql(data: DataFrame, params: MergedParameters): String = {
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
  private def copySql(sqlContext: SQLContext, params: MergedParameters): String = {
    val creds = params.credentialsString(sqlContext.sparkContext.hadoopConfiguration)
    val fixedUrl = Utils.fixS3Url(params.tempPath)
    s"COPY ${params.table.get} FROM '$fixedUrl' CREDENTIALS '$creds' FORMAT AS " +
      "AVRO 'auto' DATEFORMAT 'YYYY-MM-DD HH:MI:SS'"
  }

  /**
   * Sets up a staging table then runs the given action, passing the temporary table name
   * as a parameter.
   */
  private def withStagingTable(
      conn: Connection,
      table: String,
      action: (String) => Unit) {
    val randomSuffix = Math.abs(Random.nextInt()).toString
    val tempTable = s"${table}_staging_$randomSuffix"
    val backupTable = s"${table}_backup_$randomSuffix"

    try {
      log.info("Loading new Redshift data to: " + tempTable)
      log.info("Existing data will be backed up in: " + backupTable)

      action(tempTable)
      if (jdbcWrapper.tableExists(conn, table)) {
        conn.prepareStatement(s"ALTER TABLE $table RENAME TO $backupTable").execute()
      }
      conn.prepareStatement(s"ALTER TABLE $tempTable RENAME TO $table").execute()
    } catch {
      case e: SQLException =>
        if (jdbcWrapper.tableExists(conn, tempTable)) {
          conn.prepareStatement(s"DROP TABLE $tempTable").execute()
        }
        if (jdbcWrapper.tableExists(conn, backupTable)) {
          conn.prepareStatement(s"ALTER TABLE $backupTable RENAME TO $table").execute()
        }
        throw new Exception("Error loading data to Redshift, changes reverted.", e)
    }

    conn.prepareStatement(s"DROP TABLE IF EXISTS $backupTable").execute()
  }

  /**
   * Perform the Redshift load, including deletion of existing data in the case of an overwrite,
   * and creating the table if it doesn't already exist.
   */
  private def doRedshiftLoad(conn: Connection, data: DataFrame, params: MergedParameters): Unit = {

    // Overwrites must drop the table, in case there has been a schema update
    if (params.overwrite) {
      val deleteExisting = conn.prepareStatement(s"DROP TABLE IF EXISTS ${params.table.get}")
      deleteExisting.execute()
    }

    // If the table doesn't exist, we need to create it first, using JDBC to infer column types
    val createStatement = createTableSql(data, params)
    log.info(createStatement)
    val createTable = conn.prepareStatement(createStatement)
    createTable.execute()

    // Load the temporary data into the new file
    val copyStatement = copySql(data.sqlContext, params)
    val copyData = conn.prepareStatement(copyStatement)
    try {
      copyData.execute()
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
          val results = conn.prepareStatement(errorLookupQuery).executeQuery()
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
            logError("Error occurred while querying STL_LOAD_ERRORS", e2)
            None
        }
      throw detailedException.getOrElse(e)
    }

    // Execute postActions
    params.postActions.foreach { action =>
      val actionSql = if (action.contains("%s")) action.format(params.table.get) else action
      log.info("Executing postAction: " + actionSql)
      conn.prepareStatement(actionSql).execute()
    }
  }

  /**
   * Serialize temporary data to S3, ready for Redshift COPY
   */
  private def unloadData(sqlContext: SQLContext, data: DataFrame, tempPath: String): Unit = {
    // spark-avro does not support Date types. In addition, it converts Timestamps into longs
    // (milliseconds since the Unix epoch). Redshift is capable of loading timestamps in
    // 'epochmillisecs' format but there's no equivalent format for dates. To work around this, we
    // choose to write out both dates and timestamps as strings using the same timestamp format.
    // For additional background and discussion, see #39.

    // Convert the rows so that timestamps and dates become formatted strings:
    val conversionFunctions: Array[Any => Any] = data.schema.fields.map { field =>
      field.dataType match {
        case DateType => (v: Any) => v match {
          case null => null
          case t: Timestamp => Conversions.formatTimestamp(t)
          case d: Date => Conversions.formatDate(d)
        }
        case TimestampType => (v: Any) => {
          if (v == null) null else Conversions.formatTimestamp(v.asInstanceOf[Timestamp])
        }
        case _ => (v: Any) => v
      }
    }
    val convertedRows: RDD[Row] = data.map { row =>
      val convertedValues: Array[Any] = new Array(conversionFunctions.length)
      var i = 0
      while (i < conversionFunctions.length) {
        convertedValues(i) = conversionFunctions(i)(row(i))
        i += 1
      }
      Row.fromSeq(convertedValues)
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
      .save(tempPath)
  }

  /**
   * Write a DataFrame to a Redshift table, using S3 and Avro serialization
   */
  def saveToRedshift(sqlContext: SQLContext, data: DataFrame, params: MergedParameters) : Unit = {
    if (params.table.isEmpty) {
      throw new IllegalArgumentException(
        "For save operations you must specify a Redshift table name with the 'dbtable' parameter")
    }

    val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, new Properties()).apply()

    try {
      if (params.overwrite && params.useStagingTable) {
        withStagingTable(conn, params.table.get, stagingTable => {
          val updatedParams = MergedParameters(params.parameters.updated("dbtable", stagingTable))
          unloadData(sqlContext, data, updatedParams.tempPath)
          doRedshiftLoad(conn, data, updatedParams)
        })
      } else {
        unloadData(sqlContext, data, params.tempPath)
        doRedshiftLoad(conn, data, params)
      }
    } finally {
      conn.close()
    }
  }
}

object DefaultRedshiftWriter extends RedshiftWriter(DefaultJDBCWrapper)
