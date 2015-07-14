package com.databricks.spark.redshift

import java.sql.Connection

import com.databricks.spark.redshift.Parameters.MergedParameters
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.jdbc.RedshiftJDBCWrapper

/**
 * Functions to write data to Redshift with intermediate Avro serialisation into S3.
 */
object RedshiftWriter extends Logging {

  /**
   * Generate CREATE TABLE statement for Redshift
   */
  def createTableSql(data: DataFrame, params: MergedParameters) : String = {
    val schemaSql = RedshiftJDBCWrapper.schemaString(data, params.jdbcUrl)
    val distStyleDef = params.distStyle match {
      case Some(style) => s"DISTSTYLE $style"
      case None => ""
    }
    val distKeyDef = params.distKey match {
      case Some(key) => s"DISTKEY ($key)"
      case None => ""
    }
    val sortKeyDef = params.sortKeySpec.getOrElse("")

    s"CREATE TABLE IF NOT EXISTS ${params.table} ($schemaSql) $distStyleDef $distKeyDef $sortKeyDef"
  }

  /**
   * Generate the COPY SQL command
   */
  def copySql(table: String, path: String) = {
    val credsString = Utils.credentialsString()
    val fixedUrl = Utils.fixS3Url(path)
    s"COPY $table FROM '$fixedUrl' CREDENTIALS '$credsString' FORMAT AS AVRO 'auto' TIMEFORMAT 'epochmillisecs'"
  }

  /**
   * Perform the Redshift load, including deletion of existing data in the case of an overwrite,
   * and creating the table if it doesn't already exist.
   */
  def doRedshiftLoad(conn: Connection, data: DataFrame, tempPath: String, params: MergedParameters) : Unit = {

    // Overwrites must drop the table, in case there has been a schema update
    if(params.overwrite) {
      val deleteExisting = conn.prepareStatement(s"DROP TABLE IF EXISTS ${params.table}")
      deleteExisting.execute()
    }

    // If the table doesn't exist, we need to create it first, using JDBC to infer column types
    val createStatement = createTableSql(data, params)
    log.info(createStatement)
    val createTable = conn.prepareStatement(createStatement)
    createTable.execute()

    // Load the temporary data into the new file
    val copyStatement = copySql(params.table, tempPath)
    log.info(copyStatement)
    val copyData = conn.prepareStatement(copyStatement)
    copyData.execute()
  }

  /**
   * Serialize temporary data to S3, ready for Redshift COPY
   */
  def unloadData(data: DataFrame, tempPath: String): Unit = {
    data.write.format("com.databricks.spark.avro").save(tempPath)
  }

  /**
   * Write a DataFrame to a Redshift table, using S3 and Avro serialization
   */
  def saveToRedshift(data: DataFrame, params: MergedParameters, getConnection: () => Connection) : Unit = {
    val conn = getConnection()

    unloadData(data, params.tempPath)
    doRedshiftLoad(conn, data, params.tempPath, params)
    conn.close()
  }
}
