package com.databricks.spark.redshift

import java.sql.Connection

import com.databricks.spark.redshift.Parameters.MergedParameters
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.jdbc.RedshiftJDBCWrapper

import scala.util.Random

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
   * Sets up a staging table then runs the given action, passing the temporary table name
   * as a parameter.
   */
  def withStagingTable(conn:Connection, params: MergedParameters, action: (String) => Unit) {
    val randomSuffix = Math.abs(Random.nextInt()).toString
    val tempTable = s"${params.table}_staging_$randomSuffix"
    val backupTable = s"${params.table}_backup_$randomSuffix"

    try {
      log.info("Loading new Redshift data to: " + tempTable)
      log.info("Existing data will be backed up in: " + backupTable)

      action(tempTable)
      conn.prepareStatement(s"ALTER TABLE ${params.table} RENAME TO $backupTable").execute()
      conn.prepareStatement(s"ALTER TABLE $tempTable RENAME TO ${params.table}").execute()
    } catch {
      case e: Exception =>
        if (RedshiftJDBCWrapper.tableExists(conn, tempTable)) {
          conn.prepareStatement(s"DROP TABLE $tempTable").execute()
        }
        if (RedshiftJDBCWrapper.tableExists(conn, backupTable)) {
          conn.prepareStatement(s"ALTER TABLE $backupTable RENAME TO ${params.table}").execute()
        }
        throw new Exception("Error loading data to Redshift, changes reverted.", e)
    }

    conn.prepareStatement(s"DROP TABLE $backupTable").execute()
  }

  /**
   * Perform the Redshift load, including deletion of existing data in the case of an overwrite,
   * and creating the table if it doesn't already exist.
   */
  def doRedshiftLoad(conn: Connection, data: DataFrame, params: MergedParameters) : Unit = {

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
    val copyStatement = copySql(params.table, params.tempPath)
    val copyData = conn.prepareStatement(copyStatement)
    copyData.execute()

    // Execute postActions
    params.postActions.foreach(action => {
      val actionSql = if(action.contains("%s")) action.format(params.table) else action
      log.info("Executing postAction: " + actionSql)
      conn.prepareStatement(actionSql).execute()
    })
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

    if(params.overwrite && params.useStagingTable) {
      withStagingTable(conn, params, table => {
        val updatedParams = MergedParameters(params.parameters updated ("redshifttable", table))
        unloadData(data, updatedParams.tempPath)
        doRedshiftLoad(conn, data, updatedParams)
      })
    } else {
      unloadData(data, params.tempPath)
      doRedshiftLoad(conn, data, params)
    }

    conn.close()
  }
}
