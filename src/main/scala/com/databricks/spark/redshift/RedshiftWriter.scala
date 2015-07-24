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

import java.sql.{Connection, SQLException}
import java.util.Properties

import com.databricks.spark.redshift.Parameters.MergedParameters
import org.apache.spark.Logging
import org.apache.spark.sql.jdbc.{DefaultJDBCWrapper, JDBCWrapper}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.Random

/**
 * Functions to write data to Redshift with intermediate Avro serialisation into S3.
 */
class RedshiftWriter(jdbcWrapper: JDBCWrapper) extends Logging {

  /**
   * Generate CREATE TABLE statement for Redshift
   */
  def createTableSql(data: DataFrame, params: MergedParameters) : String = {
    val schemaSql = jdbcWrapper.schemaString(data, params.jdbcUrl)
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
  def copySql(sqlContext: SQLContext, params: MergedParameters) = {
    val copyFrom = params.tempPathForRedshift(sqlContext.sparkContext.hadoopConfiguration)
    s"COPY ${params.table} FROM $copyFrom FORMAT AS AVRO 'auto' TIMEFORMAT 'epochmillisecs'"
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
      case e: SQLException =>
        if (jdbcWrapper.tableExists(conn, tempTable)) {
          conn.prepareStatement(s"DROP TABLE $tempTable").execute()
        }
        if (jdbcWrapper.tableExists(conn, backupTable)) {
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
    val copyStatement = copySql(data.sqlContext, params)
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
  def unloadData(sqlContext: SQLContext, data: DataFrame, tempPath: String): Unit = {
    Conversions.datesToTimestamps(sqlContext, data).write.format("com.databricks.spark.avro").save(tempPath)
  }

  /**
   * Write a DataFrame to a Redshift table, using S3 and Avro serialization
   */
  def saveToRedshift(sqlContext: SQLContext, data: DataFrame, params: MergedParameters) : Unit = {
    val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, new Properties()).apply()

    try {
      if(params.overwrite && params.useStagingTable) {
        withStagingTable(conn, params, table => {
          val updatedParams = params.updated("dbtable", table)
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
