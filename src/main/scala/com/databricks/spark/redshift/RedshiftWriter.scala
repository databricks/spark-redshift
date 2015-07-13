package com.databricks.spark.redshift

import java.sql.Connection

import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.jdbc.RedshiftJDBCWrapper

/**
 * Functions to write data to Redshift with intermediate Avro serialisation into S3.
 */
object RedshiftWriter extends Logging {

  def createTableSql(data: DataFrame, jdbcUrl: String, table: String) : String = {
    val schemaSql = RedshiftJDBCWrapper.schemaString(data, jdbcUrl)
    s"CREATE TABLE IF NOT EXISTS $table ($schemaSql)"
  }

  def saveToRedshift(data: DataFrame, jdbcUrl: String, table: String,
                    tempPath: String, overWrite: Boolean,
                    getConnection: () => Connection) : Unit = {
    val conn = getConnection()
    val createTable = conn.prepareStatement(createTableSql(data, jdbcUrl, table))
    createTable.execute()

    data.write.format("com.databricks.spark.avro").save(tempPath)

    if(overWrite) {
      val deleteExisting = conn.prepareStatement(s"TRUNCATE TABLE $table")
      deleteExisting.execute()
    }

    val credsString = Utils.credentialsString()
    val fixedUrl = Utils.fixS3Url(tempPath)
    val copySql = s"COPY $table FROM '$fixedUrl' CREDENTIALS '$credsString' FORMAT AS AVRO 'auto'"
    val copyData = conn.prepareStatement(copySql)

    copyData.execute()
    conn.close()
  }
}
