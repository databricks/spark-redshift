package com.databricks.spark.redshift

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{UUID, Properties}

import com.amazonaws.auth.{AWSCredentials, DefaultAWSCredentialsProviderChain}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.jdbc.{RedshiftJDBCWrapper, DriverRegistry, JDBCRDD}
import org.apache.spark.sql.sources.{InsertableRelation, BaseRelation, TableScan}
import org.apache.spark.sql.types.{TimestampType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * Constants for JDBC depdendencies
 */
object PostgresDriver {
  val CLASS_NAME = "org.postgresql.Driver"
}

/**
 * Data Source API implementation for Amazon Redshift database tables
 */
private [redshift] case class RedshiftRelation(table: String, jdbcUrl: String,  tempRoot: String)
                                              (@transient val sqlContext: SQLContext)
  extends BaseRelation
  with TableScan
  with Logging {

  val tempPath = Utils.joinUrls(tempRoot, UUID.randomUUID().toString)

  override def schema = {
    RedshiftJDBCWrapper.registerDriver(PostgresDriver.CLASS_NAME)
    RedshiftJDBCWrapper.resolveTable(jdbcUrl, table, new Properties())
  }

  val getConnection = RedshiftJDBCWrapper.getConnector(PostgresDriver.CLASS_NAME, jdbcUrl, new Properties())

  override def buildScan(): RDD[Row] = {
    unloadToTemp()
    makeRdd()
  }

  def unloadToTemp(): Unit = {
    val conn = getConnection()
    val statement = conn.prepareStatement(unloadStmnt())
    statement.execute()
    conn.close()
  }

  def unloadStmnt() : String = {
    val credsString = Utils.credentialsString()
    val query = s"SELECT * FROM $table"

    s"UNLOAD ('$query') TO '$tempPath' WITH CREDENTIALS '$credsString' ESCAPE ALLOWOVERWRITE"
  }

  def convertTimestamp(s: String) : Timestamp = {
    val redshiftDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    new Timestamp(redshiftDateFormat.parse(s).getTime)
  }

  def convertRow(fields: Array[String]) : Row = {
    val converted = fields zip schema map {
      case (data, field) =>
        if(data.isEmpty) null else field.dataType match {
          case TimestampType => convertTimestamp(data)

          // TODO: More conversions will be needed
          case _ => data
      }
    }

    Row(converted: _*)
  }

  def makeRdd(): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val rdd = sc.newAPIHadoopFile(tempPath.replace("s3://", "s3n://"), classOf[RedshiftInputFormat],
      classOf[java.lang.Long], classOf[Array[String]], sc.hadoopConfiguration)
    rdd.values.map(convertRow)
  }
}
