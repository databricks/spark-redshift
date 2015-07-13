package com.databricks.spark.redshift

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{UUID, Properties}

import com.amazonaws.auth.{AWSCredentials, DefaultAWSCredentialsProviderChain}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.jdbc.{RedshiftJDBCWrapper, DriverRegistry, JDBCRDD}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{UTF8String, TimestampType, StructField, StructType}
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
private [redshift] case class RedshiftRelation(table: String,
                                               jdbcUrl: String,
                                               tempRoot: String,
                                               userSchema: Option[StructType])
                                              (@transient val sqlContext: SQLContext)
  extends BaseRelation
  with TableScan
  with PrunedScan
  with PrunedFilteredScan
  with InsertableRelation
  with Logging {

  val tempPath = Utils.joinUrls(tempRoot, UUID.randomUUID().toString)

  override def schema = {
    userSchema match {
      case Some(schema) => schema
      case None => {
        RedshiftJDBCWrapper.registerDriver(PostgresDriver.CLASS_NAME)
        RedshiftJDBCWrapper.resolveTable(jdbcUrl, table, new Properties())
      }
    }
  }

  val getConnection = RedshiftJDBCWrapper.getConnector(PostgresDriver.CLASS_NAME, jdbcUrl, new Properties())

  override def buildScan(): RDD[Row] = {
    unloadToTemp()
    makeRdd(schema)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val columns = columnList(requiredColumns)
    unloadToTemp(columns)
    makeRdd(pruneSchema(schema, requiredColumns))
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val columns = columnList(requiredColumns)
    val whereClause = buildWhereClause(filters)
    unloadToTemp(columns, whereClause)
    makeRdd(pruneSchema(schema, requiredColumns))
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    RedshiftWriter.saveToRedshift(data, jdbcUrl, table, tempPath, overwrite, getConnection)
  }

  def unloadToTemp(columnList: String = "*", whereClause: String = ""): Unit = {
    val conn = getConnection()
    val unloadSql = unloadStmnt(columnList, whereClause)
    val statement = conn.prepareStatement(unloadSql)

    statement.execute()
    conn.close()
  }

  def unloadStmnt(columnList: String, whereClause: String) : String = {
    val credsString = Utils.credentialsString()
    val query = s"SELECT $columnList FROM $table $whereClause"
    val fixedUrl = Utils.fixS3Url(tempPath)

    s"UNLOAD ('$query') TO '$fixedUrl' WITH CREDENTIALS '$credsString' ESCAPE"
  }

  def convertTimestamp(s: String) : Timestamp = {
    val redshiftDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    new Timestamp(redshiftDateFormat.parse(s).getTime)
  }

  def convertRow(schema: StructType, fields: Array[String]) : Row = {
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

  def makeRdd(schema: StructType): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val rdd = sc.newAPIHadoopFile(tempPath, classOf[RedshiftInputFormat],
      classOf[java.lang.Long], classOf[Array[String]], sc.hadoopConfiguration)
    rdd.values.map(convertRow(schema, _))
  }

  def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields map { x => x.metadata.getString("name") -> x }: _*)
    new StructType(columns map { name => fieldMap(name) })
  }

  def sqlQuote(identifier: String) = s""""$identifier""""

  def columnList(columns: Seq[String]): String = {
    val sb = new StringBuilder()
    columns.foreach(x => sb.append(",").append(sqlQuote(x)))
    if (sb.length == 0) "1" else sb.substring(1)
  }

  def compileValue(value: Any): Any = value match {
    case stringValue: UTF8String => s"\\'${escapeSql(stringValue.toString)}\\'"
    case _ => value
  }

  def escapeSql(value: String): String =
    if (value == null) null else value.replace("'", "''")

  def buildWhereClause(filters: Array[Filter]): String = {
    "WHERE " + ((filters map {
      case EqualTo(attr, value) => s"${sqlQuote(attr)} = ${compileValue(value)}"
      case LessThan(attr, value) => s"${sqlQuote(attr)} < ${compileValue(value)}"
      case GreaterThan(attr, value) => s"${sqlQuote(attr)}) > ${compileValue(value)}"
      case LessThanOrEqual(attr, value) => s"${sqlQuote(attr)} <= ${compileValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"${sqlQuote(attr)} >= ${compileValue(value)}"
    }) mkString "AND")
  }
}
