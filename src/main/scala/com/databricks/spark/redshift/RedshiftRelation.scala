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

import java.util.Properties

import com.databricks.spark.redshift.Parameters.MergedParameters
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.jdbc.RedshiftJDBCWrapper
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * Data Source API implementation for Amazon Redshift database tables
 */
private [redshift]
case class RedshiftRelation(params: MergedParameters,
                            userSchema: Option[StructType])
                           (@transient val sqlContext: SQLContext)
  extends BaseRelation
  with TableScan
  with PrunedScan
  with PrunedFilteredScan
  with InsertableRelation
  with Logging {

  override def schema = {
    userSchema match {
      case Some(schema) => schema
      case None => {
        RedshiftJDBCWrapper.registerDriver(params.jdbcDriver)
        RedshiftJDBCWrapper.resolveTable(params.jdbcUrl, params.table, new Properties())
      }
    }
  }

  val getConnection = RedshiftJDBCWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, new Properties())

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
    RedshiftWriter.saveToRedshift(data, params, getConnection)
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
    val query = s"SELECT $columnList FROM ${params.table} $whereClause"
    val fixedUrl = Utils.fixS3Url(params.tempPath)

    s"UNLOAD ('$query') TO '$fixedUrl' WITH CREDENTIALS '$credsString' ESCAPE ALLOWOVERWRITE"
  }

  def makeRdd(schema: StructType): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val rdd = sc.newAPIHadoopFile(params.tempPath, classOf[RedshiftInputFormat],
      classOf[java.lang.Long], classOf[Array[String]], sc.hadoopConfiguration)
    rdd.values.map(Conversions.rowConverter(schema))
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
    val filterClauses = filters map {
      case EqualTo(attr, value) => s"${sqlQuote(attr)} = ${compileValue(value)}"
      case LessThan(attr, value) => s"${sqlQuote(attr)} < ${compileValue(value)}"
      case GreaterThan(attr, value) => s"${sqlQuote(attr)}) > ${compileValue(value)}"
      case LessThanOrEqual(attr, value) => s"${sqlQuote(attr)} <= ${compileValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"${sqlQuote(attr)} >= ${compileValue(value)}"
    } mkString "AND"

    if (filterClauses.isEmpty) "" else "WHERE " + filterClauses
  }
}
