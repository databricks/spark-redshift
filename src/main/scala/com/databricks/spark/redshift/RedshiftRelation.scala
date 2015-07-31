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
import org.apache.spark.sql.jdbc.JDBCWrapper
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Data Source API implementation for Amazon Redshift database tables
 */
private [redshift]
case class RedshiftRelation(jdbcWrapper: JDBCWrapper, params: MergedParameters, userSchema: Option[StructType])
    (@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation
  with Logging {

  override def schema = userSchema match {
    case Some(schema) => schema
    case None =>
      jdbcWrapper.registerDriver(params.jdbcDriver)
      jdbcWrapper.resolveTable(params.jdbcUrl, params.tableOrQuery, new Properties())
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val columns = columnList(requiredColumns)
    val whereClause = buildWhereClause(filters)
    unloadToTemp(columns, whereClause)
    makeRdd(pruneSchema(schema, requiredColumns))
  }

  // Redshift COPY using Avro format only supports lowercase columns
  private[this] def toLowercaseColumns(data: DataFrame): DataFrame = {
    val allColumns = new collection.mutable.HashSet[String]()
    val cols = data.schema.fields.map { field =>
      val lowercase = field.name.toLowerCase
      if (allColumns.contains(lowercase)) {
        sys.error("Redshift COPY using Avro format only supports lowercase columns. " +
          "Converting all column names to lowercase causes ambiguity...")
      } else {
        allColumns += lowercase
      }
      data.col(field.name).as(lowercase)
    }
    data.select(cols: _*)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val updatedParams = params.updated("overwrite", overwrite.toString)
    new RedshiftWriter(jdbcWrapper).saveToRedshift(sqlContext, toLowercaseColumns(data), updatedParams)
  }

  protected def unloadToTemp(columnList: String = "*", whereClause: String = ""): Unit = {
    val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, new Properties()).apply()
    val unloadSql = unloadStmnt(columnList, whereClause)
    val statement = conn.prepareStatement(unloadSql)

    statement.execute()
    conn.close()
  }

  protected def unloadStmnt(columnList: String, whereClause: String) : String = {
    val unloadTo = params.tempPathForRedshift(sqlContext.sparkContext.hadoopConfiguration)
    val query = s"SELECT $columnList FROM ${params.tableOrQuery} $whereClause"

    s"UNLOAD ('$query') TO $unloadTo ESCAPE ALLOWOVERWRITE"
  }

  protected def makeRdd(schema: StructType): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val rdd = sc.newAPIHadoopFile(params.tempPath, classOf[RedshiftInputFormat],
      classOf[java.lang.Long], classOf[Array[String]], sc.hadoopConfiguration)
    rdd.values.map(Conversions.rowConverter(schema))
  }

  protected def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields map { x => x.name -> x }: _*)
    new StructType(columns map { name => fieldMap(name) })
  }

  protected def sqlQuote(identifier: String) = s""""$identifier""""

  protected def columnList(columns: Seq[String]): String = {
    columns map sqlQuote mkString ", "
  }

  protected def compileValue(value: Any): Any = value match {
    case stringValue: UTF8String => s"\\'${escapeSql(stringValue.toString)}\\'"
    case _ => value
  }

  protected def escapeSql(value: String): String =
    if (value == null) null else value.replace("'", "\\'\\'")

  protected def buildWhereClause(filters: Array[Filter]): String = {
    val filterClauses = filters map {
      case EqualTo(attr, value) => s"${sqlQuote(attr)} = ${compileValue(value)}"
      case LessThan(attr, value) => s"${sqlQuote(attr)} < ${compileValue(value)}"
      case GreaterThan(attr, value) => s"${sqlQuote(attr)}) > ${compileValue(value)}"
      case LessThanOrEqual(attr, value) => s"${sqlQuote(attr)} <= ${compileValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"${sqlQuote(attr)} >= ${compileValue(value)}"
    } mkString " AND "

    if (filterClauses.isEmpty) "" else "WHERE " + filterClauses
  }
}
