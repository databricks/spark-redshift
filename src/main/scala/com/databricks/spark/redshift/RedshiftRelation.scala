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
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * Data Source API implementation for Amazon Redshift database tables
 */
private[redshift] case class RedshiftRelation(
    jdbcWrapper: JDBCWrapper,
    params: MergedParameters,
    userSchema: Option[StructType])
    (@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation
  with Logging {

  override def schema: StructType = {
    userSchema match {
      case Some(schema) => schema
      case None => {
        jdbcWrapper.registerDriver(params.jdbcDriver)
        jdbcWrapper.resolveTable(params.jdbcUrl, params.table, new Properties())
      }
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Always quote column names:
    val columns = requiredColumns.map(col => s""""$col"""").mkString(", ")
    val whereClause = FilterPushdown.buildWhereClause(schema, filters)
    unloadToTemp(columns, whereClause)
    makeRdd(pruneSchema(schema, requiredColumns))
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val updatedParams =
      Parameters.mergeParameters(params.parameters updated ("overwrite", overwrite.toString))
    new RedshiftWriter(jdbcWrapper).saveToRedshift(sqlContext, data, updatedParams)
  }

  private def unloadToTemp(columnList: String = "*", whereClause: String = ""): Unit = {
    val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, new Properties()).apply()
    val unloadSql = unloadStmnt(columnList, whereClause)
    val statement = conn.prepareStatement(unloadSql)

    statement.execute()
    conn.close()
  }

  private def unloadStmnt(columnList: String, whereClause: String) : String = {
    val credsString = params.credentialsString(sqlContext.sparkContext.hadoopConfiguration)
    val query = s"SELECT $columnList FROM ${params.table} $whereClause"
    val fixedUrl = Utils.fixS3Url(params.tempPath)

    s"UNLOAD ('$query') TO '$fixedUrl' WITH CREDENTIALS '$credsString' ESCAPE ALLOWOVERWRITE"
  }

  private def makeRdd(schema: StructType): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val rdd = sc.newAPIHadoopFile(params.tempPath, classOf[RedshiftInputFormat],
      classOf[java.lang.Long], classOf[Array[String]], sc.hadoopConfiguration)
    rdd.values.map(Conversions.rowConverter(schema))
  }

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }
}
