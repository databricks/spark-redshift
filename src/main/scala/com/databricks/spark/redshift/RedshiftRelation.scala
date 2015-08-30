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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import com.databricks.spark.redshift.Parameters.MergedParameters

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
        val tableNameOrSubquery = params.query.map(q => s"($q)").orElse(params.table).get
        jdbcWrapper.resolveTable(params.jdbcUrl, tableNameOrSubquery)
      }
    }
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val updatedParams =
      Parameters.mergeParameters(params.parameters updated ("overwrite", overwrite.toString))
    new RedshiftWriter(jdbcWrapper).saveToRedshift(sqlContext, data, updatedParams)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    if (requiredColumns.isEmpty) {
      // In the special case where no columns were requested, issue a `count(*)` against Redshift
      // rather than unloading data.
      val whereClause = FilterPushdown.buildWhereClause(schema, filters)
      val tableNameOrSubquery = params.query.map(q => s"($q)").orElse(params.table).get
      val countQuery = s"SELECT count(*) FROM $tableNameOrSubquery $whereClause"
      logInfo(countQuery)
      val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl)
      try {
        val results = conn.prepareStatement(countQuery).executeQuery()
        if (results.next()) {
          val numRows = results.getLong(1)
          val parallelism = sqlContext.getConf("spark.sql.shuffle.partitions", "200").toInt
          val emptyRow = Row.empty
          sqlContext.sparkContext.parallelize(1L to numRows, parallelism).map(_ => emptyRow)
        } else {
          throw new IllegalStateException("Could not read count from Redshift")
        }
      } finally {
        conn.close()
      }
    } else {
      // Unload data from Redshift into a temporary directory in S3:
      val unloadSql = buildUnloadStmt(requiredColumns, filters)
      logInfo(unloadSql)
      val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl)
      try {
        conn.prepareStatement(unloadSql).execute()
      } finally {
        conn.close()
      }
      // Create a DataFrame to read the unloaded data:
      val rdd = sqlContext.sparkContext.newAPIHadoopFile(
        params.tempPathWithCredentials(sqlContext.sparkContext.hadoopConfiguration),
        classOf[RedshiftInputFormat],
        classOf[java.lang.Long],
        classOf[Array[String]])
      val prunedSchema = pruneSchema(schema, requiredColumns)
      rdd.values.mapPartitions { iter =>
        val converter: Array[String] => Row = Conversions.createRowConverter(prunedSchema)
        iter.map(converter)
      }
    }
  }

  private def buildUnloadStmt(requiredColumns: Array[String], filters: Array[Filter]): String = {
    assert(!requiredColumns.isEmpty)
    // Always quote column names:
    val columnList = requiredColumns.map(col => s""""$col"""").mkString(", ")
    val whereClause = FilterPushdown.buildWhereClause(schema, filters)
    val credsString = params.credentialsString(sqlContext.sparkContext.hadoopConfiguration)
    val query = {
      // Since the query passed to UNLOAD will be enclosed in single quotes, we need to escape
      // any single quotes that appear in the query itself
      val tableNameOrSubquery: String = {
        val unescaped = params.query.map(q => s"($q)").orElse(params.table).get
        unescaped.replace("'", "\\'")
      }
      s"SELECT $columnList FROM $tableNameOrSubquery $whereClause"
    }
    val fixedUrl = Utils.fixS3Url(params.tempPath)

    s"UNLOAD ('$query') TO '$fixedUrl' WITH CREDENTIALS '$credsString' ESCAPE ALLOWOVERWRITE"
  }

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }
}
