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

import org.apache.spark.Logging
import org.apache.spark.sql.jdbc.{DefaultJDBCWrapper, JDBCWrapper}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * Redshift Source implementation for Spark SQL
 */
class DefaultSource(jdbcWrapper: JDBCWrapper)
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider
  with Logging {

  /**
   * Default constructor required by Data Source API
   */
  def this() = this(DefaultJDBCWrapper)

  /**
   * Create a new RedshiftRelation instance using parameters from Spark SQL DDL. Resolves the schema using
   * JDBC connection over provided URL, which must contain credentials.
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val params = Parameters.mergeParameters(parameters)
    RedshiftRelation(jdbcWrapper, params, None)(sqlContext)
  }

  /**
   * Load a RedshiftRelation using user-provided schema, so no inference over JDBC will be used.
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val params = Parameters.mergeParameters(parameters)
    RedshiftRelation(jdbcWrapper, params, Some(schema))(sqlContext)
  }

  /**
   * Creates a Relation instance by first writing the contents of the given DataFrame to Redshift
   */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String],
    data: DataFrame): BaseRelation = {
    val params = Parameters.mergeParameters(parameters)

    def tableExists: Boolean = {
      val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, new Properties()).apply()
      val exists = jdbcWrapper.tableExists(conn, params.table)
      conn.close()
      exists
    }

    val (doSave, dropExisting) = mode match {
      case SaveMode.Append => (true, false)
      case SaveMode.Overwrite => (true, true)
      case SaveMode.ErrorIfExists =>
        if(tableExists) {
          sys.error(s"Table ${params.table} already exists! (SaveMode is set to ErrorIfExists)")
        } else {
          (true, false)
        }
      case SaveMode.Ignore =>
        if(tableExists) {
          log.info(s"Table ${params.table} already exists -- ignoring save request.")
          (false, false)
        } else {
          (true, false)
        }
    }

    if(doSave) {
      val updatedParams = params.updated("overwrite", dropExisting.toString)
      new RedshiftWriter(jdbcWrapper).saveToRedshift(sqlContext, data, updatedParams)
    }

    createRelation(sqlContext, parameters)
  }
}
