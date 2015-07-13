package com.databricks.spark.redshift

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
 * Redshift Source implementation for Spark SQL
 */
class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider {

  def checkTempPath(params: Map[String, String]) = {
    params.getOrElse("tempPath", sys.error("'tempPath' is required for all Redshift loads and saves"))
  }

  def checkTable(params: Map[String, String]) = {
    params.getOrElse("redshiftTable", sys.error("You must specify a Redshift table name with 'redshiftTable' parameter"))
  }

  def checkUrl(params: Map[String, String]) = {
    params.getOrElse("jdbcUrl", sys.error("A JDBC URL must be provided with 'jdbcUrl' parameter"))
  }

  /**
   * Create a new RedshiftRelation instance using parameters from Spark SQL DDL
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    RedshiftRelation(checkTable(parameters), checkUrl(parameters), checkTempPath(parameters), None)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    RedshiftRelation(checkTable(parameters), checkUrl(parameters), checkTempPath(parameters), Some(schema))(sqlContext)
  }
}
