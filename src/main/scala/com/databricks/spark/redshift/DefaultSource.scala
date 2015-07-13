package com.databricks.spark.redshift

import java.util.Properties

import org.apache.spark.Logging
import org.apache.spark.sql.jdbc.RedshiftJDBCWrapper
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{CreatableRelationProvider, BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
 * Redshift Source implementation for Spark SQL
 */
class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider
  with Logging {

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
   * Create a new RedshiftRelation instance using parameters from Spark SQL DDL. Resolves the schema using
   * JDBC connection over provided URL, which must contain credentials.
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    RedshiftRelation(checkTable(parameters), checkUrl(parameters), checkTempPath(parameters), None)(sqlContext)
  }

  /**
   * Load a RedshiftRelation using user-provided schema, so no inference over JDBC will be used.
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    RedshiftRelation(checkTable(parameters), checkUrl(parameters), checkTempPath(parameters), Some(schema))(sqlContext)
  }

  /**
   * Creates a Relation instance by first writing the contents of the given DataFrame to Redshift
   */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val url = checkUrl(parameters)
    val tempPath = checkTempPath(parameters)
    val table = checkTable(parameters)
    val getConnection = RedshiftJDBCWrapper.getConnector(PostgresDriver.CLASS_NAME, url, new Properties())
    val conn = getConnection()

    val (doSave, dropExisting) = mode match {
      case SaveMode.Append => (true, false)
      case SaveMode.Overwrite => (true, true)
      case SaveMode.ErrorIfExists =>
        if(RedshiftJDBCWrapper.tableExists(conn, table)) {
          sys.error(s"Table $table already exists! (SaveMode is set to ErrorIfExists)")
        } else {
          (true, false)
        }
      case SaveMode.Ignore =>
        if(RedshiftJDBCWrapper.tableExists(conn, table)) {
          log.info(s"Table $table already exists -- ignoring save request.")
          (false, false)
        } else {
          (true, false)
        }
    }

    if(doSave) {
      RedshiftWriter.saveToRedshift(data, url, table, tempPath, dropExisting, getConnection)
    }

    createRelation(sqlContext, parameters)
  }
}
