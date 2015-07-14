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

  /**
   * Create a new RedshiftRelation instance using parameters from Spark SQL DDL. Resolves the schema using
   * JDBC connection over provided URL, which must contain credentials.
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val params = Parameters.mergeParameters(parameters)
    RedshiftRelation(params, None)(sqlContext)
  }

  /**
   * Load a RedshiftRelation using user-provided schema, so no inference over JDBC will be used.
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val params = Parameters.mergeParameters(parameters)
    RedshiftRelation(params, Some(schema))(sqlContext)
  }

  /**
   * Creates a Relation instance by first writing the contents of the given DataFrame to Redshift
   */
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val params = Parameters.mergeParameters(parameters)
    val getConnection = RedshiftJDBCWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, new Properties())
    val conn = getConnection()

    val (doSave, dropExisting) = mode match {
      case SaveMode.Append => (true, false)
      case SaveMode.Overwrite => (true, true)
      case SaveMode.ErrorIfExists =>
        if(RedshiftJDBCWrapper.tableExists(conn, params.table)) {
          sys.error(s"Table ${params.table} already exists! (SaveMode is set to ErrorIfExists)")
        } else {
          (true, false)
        }
      case SaveMode.Ignore =>
        if(RedshiftJDBCWrapper.tableExists(conn, params.table)) {
          log.info(s"Table ${params.table} already exists -- ignoring save request.")
          (false, false)
        } else {
          (true, false)
        }
    }

    if(doSave) {
      val updatedParams = parameters updated ("overwrite", dropExisting.toString)
      RedshiftWriter.saveToRedshift(data, Parameters.mergeParameters(updatedParams), getConnection)
    }

    createRelation(sqlContext, parameters)
  }
}
