/*
 * Copyright 2015 Databricks
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

package com.databricks.spark

import java.util.{Properties, UUID}

import com.databricks.spark.redshift.RedshiftRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jdbc.RedshiftJDBCWrapper
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

package object redshift {

  /**
   * Wrapper of SQLContext that provide `redshiftFile` method.
   */
  implicit class RedshiftContext(sqlContext: SQLContext) {

    /**
     * Read a file unloaded from Redshift into a DataFrame.
     * @param path input path
     * @return a DataFrame with all string columns
     */
    def redshiftFile(path: String, columns: Seq[String]): DataFrame = {
      val sc = sqlContext.sparkContext
      val rdd = sc.newAPIHadoopFile(path, classOf[RedshiftInputFormat],
        classOf[java.lang.Long], classOf[Array[String]], sc.hadoopConfiguration)
      // TODO: allow setting NULL string.
      val nullable = rdd.values.map(_.map(f => if (f.isEmpty) null else f)).map(x => Row(x: _*))
      val schema = StructType(columns.map(c => StructField(c, StringType, nullable = true)))
      sqlContext.createDataFrame(nullable, schema)
    }

    /**
     * Reads a table unload from Redshift with its schema in format "name0 type0 name1 type1 ...".
     */
    def redshiftFile(path: String, schema: String): DataFrame = {
      val structType = SchemaParser.parseSchema(schema)
      val casts = structType.fields.map { field =>
        col(field.name).cast(field.dataType).as(field.name)
      }
      redshiftFile(path, structType.fieldNames).select(casts: _*)
    }

    def redshiftTable(url: String, table: String, tempPath: String)
    = sqlContext.baseRelationToDataFrame(RedshiftRelation(table, url, tempPath)(sqlContext))
  }

  /**
   * Add write functionality to DataFrame
   */
  implicit class RedshiftDataFrame(dataFrame: DataFrame) {

    /**
     * Load the DataFrame into a Redshift database table
     *
     * @param table The name of the table
     * @param jdbcUrl URL for the table - must include credentials using ?user=username&password=password
     * @param tempDir S3 path to use a cache for data being loaded into Redshift
     * @param overwrite If true, first truncate the table before copying new data in
     */
    def saveAsRedshiftTable(table: String, jdbcUrl: String, tempDir: String, overwrite: Boolean = false): Unit = {
      val tempPath = Utils.joinUrls(tempDir, UUID.randomUUID().toString)
      val getConnection = RedshiftJDBCWrapper.getConnector(PostgresDriver.CLASS_NAME, jdbcUrl, new Properties())
      RedshiftWriter.saveToRedshift(dataFrame, jdbcUrl, table, tempPath, overwrite, getConnection)
    }
  }
}
