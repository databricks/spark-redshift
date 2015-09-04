/*
 * Copyright 2015 Databricks
 * Copyright 2015 TouchType Ltd. (Added JDBC-based Data Source API implementation)
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

import com.amazonaws.services.s3.AmazonS3Client
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SQLContext}

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
    @deprecated("Use data sources API or perform string -> data type casts yourself", "0.5.0")
    def redshiftFile(path: String, schema: String): DataFrame = {
      val structType = SchemaParser.parseSchema(schema)
      val casts = structType.fields.map { field =>
        col(field.name).cast(field.dataType).as(field.name)
      }
      redshiftFile(path, structType.fieldNames).select(casts: _*)
    }

    /**
     * Read a Redshift table into a DataFrame, using S3 for data transfer and JDBC
     * to control Redshift and resolve the schema
     */
    @deprecated("Use sqlContext.read()", "0.5.0")
    def redshiftTable(parameters: Map[String, String]): DataFrame = {
      val params = Parameters.mergeParameters(parameters)
      sqlContext.baseRelationToDataFrame(
        RedshiftRelation(
          DefaultJDBCWrapper, creds => new AmazonS3Client(creds), params, None)(sqlContext))
    }
  }

  /**
   * Add write functionality to DataFrame
   */
  @deprecated("Use DataFrame.write()", "0.5.0")
  implicit class RedshiftDataFrame(dataFrame: DataFrame) {

    /**
     * Load the DataFrame into a Redshift database table
     */
    @deprecated("Use DataFrame.write()", "0.5.0")
    def saveAsRedshiftTable(parameters: Map[String, String]): Unit = {
      val params = Parameters.mergeParameters(parameters)
      DefaultRedshiftWriter.saveToRedshift(dataFrame.sqlContext, dataFrame, params)
    }
  }
}
