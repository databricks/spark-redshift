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

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
  * End-to-end tests for loading/unloading data from Redshift using Avro
  * compression.
  */
class CompressionIntegrationSuite extends IntegrationSuiteBase {

  test("roundtrip save and load with Avro snappy compression") {
    val tableName = s"roundtrip_save_and_load$randomSuffix"
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
      StructType(StructField("a", IntegerType) :: Nil))
    try {
      df.write
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("tempdir", tempDir)
        .option("avrocompression", "snappy")
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = sqlContext.read
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("tempdir", tempDir)
        .option("avrocompression", "snappy")
        .load()
      assert(loadedDf.schema.length === 1)
      assert(loadedDf.columns === Seq("a"))
      checkAnswer(loadedDf, Seq(Row(1)))
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }

  test("roundtrip save and load with Avro deflate compression") {
    val tableName = s"roundtrip_save_and_load$randomSuffix"
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
      StructType(StructField("a", IntegerType) :: Nil))
    try {
      df.write
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("tempdir", tempDir)
        .option("avrocompression", "deflate")
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = sqlContext.read
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("tempdir", tempDir)
        .option("avrocompression", "deflate")
        .load()
      assert(loadedDf.schema.length === 1)
      assert(loadedDf.columns === Seq("a"))
      checkAnswer(loadedDf, Seq(Row(1)))
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }
}
