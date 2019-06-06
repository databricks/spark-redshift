/*
 * Copyright 2016 Databricks
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

import org.apache.spark.sql.{SaveMode, Row}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * End-to-end tests of [[SaveMode]] behavior.
 */
class SaveModeIntegrationSuite extends IntegrationSuiteBase {
  test("SaveMode.Overwrite with schema-qualified table name (#97)") {
    withTempRedshiftTable("overwrite_schema_qualified_table_name") { tableName =>
      val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("a", IntegerType) :: Nil))
      // Ensure that the table exists:
      write(df)
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(DefaultJDBCWrapper.tableExists(conn, s"PUBLIC.$tableName"))
      // Try overwriting that table while using the schema-qualified table name:
      write(df)
        .option("dbtable", s"PUBLIC.$tableName")
        .mode(SaveMode.Overwrite)
        .save()
    }
  }

  test("SaveMode.Overwrite with non-existent table") {
    testRoundtripSaveAndLoad(
      s"overwrite_non_existent_table$randomSuffix",
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("a", IntegerType) :: Nil)),
      saveMode = SaveMode.Overwrite)
  }

  test("SaveMode.Overwrite with existing table") {
    withTempRedshiftTable("overwrite_existing_table") { tableName =>
      // Create a table to overwrite
      write(sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("a", IntegerType) :: Nil)))
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))

      val overwritingDf =
        sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema)
      write(overwritingDf)
        .option("dbtable", tableName)
        .mode(SaveMode.Overwrite)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), TestUtils.expectedData)
    }
  }

  // TODO:test overwrite that fails.

  // TODO (luca|) make SaveMode work
  ignore("Append SaveMode doesn't destroy existing data") {
    withTempRedshiftTable("append_doesnt_destroy_existing_data") { tableName =>
      createTestDataInRedshift(tableName)
      val extraData = Seq(
        Row(2.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L,
          24.toShort, "___|_123", null))

      write(sqlContext.createDataFrame(sc.parallelize(extraData), TestUtils.testSchema))
        .option("dbtable", tableName)
        .mode(SaveMode.Append)
        .saveAsTable(tableName)

      checkAnswer(
        sqlContext.sql(s"select * from $tableName"),
        TestUtils.expectedData ++ extraData)
    }
  }

  ignore("Respect SaveMode.ErrorIfExists when table exists") {
    withTempRedshiftTable("respect_savemode_error_if_exists") { tableName =>
      val rdd = sc.parallelize(TestUtils.expectedData)
      val df = sqlContext.createDataFrame(rdd, TestUtils.testSchema)
      createTestDataInRedshift(tableName) // to ensure that the table already exists

      // Check that SaveMode.ErrorIfExists throws an exception
      val e = intercept[Exception] {
        write(df)
          .option("dbtable", tableName)
          .mode(SaveMode.ErrorIfExists)
          .saveAsTable(tableName)
      }
      assert(e.getMessage.contains("exists"))
    }
  }

  ignore("Do nothing when table exists if SaveMode = Ignore") {
    withTempRedshiftTable("do_nothing_when_savemode_ignore") { tableName =>
      val rdd = sc.parallelize(TestUtils.expectedData.drop(1))
      val df = sqlContext.createDataFrame(rdd, TestUtils.testSchema)
      createTestDataInRedshift(tableName) // to ensure that the table already exists
      write(df)
        .option("dbtable", tableName)
        .mode(SaveMode.Ignore)
        .saveAsTable(tableName)

      // Check that SaveMode.Ignore does nothing
      checkAnswer(
        sqlContext.sql(s"select * from $tableName"),
        TestUtils.expectedData)
    }
  }
}
