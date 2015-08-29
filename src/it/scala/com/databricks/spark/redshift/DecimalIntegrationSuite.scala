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

package com.databricks.spark.redshift

import org.apache.spark.sql.Row

/**
 * Integration tests for decimal support. For a reference on Redshift's DECIMAL type, see
 * http://docs.aws.amazon.com/redshift/latest/dg/r_Numeric_types201.html
 */
class DecimalIntegrationSuite extends IntegrationSuiteBase {

  test("reading DECIMAL(19, 0)") {
    val tableName = s"reading_decimal_19_0_$randomSuffix"
    val decimals = Seq(
      // Max and min values of DECIMAL(19, 0) column according to Redshift docs:
      "9223372036854775807", // 2^63 - 1
      "-9223372036854775807",
      "0",
      "12345678910",
      null
    )
    val expectedRows = decimals.map(d => Row(if (d == null) null else Conversions.parseDecimal(d)))
    try {
      conn.createStatement().executeUpdate(s"CREATE TABLE $tableName (x DECIMAL(19, 0))")
      for (x <- decimals) {
        conn.createStatement().executeUpdate(s"INSERT INTO $tableName VALUES ($x)")
      }
      conn.commit()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = sqlContext.read
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("tempdir", tempDir)
        .option("aws_access_key_id", AWS_ACCESS_KEY_ID)
        .option("aws_secret_access_key", AWS_SECRET_ACCESS_KEY)
        .load()
      checkAnswer(loadedDf, expectedRows)
      checkAnswer(loadedDf.selectExpr("x + 0"), expectedRows)
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }

  test("reading DECIMAL(19, 4)") {
    val tableName = s"reading_decimal_19_4_$randomSuffix"
    val decimals = Seq(
      "922337203685477.5807",
      "-922337203685477.5807",
      "0",
      "1234567.8910",
      null
    )
    val expectedRows = decimals.map(d => Row(if (d == null) null else Conversions.parseDecimal(d)))
    try {
      conn.createStatement().executeUpdate(s"CREATE TABLE $tableName (x DECIMAL(19, 4))")
      for (x <- decimals) {
        conn.createStatement().executeUpdate(s"INSERT INTO $tableName VALUES ($x)")
      }
      conn.commit()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = sqlContext.read
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("tempdir", tempDir)
        .option("aws_access_key_id", AWS_ACCESS_KEY_ID)
        .option("aws_secret_access_key", AWS_SECRET_ACCESS_KEY)
        .load()
      checkAnswer(loadedDf, expectedRows)
      checkAnswer(loadedDf.selectExpr("x + 0"), expectedRows)
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }

  test("reading DECIMAL(38, 4)") {
    val tableName = s"reading_decimal_38_4_$randomSuffix"
    val decimals = Seq(
      "922337203685477.5808",
      "9999999999999999999999999999999999.0000",
      "-9999999999999999999999999999999999.0000",
      "0",
      "1234567.8910",
      null
    )
    val expectedRows = decimals.map(d => Row(if (d == null) null else Conversions.parseDecimal(d)))
    try {
      conn.createStatement().executeUpdate(s"CREATE TABLE $tableName (x DECIMAL(38, 4))")
      for (x <- decimals) {
        conn.createStatement().executeUpdate(s"INSERT INTO $tableName VALUES ($x)")
      }
      conn.commit()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = sqlContext.read
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("tempdir", tempDir)
        .option("aws_access_key_id", AWS_ACCESS_KEY_ID)
        .option("aws_secret_access_key", AWS_SECRET_ACCESS_KEY)
        .load()
      checkAnswer(loadedDf, expectedRows)
      checkAnswer(loadedDf.selectExpr("x + 0"), expectedRows)
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }
}
