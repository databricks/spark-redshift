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

  private def testReadingDecimals(precision: Int, scale: Int, decimalStrings: Seq[String]): Unit = {
    test(s"reading DECIMAL($precision, $scale)") {
      val tableName = s"reading_decimal_${precision}_${scale}_$randomSuffix"
      val expectedRows = decimalStrings.map { d =>
        if (d == null) {
          Row(null)
        } else {
          Row(Conversions.createRedshiftDecimalFormat().parse(d).asInstanceOf[java.math.BigDecimal])
        }
      }
      try {
        conn.createStatement().executeUpdate(
          s"CREATE TABLE $tableName (x DECIMAL($precision, $scale))")
        for (x <- decimalStrings) {
          conn.createStatement().executeUpdate(s"INSERT INTO $tableName VALUES ($x)")
        }
        conn.commit()
        assert(DefaultJDBCWrapper.tableExists(conn, tableName))
        val loadedDf = sqlContext.read
          .format("com.databricks.spark.redshift")
          .option("url", jdbcUrl)
          .option("dbtable", tableName)
          .option("tempdir", tempDir)
          .load()
        checkAnswer(loadedDf, expectedRows)
        checkAnswer(loadedDf.selectExpr("x + 0"), expectedRows)
      } finally {
        conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
        conn.commit()
      }
    }
  }

  testReadingDecimals(19, 0, Seq(
    // Max and min values of DECIMAL(19, 0) column according to Redshift docs:
    "9223372036854775807", // 2^63 - 1
    "-9223372036854775807",
    "0",
    "12345678910",
    null
  ))

  testReadingDecimals(19, 4, Seq(
    "922337203685477.5807",
    "-922337203685477.5807",
    "0",
    "1234567.8910",
    null
  ))

  testReadingDecimals(38, 4, Seq(
    "922337203685477.5808",
    "9999999999999999999999999999999999.0000",
    "-9999999999999999999999999999999999.0000",
    "0",
    "1234567.8910",
    null
  ))

  test("Decimal precision is preserved when reading from query (regression test for issue #203)") {
    val tableName = s"issue203_$randomSuffix"
    val sqlc = sqlContext
    import sqlc.implicits._
    try {
      sc.parallelize(Seq(Tuple1(91593373L)))
        .toDF("foo")
        .write
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("tempdir", tempDir)
        .save()
      val df = sqlContext.read
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("tempdir", tempDir)
        .option("query", s"select foo / 1000000.0 from $tableName limit 1")
        .load()
      val res: math.BigDecimal = df.collect().toSeq.head.getDecimal(0).stripTrailingZeros()
      assert(res === new java.math.BigDecimal(91593373L).divide(new java.math.BigDecimal(1000000L)))
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }
}
