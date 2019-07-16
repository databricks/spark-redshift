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

package io.github.spark_redshift_community.spark.redshift

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DecimalType

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
        assert(DefaultJDBCWrapper.tableExists(conn, tableName))
        val loadedDf = read.option("dbtable", tableName).load()
        checkAnswer(loadedDf, expectedRows)
        checkAnswer(loadedDf.selectExpr("x + 0"), expectedRows)
      } finally {
        conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
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
    withTempRedshiftTable("issue203") { tableName =>
      conn.createStatement().executeUpdate(s"CREATE TABLE $tableName (foo BIGINT)")
      conn.createStatement().executeUpdate(s"INSERT INTO $tableName VALUES (91593373)")
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val df = read
        .option("query", s"select foo / 1000000.0 from $tableName limit 1")
        .load()
      val res: Double = df.collect().toSeq.head.getDecimal(0).doubleValue()
      assert(res === (91593373L / 1000000.0) +- 0.01)
      assert(df.schema.fields.head.dataType === DecimalType(28, 8))
    }
  }
}
