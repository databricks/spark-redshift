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

import java.sql.Timestamp
import java.util.Locale

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.scalatest.FunSuite

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Unit test for data type conversions
 */
class ConversionsSuite extends FunSuite {

  private def createRowConverter(schema: StructType) = {
    Conversions.createRowConverter(schema, Parameters.DEFAULT_PARAMETERS("csvnullstring"))
      .andThen(RowEncoder(schema).resolveAndBind().fromRow)
  }

  test("Data should be correctly converted") {
    val convertRow = createRowConverter(TestUtils.testSchema)
    val doubleMin = Double.MinValue.toString
    val longMax = Long.MaxValue.toString
    // scalastyle:off
    val unicodeString = "Unicode是樂趣"
    // scalastyle:on

    val timestampWithMillis = "2014-03-01 00:00:01.123"

    val expectedDateMillis = TestUtils.toMillis(2015, 6, 1, 0, 0, 0)
    val expectedTimestampMillis = TestUtils.toMillis(2014, 2, 1, 0, 0, 1, 123)

    val convertedRow = convertRow(
      Array("1", "t", "2015-07-01", doubleMin, "1.0", "42",
        longMax, "23", unicodeString, timestampWithMillis))

    val expectedRow = Row(1.asInstanceOf[Byte], true, new Timestamp(expectedDateMillis),
      Double.MinValue, 1.0f, 42, Long.MaxValue, 23.toShort, unicodeString,
      new Timestamp(expectedTimestampMillis))

    assert(convertedRow == expectedRow)
  }

  test("Row conversion handles null values") {
    val convertRow = createRowConverter(TestUtils.testSchema)
    val emptyRow = List.fill(TestUtils.testSchema.length)(null).toArray[String]
    assert(convertRow(emptyRow) === Row(emptyRow: _*))
  }

  test("Booleans are correctly converted") {
    val convertRow = createRowConverter(StructType(Seq(StructField("a", BooleanType))))
    assert(convertRow(Array("t")) === Row(true))
    assert(convertRow(Array("f")) === Row(false))
    assert(convertRow(Array(null)) === Row(null))
    intercept[IllegalArgumentException] {
      convertRow(Array("not-a-boolean"))
    }
  }

  test("timestamp conversion handles millisecond-level precision (regression test for #214)") {
    val schema = StructType(Seq(StructField("a", TimestampType)))
    val convertRow = createRowConverter(schema)
    Seq(
      "2014-03-01 00:00:01" -> TestUtils.toMillis(2014, 2, 1, 0, 0, 0, millis = 1000),
      "2014-03-01 00:00:01.000" -> TestUtils.toMillis(2014, 2, 1, 0, 0, 0, millis = 1000),
      "2014-03-01 00:00:00.1" -> TestUtils.toMillis(2014, 2, 1, 0, 0, 0, millis = 100),
      "2014-03-01 00:00:00.10" -> TestUtils.toMillis(2014, 2, 1, 0, 0, 0, millis = 100),
      "2014-03-01 00:00:00.100" -> TestUtils.toMillis(2014, 2, 1, 0, 0, 0, millis = 100),
      "2014-03-01 00:00:00.01" -> TestUtils.toMillis(2014, 2, 1, 0, 0, 0, millis = 10),
      "2014-03-01 00:00:00.010" -> TestUtils.toMillis(2014, 2, 1, 0, 0, 0, millis = 10),
      "2014-03-01 00:00:00.001" -> TestUtils.toMillis(2014, 2, 1, 0, 0, 0, millis = 1)
    ).foreach { case (timestampString, expectedTime) =>
      withClue(s"timestamp string is '$timestampString'") {
        val convertedRow = convertRow(Array(timestampString))
        val convertedTimestamp = convertedRow.get(0).asInstanceOf[Timestamp]
        assert(convertedTimestamp === new Timestamp(expectedTime))
      }
    }
  }

  test("RedshiftDecimalFormat is locale-insensitive (regression test for #243)") {
    for (locale <- Seq(Locale.US, Locale.GERMAN, Locale.UK)) {
      withClue(s"locale = $locale") {
        TestUtils.withDefaultLocale(locale) {
          val decimalFormat = Conversions.createRedshiftDecimalFormat()
          val parsed = decimalFormat.parse("151.20").asInstanceOf[java.math.BigDecimal]
          assert(parsed.doubleValue() === 151.20)
        }
      }
    }
  }

  test("Row conversion properly handles NaN and Inf float values (regression test for #261)") {
    val convertRow = createRowConverter(StructType(Seq(StructField("a", FloatType))))
    assert(java.lang.Float.isNaN(convertRow(Array("nan")).getFloat(0)))
    assert(convertRow(Array("inf")) === Row(Float.PositiveInfinity))
    assert(convertRow(Array("-inf")) === Row(Float.NegativeInfinity))
  }

  test("Row conversion properly handles NaN and Inf double values (regression test for #261)") {
    val convertRow = createRowConverter(StructType(Seq(StructField("a", DoubleType))))
    assert(java.lang.Double.isNaN(convertRow(Array("nan")).getDouble(0)))
    assert(convertRow(Array("inf")) === Row(Double.PositiveInfinity))
    assert(convertRow(Array("-inf")) === Row(Double.NegativeInfinity))
  }
}
