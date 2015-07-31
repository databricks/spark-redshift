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

import org.apache.spark.sql.Row
import org.scalatest.FunSuite

/**
 * Unit test for data type conversions
 */
class ConversionsSuite extends FunSuite {

  val convertRow = Conversions.rowConverter(TestUtils.testSchema)

  test("Data should be correctly converted") {
    val doubleMin = Double.MinValue.toString
    val longMax = Long.MaxValue.toString
    val unicodeString = "Unicode是樂趣"

    val timestampWithMillis = "2014-03-01 00:00:01.123"

    val expectedDateMillis = TestUtils.toMillis(2015, 6, 1, 0, 0, 0)
    val expectedTimestampMillis = TestUtils.toMillis(2014, 2, 1, 0, 0, 1, 123)

    val convertedRow = convertRow(
      Array("1", "t", "2015-07-01", doubleMin, "1.0", "42", longMax, "23", unicodeString, timestampWithMillis))

    val expectedRow = Row(1.asInstanceOf[Byte], true, new Timestamp(expectedDateMillis),
      Double.MinValue, 1.0f, 42, Long.MaxValue, 23.toShort, unicodeString,
      new Timestamp(expectedTimestampMillis))

    assert(convertedRow == expectedRow)
  }

  test("Row conversion handles null values") {
    val emptyRow = List.fill(TestUtils.testSchema.length)(null).toArray[String]
    assert(convertRow(emptyRow) == Row(emptyRow: _*))
  }
}
