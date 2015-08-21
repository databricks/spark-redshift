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

import java.sql.{Date, Timestamp}
import java.util.Calendar

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Helpers for Redshift tests that require common mocking
 */
object TestUtils {

  /**
   * Simple schema that includes all data types we support
   */
  val testSchema: StructType = {
    def makeField(name: String, typ: DataType) = {
      val md = (new MetadataBuilder).putString("name", name).build()
      StructField(name, typ, nullable = true, metadata = md)
    }
    // These column names need to be lowercase; see #51
    StructType(Seq(
      makeField("testbyte", ByteType),
      makeField("testbool", BooleanType),
      makeField("testdate", DateType),
      makeField("testdouble", DoubleType),
      makeField("testfloat", FloatType),
      makeField("testint", IntegerType),
      makeField("testlong", LongType),
      makeField("testshort", ShortType),
      makeField("teststring", StringType),
      makeField("testtimestamp", TimestampType)))
  }

  // scalastyle:off
  /**
   * Expected parsed output corresponding to the output of testData.
   */
  val expectedData: Seq[Row] = Seq(
    Row(1.toByte, true, TestUtils.toDate(2015, 6, 1), 1234152.12312498,
      1.0f, 42, 1239012341823719L, 23, "Unicode's樂趣",
      TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1)),
    Row(1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
      1239012341823719L, -13, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
    Row(0.toByte, null, TestUtils.toDate(2015, 6, 3), 0.0, -1.0f, 4141214,
      1239012341823719L, null, "f", TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0)),
    Row(0.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L, 24,
      "___|_123", null),
    Row(List.fill(10)(null): _*))
  // scalastyle:on

  /**
   * The same as `expectedData`, but with dates and timestamps converted into string format.
   * See #39 for context.
   */
  val expectedDataWithConvertedTimesAndDates: Seq[Row] = expectedData.map { row =>
    Row.fromSeq(row.toSeq.map {
      case t: Timestamp => Conversions.formatTimestamp(t)
      case d: Date => Conversions.formatDate(d)
      case other => other
    })
  }

  /**
   * Convert date components to a millisecond timestamp
   */
  def toMillis(
      year: Int,
      zeroBasedMonth: Int,
      date: Int,
      hour: Int,
      minutes: Int,
      seconds: Int,
      millis: Int = 0): Long = {
    val calendar = Calendar.getInstance()
    calendar.set(year, zeroBasedMonth, date, hour, minutes, seconds)
    calendar.set(Calendar.MILLISECOND, millis)
    calendar.getTime.getTime
  }

  /**
   * Convert date components to a SQL Timestamp
   */
  def toTimestamp(
      year: Int,
      zeroBasedMonth: Int,
      date: Int,
      hour: Int,
      minutes: Int,
      seconds: Int,
      millis: Int = 0): Timestamp = {
    new Timestamp(toMillis(year, zeroBasedMonth, date, hour, minutes, seconds, millis))
  }

  /**
   * Convert date components to a SQL [[Date]].
   */
  def toDate(year: Int, zeroBasedMonth: Int, date: Int): Date = {
    new Date(toTimestamp(year, zeroBasedMonth, date, 0, 0, 0).getTime)
  }
}
