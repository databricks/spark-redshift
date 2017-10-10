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
import java.util.{Calendar, Locale, TimeZone}

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
    // These column names need to be lowercase; see #51
    StructType(Seq(
      StructField("testbyte", ByteType),
      StructField("testbool", BooleanType),
      StructField("testdate", DateType),
      StructField("testdouble", DoubleType),
      StructField("testfloat", FloatType),
      StructField("testint", IntegerType),
      StructField("testlong", LongType),
      StructField("testshort", ShortType),
      StructField("teststring", StringType),
      StructField("testtimestamp", TimestampType),
      StructField("testtimestamptz", TimestampType)))
  }

  // scalastyle:off
  /**
   * Expected parsed output corresponding to the output of testData.
   */
  val expectedData: Seq[Row] = Seq(
    Row(1.toByte, true, TestUtils.toDate(2015, 6, 1), 1234152.12312498,
      1.0f, 42, 1239012341823719L, 23.toShort, "Unicode's樂趣",
      TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1), TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1,
        TimeZone.getTimeZone("UTC"))),
    Row(1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42,
      1239012341823719L, -13.toShort, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0),
      TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0, TimeZone.getTimeZone("UTC"))),
    Row(0.toByte, null, TestUtils.toDate(2015, 6, 3), 0.0, -1.0f, 4141214,
      1239012341823719L, null, "f", TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0),
      TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0, timezone = TimeZone.getTimeZone("UTC"))),
    Row(0.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L, 24.toShort,
      "___|_123", null, null),
    Row(List.fill(11)(null): _*))
  // scalastyle:on

  /**
   * The same as `expectedData`, but with dates and timestamps converted into string format.
   * See #39 for context.
   */
  val expectedDataWithConvertedTimesAndDates: Seq[Row] = expectedData.map { row =>
    Row.fromSeq(row.toSeq.map {
      case t: Timestamp => Conversions.createRedshiftTimestampFormat().format(t)
      case d: Date => Conversions.createRedshiftDateFormat().format(d)
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
      millis: Int = 0,
      timezone: TimeZone = TimeZone.getDefault): Long = {
    val calendar = Calendar.getInstance(timezone)
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
      millis: Int = 0,
      timezone: TimeZone = TimeZone.getDefault): Timestamp = {
    new Timestamp(toMillis(year, zeroBasedMonth, date, hour, minutes, seconds, millis, timezone))
  }

  /**
   * Convert date components to a SQL [[Date]].
   */
  def toDate(year: Int, zeroBasedMonth: Int, date: Int): Date = {
    new Date(toTimestamp(year, zeroBasedMonth, date, 0, 0, 0).getTime)
  }

  def withDefaultLocale[T](newDefaultLocale: Locale)(block: => T): T = {
    val originalDefaultLocale = Locale.getDefault
    try {
      Locale.setDefault(newDefaultLocale)
      block
    } finally {
      Locale.setDefault(originalDefaultLocale)
    }
  }
}
