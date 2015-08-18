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

import org.apache.spark.sql.types._

/**
 * Helpers for Redshift tests that require common mocking
 */
object TestUtils {

  /**
   * Makes a field for the test schema
   */
  def makeField(name: String, typ: DataType) = {
    val md = (new MetadataBuilder).putString("name", name).build()
    StructField(name, typ, nullable = true, metadata = md)
  }

  /**
   * Simple schema that includes all data types we support
   */
  lazy val testSchema =
    StructType(
      Seq(
        makeField("testByte", ByteType),
        makeField("testBool", BooleanType),
        makeField("testDate", DateType),
        makeField("testDouble", DoubleType),
        makeField("testFloat", FloatType),
        makeField("testInt", IntegerType),
        makeField("testLong", LongType),
        makeField("testShort", ShortType),
        makeField("testString", StringType),
        makeField("testTimestamp", TimestampType)))

  /**
   * Convert date components to a millisecond timestamp
   */
  def toMillis(year: Int, zeroBasedMonth: Int, date: Int, hour: Int, minutes: Int, seconds: Int, millis: Int = 0) = {
    val calendar = Calendar.getInstance()
    calendar.set(year, zeroBasedMonth, date, hour, minutes, seconds)
    calendar.set(Calendar.MILLISECOND, millis)
    calendar.getTime.getTime
  }

  /**
   * Convert date components to a SQL Timestamp
   */
  def toTimestamp(year: Int, zeroBasedMonth: Int, date: Int, hour: Int, minutes: Int, seconds: Int, millis: Int = 0) = {
    new Timestamp(toMillis(year, zeroBasedMonth, date, hour, minutes, seconds, millis))
  }


  /**
   * Convert date components to a SQL [[Date]].
   */
  def toDate(year: Int, zeroBasedMonth: Int, date: Int) = {
    new Date(toTimestamp(year, zeroBasedMonth, date, 0, 0, 0).getTime)
  }
}
