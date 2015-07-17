package com.databricks.spark.redshift

import java.sql.Timestamp
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
}
