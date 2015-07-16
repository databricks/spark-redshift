package com.databricks.spark.redshift

import java.sql.Timestamp
import java.util.Calendar

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}

/**
 * Unit test for data type conversions
 */
class ConversionsSuite extends FunSuite {

  val testSchema =
    StructType(
      Seq(
        StructField("testByte", ByteType, nullable = true),
        StructField("testBool", BooleanType, nullable = true),
        StructField("testDate", DateType, nullable = true),
        StructField("testDouble", DoubleType, nullable = true),
        StructField("testFloat", FloatType, nullable = true),
        StructField("testInt", IntegerType, nullable = true),
        StructField("testLong", LongType, nullable = true),
        StructField("testShort", ShortType, nullable = true),
        StructField("testString", StringType, nullable = true),
        StructField("testTimestamp", TimestampType, nullable = true)))

  val convertRow = Conversions.rowConverter(testSchema)

  test("Data should be correctly converted") {
    val doubleMin = Double.MinValue.toString
    val longMax = Long.MaxValue.toString
    val unicodeString = "Unicode是樂趣"

    val timestampWithMillis = "2014-03-01 00:00:01.123"

    val expectedDateMillis = {
      val calendar = Calendar.getInstance()
      calendar.set(2015, 6, 1, 0, 0, 0)
      calendar.set(Calendar.MILLISECOND, 0)
      calendar.getTime.getTime
    }

    val expectedTimestampMillis = {
      val calendar = Calendar.getInstance()
      calendar.set(2014, 2, 1, 0, 0, 1)
      calendar.set(Calendar.MILLISECOND, 123)
      calendar.getTime.getTime
    }

    val convertedRow = convertRow(
      Array("1", "t", "2015-07-01", doubleMin, "1.0", "42",
        longMax, "23", unicodeString, timestampWithMillis))

    val expectedRow = Row(1.asInstanceOf[Byte], true, new Timestamp(expectedDateMillis),
      Double.MinValue, 1.0f, 42, Long.MaxValue, 23.toShort, unicodeString,
      new Timestamp(expectedTimestampMillis))

    assert(convertedRow == expectedRow)
  }

  test("Row conversion handles null values") {
    val emptyRow = List.fill(testSchema.length)(null).toArray[String]
    assert(convertRow(emptyRow) == Row(emptyRow: _*))
  }
}
