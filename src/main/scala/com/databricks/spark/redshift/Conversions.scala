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
import java.text.{DecimalFormat, DateFormat, FieldPosition, ParsePosition, SimpleDateFormat}
import java.util.Date

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

/**
 * Data type conversions for Redshift unloaded data
 */
private[redshift] object Conversions {

  /**
   * Parse a boolean using Redshift's UNLOAD bool syntax
   */
  private def parseBoolean(s: String): Boolean = {
    if (s == "t") true
    else if (s == "f") false
    else throw new IllegalArgumentException(s"Expected 't' or 'f' but got '$s'")
  }

  /**
   * Formatter for writing decimals unloaded from Redshift.
   *
   * Note that Java Formatters are NOT thread-safe, so you should not re-use instances of this
   * DecimalFormat across threads.
   */
  def createRedshiftDecimalFormat(): DecimalFormat = {
    val format = new DecimalFormat()
    format.setParseBigDecimal(true)
    format
  }

  /**
   * Formatter for parsing strings exported from Redshift DATE columns.
   * This formatter should not be used when saving dates back to Redshift; instead, use
   * [[RedshiftTimestampFormat]].
   *
   * Note that Java Formatters are NOT thread-safe, so you should not re-use instances of this
   * SimpleDateFormat across threads.
   */
  def createRedshiftDateFormat(): SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  /**
   * Return a function that will convert arrays of strings conforming to the given schema to Rows.
   *
   * Note that instances of this function are NOT thread-safe.
   */
  def createRowConverter(schema: StructType): (Array[String]) => Row = {
    val timestampFormat = new RedshiftTimestampFormat
    val dateFormat = createRedshiftDateFormat()
    val decimalFormat = createRedshiftDecimalFormat()
    val conversionFunctions: Array[String => Any] = schema.fields.map { field =>
      field.dataType match {
        case ByteType => (data: String) => data.toByte
        case BooleanType => (data: String) => parseBoolean(data)
        case DateType => (data: String) => new java.sql.Date(dateFormat.parse(data).getTime)
        case DoubleType => (data: String) => data.toDouble
        case FloatType => (data: String) => data.toFloat
        case dt: DecimalType =>
          (data: String) => decimalFormat.parse(data).asInstanceOf[java.math.BigDecimal]
        case IntegerType => (data: String) => data.toInt
        case LongType => (data: String) => data.toLong
        case ShortType => (data: String) => data.toShort
        case StringType => (data: String) => data
        case TimestampType => (data: String) => new Timestamp(timestampFormat.parse(data).getTime)
        case _ => (data: String) => data
      }
    }
    // As a performance optimization, re-use a single row that is backed by a mutable Seq:
    val converted: Array[Any] = Array.fill(schema.length)(null)
    val row = new GenericMutableRow(converted)
    (fields: Array[String]) => {
      var i = 0
      while (i < schema.length) {
        val data = fields(i)
        converted(i) = if (data == null || data.isEmpty) null else conversionFunctions(i)(data)
        i += 1
      }
      row
    }
  }
}

/**
 * Formatter for parsing strings exported from Redshift TIMESTAMP columns and for formatting
 * timestamps as strings when writing data back to Redshift via Avro.
 *
 * Redshift may or may not include the fraction component in the UNLOAD data, and there are
 * apparently not clues about this in the table schema. This format delegates to one of two
 * formats based on string length.
 *
 * Instances of this class are NOT thread-safe (because they rely on Java's DateFormat classes,
 * which are also not thread-safe).
 */
private[redshift] class RedshiftTimestampFormat extends DateFormat {

  // Imports and exports with Redshift require that timestamps are represented
  // as strings, using the following formats
  private val PATTERN_WITH_MILLIS = "yyyy-MM-dd HH:mm:ss.SSS"
  private val PATTERN_WITHOUT_MILLIS = "yyyy-MM-dd HH:mm:ss"

  private val redshiftTimestampFormatWithMillis = new SimpleDateFormat(PATTERN_WITH_MILLIS)
  private val redshiftTimestampFormatWithoutMillis = new SimpleDateFormat(PATTERN_WITHOUT_MILLIS)

  override def format(
      date: Date,
      toAppendTo: StringBuffer,
      fieldPosition: FieldPosition): StringBuffer = {
    // Always export with milliseconds, as they can just be zero if not specified
    redshiftTimestampFormatWithMillis.format(date, toAppendTo, fieldPosition)
  }

  override def parse(source: String, pos: ParsePosition): Date = {
    if (source.length < PATTERN_WITH_MILLIS.length) {
      redshiftTimestampFormatWithoutMillis.parse(source, pos)
    } else {
      redshiftTimestampFormatWithMillis.parse(source, pos)
    }
  }
}
