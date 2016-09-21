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
import java.text.{DecimalFormat, DecimalFormatSymbols, SimpleDateFormat}
import java.util.Locale

import scala.collection.mutable

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
    format.setDecimalFormatSymbols(new DecimalFormatSymbols(Locale.US))
    format
  }

  /**
   * Formatter for parsing strings exported from Redshift DATE columns.
   *
   * Note that Java Formatters are NOT thread-safe, so you should not re-use instances of this
   * SimpleDateFormat across threads.
   */
  def createRedshiftDateFormat(): SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  /**
   * Formatter for formatting timestamps for insertion into Redshift TIMESTAMP columns.
   *
   * This formatter should not be used to parse timestamps returned from Redshift UNLOAD commands;
   * instead, use [[Timestamp.valueOf()]].
   *
   * Note that Java Formatters are NOT thread-safe, so you should not re-use instances of this
   * SimpleDateFormat across threads.
   */
  def createRedshiftTimestampFormat(): SimpleDateFormat = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  }

  /**
   * Return a function that will convert arrays of strings conforming to the given schema to Rows.
   *
   * Note that instances of this function are NOT thread-safe.
   */
  def createRowConverter(schema: StructType): (Array[String]) => Row = {
    val dateFormat = createRedshiftDateFormat()
    val decimalFormat = createRedshiftDecimalFormat()
    val conversionFunctions: Array[String => Any] = schema.fields.map { field =>
      field.dataType match {
        case ByteType => (data: String) => java.lang.Byte.parseByte(data)
        case BooleanType => (data: String) => parseBoolean(data)
        case DateType => (data: String) => new java.sql.Date(dateFormat.parse(data).getTime)
        case DoubleType => (data: String) => data match {
          case "nan" => Double.NaN
          case "inf" => Double.PositiveInfinity
          case "-inf" => Double.NegativeInfinity
          case _ => java.lang.Double.parseDouble(data)
        }
        case FloatType => (data: String) => data match {
          case "nan" => Float.NaN
          case "inf" => Float.PositiveInfinity
          case "-inf" => Float.NegativeInfinity
          case _ => java.lang.Float.parseFloat(data)
        }
        case dt: DecimalType =>
          (data: String) => decimalFormat.parse(data).asInstanceOf[java.math.BigDecimal]
        case IntegerType => (data: String) => java.lang.Integer.parseInt(data)
        case LongType => (data: String) => java.lang.Long.parseLong(data)
        case ShortType => (data: String) => java.lang.Short.parseShort(data)
        case StringType => (data: String) => data
        case TimestampType => (data: String) => Timestamp.valueOf(data)
        case _ => (data: String) => data
      }
    }
    // As a performance optimization, re-use the same mutable Seq:
    val converted: mutable.IndexedSeq[Any] = mutable.IndexedSeq.fill(schema.length)(null)
    (fields: Array[String]) => {
      var i = 0
      while (i < schema.length) {
        val data = fields(i)
        converted(i) = if (data == null || data.isEmpty) null else conversionFunctions(i)(data)
        i += 1
      }
      Row.fromSeq(converted)
    }
  }
}
