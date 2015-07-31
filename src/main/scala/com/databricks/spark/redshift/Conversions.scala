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
import java.text.{DateFormat, FieldPosition, ParsePosition, SimpleDateFormat}
import java.util.{Date => utilDate}

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.util.parsing.combinator.JavaTokenParsers

/**
 * Simple parser for Redshift's UNLOAD bool syntax
 */
private object RedshiftBooleanParser extends JavaTokenParsers {
  val TRUE: Parser[Boolean] = "t" ^^ Function.const(true)
  val FALSE: Parser[Boolean] = "f" ^^ Function.const(false)

  def parseRedshiftBoolean(s: String): Boolean = parse(TRUE | FALSE, s).get
}

/**
 * Data type conversions for Redshift unloaded data
 */
private[redshift] object Conversions {
  // Imports and exports with Redshift require that timestamps are represented
  // as strings, using the following formats
  val PATTERN_WITH_MILLIS = "yyyy-MM-dd HH:mm:ss.S"
  val PATTERN_WITHOUT_MILLIS = "yyyy-MM-dd HH:mm:ss"

  val redshiftTimestampFormatWithMillis = new SimpleDateFormat(PATTERN_WITH_MILLIS)
  val redshiftTimestampFormatWithoutMillis = new SimpleDateFormat(PATTERN_WITHOUT_MILLIS)

  // Redshift may or may not include the fraction component in the UNLOAD data, and there are
  // apparently not clues about this in the table schema. This format delegates to one of the above
  // formats based on string length.
  val redshiftTimestampFormat = new DateFormat() {
    override def format(date: utilDate, toAppendTo: StringBuffer, fieldPosition: FieldPosition): StringBuffer = {
      // Always export with milliseconds, as they can just be zero if not specified
      redshiftTimestampFormatWithMillis.format(date, toAppendTo, fieldPosition)
    }

    override def parse(source: String, pos: ParsePosition): utilDate = {
      if(source.length < PATTERN_WITH_MILLIS.length) {
        redshiftTimestampFormatWithoutMillis.parse(source, pos)
      } else {
        redshiftTimestampFormatWithMillis.parse(source, pos)
      }
    }
  }

  val redshiftDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  /**
   * Parse a string exported from a Redshift TIMESTAMP column
   */
  def parseTimestamp(s: String): Timestamp = new Timestamp(redshiftTimestampFormat.parse(s).getTime)

  /**
   * Parse a string exported from a Redshift DATE column
   */
  def parseDate(s: String): Date = new Date(redshiftDateFormat.parse(s).getTime)

  /**
   * Construct a Row from the given array of strings, retrieved from Redshift UNLOAD.
   * The schema will be used for type mappings.
   */
  def convertRow(schema: StructType, fields: Array[String]): Row = {
    val converted = fields zip schema map {
      case (data, field) =>
        if (data == null || data.isEmpty) null
        else field.dataType match {
          case ByteType => data.toByte
          case BooleanType => RedshiftBooleanParser.parseRedshiftBoolean(data)
          case DateType => parseDate(data)
          case DoubleType => data.toDouble
          case FloatType => data.toFloat
          case IntegerType => data.toInt
          case LongType => data.toLong
          case ShortType => data.toShort
          case StringType => data
          case TimestampType => parseTimestamp(data)
          case _ => data
        }
    }

    Row(converted: _*)
  }

  /**
   * Return a function that will convert arrays of strings conforming to
   * the given schema to Row instances
   */
  def rowConverter(schema: StructType): (Array[String]) => Row = convertRow(schema, _: Array[String])

  /**
   * Convert schema representation of Dates to Timestamps, as spark-avro only works with Timestamps
   */
  def datesToTimestamps(sqlContext: SQLContext, df: DataFrame): DataFrame = {
    val cols = df.schema.fields.map { field =>
      field.dataType match {
        case DateType => df.col(field.name).cast(TimestampType).as(field.name)
        case _ => df.col(field.name)
      }
    }
    df.select(cols: _*)
  }
}