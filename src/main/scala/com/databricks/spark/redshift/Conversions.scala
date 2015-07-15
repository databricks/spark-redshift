package com.databricks.spark.redshift

import java.sql.Timestamp
import java.text.{FieldPosition, ParsePosition, DateFormat, SimpleDateFormat}
import java.util.Date

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

import scala.util.parsing.combinator.JavaTokenParsers

/**
 * Simple parser for Redshift's UNLOAD bool syntax
 */
private object RedshiftBooleanParser extends JavaTokenParsers {
  val TRUE: Parser[Boolean] = "t" ^^ Function.const(true)
  val FALSE: Parser[Boolean] = "f" ^^ Function.const(false)

  def parseRedshiftBoolean(s: String) = parse(TRUE | FALSE, s)
}

/**
 * Data type conversions for Redshift unloaded data
 */
object Conversions {

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
    override def format(date: Date, toAppendTo: StringBuffer, fieldPosition: FieldPosition): StringBuffer = {
      // Always export with milliseconds, as they can just be zero if not specified
      redshiftTimestampFormatWithMillis.format(date, toAppendTo, fieldPosition)
    }

    override def parse(source: String, pos: ParsePosition): Date = {
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
  def parseTimestamp(s: String): Timestamp = {
    new Timestamp(redshiftTimestampFormat.parse(s).getTime)
  }

  /**
   * Parse a string exported from a Redshift DATE column
   */
  def parseDate(s: String): Timestamp = {
    new Timestamp(redshiftDateFormat.parse(s).getTime)
  }

  /**
   * Construct a Row from the given array of strings, retrieved from Reshift UNLOAD.
   * The schema will be used for type mappings.
   */
  def convertRow(schema: StructType, fields: Array[String]): Row = {
    val converted = fields zip schema map {
      case (data, field) =>
        if (data.isEmpty) null
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
  def rowConverter(schema: StructType) = convertRow(schema, _: Array[String])

}