package com.databricks.spark.redshift

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.Row
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

  /**
   * Parse a string exported from a Redshift TIMESTAMP column
   */
  def parseTimestamp(s: String): Timestamp = {
    val redshiftTimestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    new Timestamp(redshiftTimestampFormat.parse(s).getTime)
  }

  /**
   * Parse a string exported from a Redshift DATE column
   */
  def parseDate(s: String): Timestamp = {
    val redshiftDateFormat = new SimpleDateFormat("yyyy-MM-dd")
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