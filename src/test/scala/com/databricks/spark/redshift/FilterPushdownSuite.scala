/*
 * Copyright 2015 Databricks
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

import org.scalatest.FunSuite

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import com.databricks.spark.redshift.FilterPushdown._


class FilterPushdownSuite extends FunSuite {
  test("buildWhereClause with empty list of filters") {
    assert(buildWhereClause(StructType(Nil), Seq.empty) === "")
  }

  test("buildWhereClause with no filters that can be pushed down") {
    assert(buildWhereClause(StructType(Nil), Seq(NewFilter, NewFilter)) === "")
  }

  test("buildWhereClause with with some filters that cannot be pushed down") {
    val whereClause = buildWhereClause(testSchema, Seq(EqualTo("test_int", 1), NewFilter))
    assert(whereClause === """WHERE "test_int" = 1""")
  }

  test("buildWhereClause with string literals that contain Unicode characters") {
    // scalastyle:off
    val whereClause = buildWhereClause(testSchema, Seq(EqualTo("test_string", "Unicode's樂趣")))
    // Here, the apostrophe in the string needs to be replaced with two single quotes, '', but we
    // also need to escape those quotes with backslashes because this WHERE clause is going to
    // eventually be embedded inside of a single-quoted string that's embedded inside of a larger
    // Redshift query.
    assert(whereClause === """WHERE "test_string" = \'Unicode\'\'s樂趣\'""")
    // scalastyle:on
  }

  test("buildWhereClause with multiple filters") {
    val filters = Seq(
      EqualTo("test_bool", true),
      // scalastyle:off
      EqualTo("test_string", "Unicode是樂趣"),
      // scalastyle:on
      GreaterThan("test_double", 1000.0),
      LessThan("test_double", Double.MaxValue),
      GreaterThanOrEqual("test_float", 1.0f),
      LessThanOrEqual("test_int", 43),
      IsNotNull("test_int"),
      IsNull("test_int"))
    val whereClause = buildWhereClause(testSchema, filters)
    // scalastyle:off
    val expectedWhereClause =
      """
        |WHERE "test_bool" = true
        |AND "test_string" = \'Unicode是樂趣\'
        |AND "test_double" > 1000.0
        |AND "test_double" < 1.7976931348623157E308
        |AND "test_float" >= 1.0
        |AND "test_int" <= 43
        |AND "test_int" IS NOT NULL
        |AND "test_int" IS NULL
      """.stripMargin.lines.mkString(" ").trim
    // scalastyle:on
    assert(whereClause === expectedWhereClause)
  }

  private val testSchema: StructType = StructType(Seq(
    StructField("test_byte", ByteType),
    StructField("test_bool", BooleanType),
    StructField("test_date", DateType),
    StructField("test_double", DoubleType),
    StructField("test_float", FloatType),
    StructField("test_int", IntegerType),
    StructField("test_long", LongType),
    StructField("test_short", ShortType),
    StructField("test_string", StringType),
    StructField("test_timestamp", TimestampType)))

  /** A new filter subclasss which our pushdown logic does not know how to handle */
  private case object NewFilter extends Filter {
    override def references: Array[String] = Array.empty
  }
}
