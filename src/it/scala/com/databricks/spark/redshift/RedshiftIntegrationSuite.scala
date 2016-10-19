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

import java.sql.SQLException

import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
 * End-to-end tests which run against a real Redshift cluster.
 */
class RedshiftIntegrationSuite extends IntegrationSuiteBase {

  private val test_table: String = s"test_table_$randomSuffix"

  override def beforeAll(): Unit = {
    super.beforeAll()
    conn.prepareStatement(s"drop table if exists $test_table").executeUpdate()
    conn.commit()
    createTestDataInRedshift(test_table)
  }

  override def afterAll(): Unit = {
    try {
      conn.prepareStatement(s"drop table if exists $test_table").executeUpdate()
      conn.commit()
    } finally {
      super.afterAll()
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    read.option("dbtable", test_table).load().createOrReplaceTempView("test_table")
  }

  test("DefaultSource can load Redshift UNLOAD output to a DataFrame") {
    checkAnswer(
      sqlContext.sql("select * from test_table"),
      TestUtils.expectedData)
  }

  test("count() on DataFrame created from a Redshift table") {
    checkAnswer(
      sqlContext.sql("select count(*) from test_table"),
      Seq(Row(TestUtils.expectedData.length))
    )
  }

  test("count() on DataFrame created from a Redshift query") {
    val loadedDf =
      // scalastyle:off
      read.option("query", s"select * from $test_table where teststring = 'Unicode''s樂趣'").load()
      // scalastyle:on
    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(1))
    )
  }

  test("backslashes in queries/subqueries are escaped (regression test for #215)") {
    val loadedDf =
      read.option("query", s"select replace(teststring, '\\\\', '') as col from $test_table").load()
    checkAnswer(
      loadedDf.filter("col = 'asdf'"),
      Seq(Row("asdf"))
    )
  }

  test("Can load output when 'dbtable' is a subquery wrapped in parentheses") {
    // scalastyle:off
    val query =
      s"""
        |(select testbyte, testbool
        |from $test_table
        |where testbool = true
        | and teststring = 'Unicode''s樂趣'
        | and testdouble = 1234152.12312498
        | and testfloat = 1.0
        | and testint = 42)
      """.stripMargin
    // scalastyle:on
    checkAnswer(read.option("dbtable", query).load(), Seq(Row(1, true)))
  }

  test("Can load output when 'query' is specified instead of 'dbtable'") {
    // scalastyle:off
    val query =
      s"""
        |select testbyte, testbool
        |from $test_table
        |where testbool = true
        | and teststring = 'Unicode''s樂趣'
        | and testdouble = 1234152.12312498
        | and testfloat = 1.0
        | and testint = 42
      """.stripMargin
    // scalastyle:on
    checkAnswer(read.option("query", query).load(), Seq(Row(1, true)))
  }

  test("Can load output of Redshift aggregation queries") {
    checkAnswer(
      read.option("query", s"select testbool, count(*) from $test_table group by testbool").load(),
      Seq(Row(true, 1), Row(false, 2), Row(null, 2)))
  }

  test("DefaultSource supports simple column filtering") {
    checkAnswer(
      sqlContext.sql("select testbyte, testbool from test_table"),
      Seq(
        Row(null, null),
        Row(0.toByte, null),
        Row(0.toByte, false),
        Row(1.toByte, false),
        Row(1.toByte, true)))
  }

  test("query with pruned and filtered scans") {
    // scalastyle:off
    checkAnswer(
      sqlContext.sql(
        """
          |select testbyte, testbool
          |from test_table
          |where testbool = true
          | and teststring = "Unicode's樂趣"
          | and testdouble = 1234152.12312498
          | and testfloat = 1.0
          | and testint = 42
        """.stripMargin),
      Seq(Row(1, true)))
    // scalastyle:on
  }

  test("RedshiftRelation implements Spark 1.6+'s unhandledFilters API") {
    assume(org.apache.spark.SPARK_VERSION.take(3) >= "1.6")
    val df = sqlContext.sql("select testbool from test_table where testbool = true")
    val physicalPlan = df.queryExecution.sparkPlan
    physicalPlan.collectFirst { case f: execution.FilterExec => f }.foreach { filter =>
      fail(s"Filter should have been eliminated:\n${df.queryExecution}")
    }
  }

  test("roundtrip save and load") {
    // This test can be simplified once #98 is fixed.
    val tableName = s"roundtrip_save_and_load_$randomSuffix"
    try {
      write(
        sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema))
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), TestUtils.expectedData)
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }

  test("roundtrip save and load with uppercase column names") {
    testRoundtripSaveAndLoad(
      s"roundtrip_write_and_read_with_uppercase_column_names_$randomSuffix",
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("A", IntegerType) :: Nil)),
      expectedSchemaAfterLoad = Some(StructType(StructField("a", IntegerType) :: Nil)))
  }

  test("save with column names that are reserved words") {
    testRoundtripSaveAndLoad(
      s"save_with_column_names_that_are_reserved_words_$randomSuffix",
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("table", IntegerType) :: Nil)))
  }

  test("save with one empty partition (regression test for #96)") {
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1)), 2),
      StructType(StructField("foo", IntegerType) :: Nil))
    assert(df.rdd.glom.collect() === Array(Array.empty[Row], Array(Row(1))))
    testRoundtripSaveAndLoad(s"save_with_one_empty_partition_$randomSuffix", df)
  }

  test("save with all empty partitions (regression test for #96)") {
    val df = sqlContext.createDataFrame(sc.parallelize(Seq.empty[Row], 2),
      StructType(StructField("foo", IntegerType) :: Nil))
    assert(df.rdd.glom.collect() === Array(Array.empty[Row], Array.empty[Row]))
    testRoundtripSaveAndLoad(s"save_with_all_empty_partitions_$randomSuffix", df)
    // Now try overwriting that table. Although the new table is empty, it should still overwrite
    // the existing table.
    val df2 = df.withColumnRenamed("foo", "bar")
    testRoundtripSaveAndLoad(
      s"save_with_all_empty_partitions_$randomSuffix", df2, saveMode = SaveMode.Overwrite)
  }

  test("multiple scans on same table") {
    // .rdd() forces the first query to be unloaded from Redshift
    val rdd1 = sqlContext.sql("select testint from test_table").rdd
    // Similarly, this also forces an unload:
    sqlContext.sql("select testdouble from test_table").rdd
    // If the unloads were performed into the same directory then this call would fail: the
    // second unload from rdd2 would have overwritten the integers with doubles, so we'd get
    // a NumberFormatException.
    rdd1.count()
  }

  test("informative error message when saving a table with string that is longer than max length") {
    val tableName = s"error_message_when_string_too_long_$randomSuffix"
    try {
      val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row("a" * 512))),
        StructType(StructField("A", StringType) :: Nil))
      val e = intercept[SQLException] {
        write(df)
          .option("dbtable", tableName)
          .mode(SaveMode.ErrorIfExists)
          .save()
      }
      assert(e.getMessage.contains("while loading data into Redshift"))
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }

  test("filtering based on date constants (regression test for #152)") {
    val date = TestUtils.toDate(year = 2015, zeroBasedMonth = 6, date = 3)
    val df = sqlContext.sql("select testdate from test_table")

    checkAnswer(df.filter(df("testdate") === date), Seq(Row(date)))
    // This query failed in Spark 1.6.0 but not in earlier versions. It looks like 1.6.0 performs
    // constant-folding, whereas earlier Spark versions would preserve the cast which prevented
    // filter pushdown.
    checkAnswer(df.filter("testdate = to_date('2015-07-03')"), Seq(Row(date)))
  }

  test("filtering based on timestamp constants (regression test for #152)") {
    val timestamp = TestUtils.toTimestamp(2015, zeroBasedMonth = 6, 1, 0, 0, 0, 1)
    val df = sqlContext.sql("select testtimestamp from test_table")

    checkAnswer(df.filter(df("testtimestamp") === timestamp), Seq(Row(timestamp)))
    // This query failed in Spark 1.6.0 but not in earlier versions. It looks like 1.6.0 performs
    // constant-folding, whereas earlier Spark versions would preserve the cast which prevented
    // filter pushdown.
    checkAnswer(df.filter("testtimestamp = '2015-07-01 00:00:00.001'"), Seq(Row(timestamp)))
  }

  test("full timestamp precision is preserved in loads (regression test for #214)") {
    val timestamps = Seq(
      TestUtils.toTimestamp(1970, 0, 1, 0, 0, 0, millis = 1),
      TestUtils.toTimestamp(1970, 0, 1, 0, 0, 0, millis = 10),
      TestUtils.toTimestamp(1970, 0, 1, 0, 0, 0, millis = 100),
      TestUtils.toTimestamp(1970, 0, 1, 0, 0, 0, millis = 1000))
    testRoundtripSaveAndLoad(
      s"full_timestamp_precision_is_preserved$randomSuffix",
      sqlContext.createDataFrame(sc.parallelize(timestamps.map(Row(_))),
        StructType(StructField("ts", TimestampType) :: Nil))
    )
  }

  test("read special float values (regression test for #261)") {
    val tableName = s"roundtrip_special_float_values_$randomSuffix"
    try {
      conn.createStatement().executeUpdate(
        s"CREATE TABLE $tableName (x real)")
      conn.createStatement().executeUpdate(
        s"INSERT INTO $tableName VALUES ('NaN'), ('Infinity'), ('-Infinity')")
      conn.commit()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      // Due to #98, we use Double here instead of float:
      checkAnswer(
        read.option("dbtable", tableName).load(),
        Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).map(x => Row.apply(x)))
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }

  test("read special double values (regression test for #261)") {
    val tableName = s"roundtrip_special_double_values_$randomSuffix"
    try {
      conn.createStatement().executeUpdate(
        s"CREATE TABLE $tableName (x double precision)")
      conn.createStatement().executeUpdate(
        s"INSERT INTO $tableName VALUES ('NaN'), ('Infinity'), ('-Infinity')")
      conn.commit()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      checkAnswer(
        read.option("dbtable", tableName).load(),
        Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).map(x => Row.apply(x)))
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }
}
