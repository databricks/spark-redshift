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

import java.io.File
import java.sql.{Connection, PreparedStatement, SQLException}

import com.google.common.io.Files

import scala.util.matching.Regex

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputFormat
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, FunSuite, Matchers}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.jdbc.JDBCWrapper
import org.apache.spark.sql.sources._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

private class TestContext extends SparkContext("local", "RedshiftSourceSuite") {

  /**
   * A text file containing fake unloaded Redshift data of all supported types
   */
  val testData = new File("src/test/resources/redshift_unload_data.txt").toURI.toString

  override def newAPIHadoopFile[K, V, F <: InputFormat[K, V]](
      path: String,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V],
      conf: Configuration = hadoopConfiguration): RDD[(K, V)] = {
    super.newAPIHadoopFile[K, V, F](testData, fClass, kClass, vClass, conf)
  }
}

/**
 * Tests main DataFrame loading and writing functionality
 */
class RedshiftSourceSuite
  extends FunSuite
  with Matchers
  with MockFactory
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  // scalastyle:off
  /**
   * Expected parsed output corresponding to the output of testData.
   */
  private val expectedData: Seq[Row] = Seq(
    Row(1.toByte, true, TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0), 1234152.123124981,
      1.0f, 42, 1239012341823719L, 23, "Unicode是樂趣",
      TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1)),
    Row(1.toByte, false, TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0), 0.0, 0.0f, 42,
      1239012341823719L, -13, "asdf", TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
    Row(0.toByte, null, TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0), 0.0, -1.0f, 4141214,
      1239012341823719L, null, "f", TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0)),
    Row(0.toByte, false, null, -1234152.123124981, 100000.0f, null, 1239012341823719L, 24,
      "___|_123", null),
    Row(List.fill(10)(null): _*))
  // scalastyle:on

  /**
   * Spark Context with Hadoop file overridden to point at our local test data file for this suite,
   * no matter what temp directory was generated and requested.
   */
  private var sc: SparkContext = _

  private var testSqlContext: SQLContext = _

  /** Temporary folder for unloading data; reset after each test. */
  private var tempDir: File = _

  // Parameters common to most tests. Some parameters are overridden in specific tests.
  private def defaultParams: Map[String, String] = Map(
    "url" -> "jdbc:postgresql://foo/bar",
    "tempdir" -> tempDir.toURI.toString,
    "dbtable" -> "test_table",
    "aws_access_key_id" -> "test1",
    "aws_secret_access_key" -> "test2")

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new TestContext
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    testSqlContext = new SQLContext(sc)
    tempDir = Files.createTempDir()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    testSqlContext = null
    Option(tempDir.listFiles()).getOrElse(Array.empty).foreach(_.delete())
    tempDir.delete()
  }

  override def afterAll(): Unit = {
    sc.stop()
    super.afterAll()
  }

  /**
   * Set up a mocked JDBCWrapper instance that expects a sequence of queries matching the given
   * regular expressions will be executed, and that the connection returned will be closed.
   */
  private def mockJdbcWrapper(expectedUrl: String, expectedQueries: Seq[Regex]): JDBCWrapper = {
    val jdbcWrapper = mock[JDBCWrapper]
    val mockedConnection = mock[Connection]

    (jdbcWrapper.getConnector _).expects(*, expectedUrl, *).returning(() => mockedConnection)

    inSequence {
      expectedQueries foreach { r =>
        val mockedStatement = mock[PreparedStatement]
        (mockedConnection.prepareStatement(_: String))
          .expects(where {(sql: String) => r.findFirstMatchIn(sql).nonEmpty})
          .returning(mockedStatement)
        (mockedStatement.execute _).expects().returning(true)
      }

      (mockedConnection.close _).expects()
    }

    jdbcWrapper
  }

  /**
   * Prepare the JDBC wrapper for an UNLOAD test.
   */
  private def prepareUnloadTest(params: Map[String, String]): JDBCWrapper = {
    val jdbcUrl = params("url")
    val jdbcWrapper = mockJdbcWrapper(jdbcUrl, Seq("UNLOAD.*".r))

    // We expect some extra calls to the JDBC wrapper,
    // to register the driver and retrieve the schema.
    (jdbcWrapper.registerDriver _)
      .expects(*)
      .anyNumberOfTimes()
    (jdbcWrapper.resolveTable _)
      .expects(jdbcUrl, "test_table", *)
      .returning(TestUtils.testSchema)
      .anyNumberOfTimes()

    jdbcWrapper
  }

  test("DefaultSource can load Redshift UNLOAD output to a DataFrame") {
    val jdbcWrapper = prepareUnloadTest(defaultParams)

    // Assert that we've loaded and converted all data in the test file
    val source = new DefaultSource(jdbcWrapper)
    val relation = source.createRelation(testSqlContext, defaultParams)
    val df = testSqlContext.baseRelationToDataFrame(relation)
    QueryTest.checkAnswer(df, expectedData)
  }

  test("DefaultSource supports simple column filtering") {
    val jdbcWrapper = prepareUnloadTest(defaultParams)

    // Construct the source with a custom schema
    val source = new DefaultSource(jdbcWrapper)
    val relation = source.createRelation(testSqlContext, defaultParams, TestUtils.testSchema)

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testByte", "testBool"), Array.empty[Filter])
    val prunedExpectedValues = Array(
      Row(1.toByte, true),
      Row(1.toByte, false),
      Row(0.toByte, null),
      Row(0.toByte, false),
      Row(null, null))
    assert(rdd.collect() === prunedExpectedValues)
  }

  test("DefaultSource supports user schema, pruned and filtered scans") {
    val jdbcWrapper = prepareUnloadTest(defaultParams)

    // Construct the source with a custom schema
    val source = new DefaultSource(jdbcWrapper)
    val relation = source.createRelation(testSqlContext, defaultParams, TestUtils.testSchema)

    // Define a simple filter to only include a subset of rows
    val filters: Array[Filter] = Array(
      EqualTo("testBool", true),
      // scalastyle:off
      EqualTo("testString", "Unicode是樂趣"),
      // scalastyle:on
      GreaterThan("testDouble", 1000.0),
      LessThan("testDouble", Double.MaxValue),
      GreaterThanOrEqual("testFloat", 1.0f),
      LessThanOrEqual("testInt", 43))
    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testByte", "testBool"), filters)

    assert(rdd.collect() === Array(Row(1, true)))
  }

  test("DefaultSource serializes data as Avro, then sends Redshift COPY command") {
    val params = defaultParams ++ Map(
      "postactions" -> "GRANT SELECT ON %s TO jeremy",
      "diststyle" -> "KEY",
      "distkey" -> "testInt")

    val rdd = sc.parallelize(expectedData.toSeq)
    val df = testSqlContext.createDataFrame(rdd, TestUtils.testSchema)

    val expectedCommands = Seq(
      "DROP TABLE IF EXISTS test_table_staging_.*".r,
      "CREATE TABLE IF NOT EXISTS test_table_staging.* DISTSTYLE KEY DISTKEY \\(testInt\\).*".r,
      "COPY test_table_staging_.*".r,
      "GRANT SELECT ON test_table_staging.+ TO jeremy".r,
      "ALTER TABLE test_table RENAME TO test_table_backup_.*".r,
      "ALTER TABLE test_table_staging_.* RENAME TO test_table".r,
      "DROP TABLE test_table_backup.*".r)

    val jdbcWrapper = mockJdbcWrapper(params("url"), expectedCommands)

    (jdbcWrapper.tableExists _)
      .expects(*, "test_table")
      .returning(true)
      .anyNumberOfTimes()

    (jdbcWrapper.schemaString _)
      .expects(*, params("url"))
      .returning("schema")
      .anyNumberOfTimes()

    val relation =
      RedshiftRelation(jdbcWrapper, Parameters.mergeParameters(params), None)(testSqlContext)
    relation.asInstanceOf[InsertableRelation].insert(df, true)

    // Make sure we wrote the data out ready for Redshift load, in the expected formats
    val written = testSqlContext.read.format("com.databricks.spark.avro").load(params("tempdir"))
    QueryTest.checkAnswer(written, expectedData)
  }

  test("Failed copies are handled gracefully when using a staging table") {
    val params = defaultParams ++ Map("usestagingtable" -> "true")

    val rdd = sc.parallelize(expectedData.toSeq)
    val df = testSqlContext.createDataFrame(rdd, TestUtils.testSchema)

    val jdbcWrapper = mock[JDBCWrapper]
    val mockedConnection = mock[Connection]

    (jdbcWrapper.getConnector _)
      .expects(*, params("url"), *)
      .returning(() => mockedConnection)

    def successfulStatement(pattern: Regex): PreparedStatement = {
      val mockedStatement = mock[PreparedStatement]
      (mockedConnection.prepareStatement(_: String))
        .expects(where {(sql: String) => pattern.findFirstMatchIn(sql).nonEmpty})
        .returning(mockedStatement)
      (mockedStatement.execute _).expects().returning(true)

      mockedStatement
    }

    def failedStatement(pattern: Regex) : PreparedStatement = {
      val mockedStatement = mock[PreparedStatement]
      (mockedConnection.prepareStatement(_: String))
        .expects(where {(sql: String) => pattern.findFirstMatchIn(sql).nonEmpty})
        .returning(mockedStatement)

      (mockedStatement.execute _)
        .expects()
        .throwing(new SQLException("Mocked Error"))

      mockedStatement
    }

    (jdbcWrapper.tableExists _)
      .expects(*, "test_table")
      .returning(true)
      .anyNumberOfTimes()

    (jdbcWrapper.schemaString _)
      .expects(*, params("url"))
      .anyNumberOfTimes()

    inSequence {
      // Initial staging table setup succeeds
      successfulStatement("DROP TABLE IF EXISTS test_table_staging_.*".r)
      successfulStatement("CREATE TABLE IF NOT EXISTS test_table_staging.*".r)

      // Simulate COPY failure
      failedStatement("COPY test_table_staging_.*".r)

      // Expect recovery operations
      (jdbcWrapper.tableExists _)
        .expects(where {(conn: Connection, sql: String) =>
          "test_table_staging.*".r.findFirstIn(sql).nonEmpty})
        .returning(true)
      successfulStatement("DROP TABLE test_table_staging.*".r)

      (jdbcWrapper.tableExists _)
        .expects(where {(conn: Connection, sql: String) =>
          "test_table_backup.*".r.findFirstIn(sql).nonEmpty})
        .returning(true)
      successfulStatement("ALTER TABLE test_table_backup.+ RENAME TO test_table".r)

      (mockedConnection.close _).expects()
    }

    val source = new DefaultSource(jdbcWrapper)
    intercept[Exception] {
      source.createRelation(testSqlContext, SaveMode.Overwrite, params, df)
    }
  }

  test("Append SaveMode doesn't destroy existing data") {
    val rdd = sc.parallelize(expectedData.toSeq)
    val df = testSqlContext.createDataFrame(rdd, TestUtils.testSchema)

    val expectedCommands =
      Seq("CREATE TABLE IF NOT EXISTS test_table .*".r,
          "COPY test_table .*".r)

    val jdbcWrapper = mockJdbcWrapper(defaultParams("url"), expectedCommands)

    (jdbcWrapper.tableExists _)
      .expects(*, "test_table")
      .returning(true)
      .anyNumberOfTimes()

    (jdbcWrapper.schemaString _)
      .expects(*, defaultParams("url"))
      .returning("schema")
      .anyNumberOfTimes()

    val source = new DefaultSource(jdbcWrapper)
    val savedDf = source.createRelation(testSqlContext, SaveMode.Append, defaultParams, df)

    // This test is "appending" to an empty table, so we expect all our test data to be
    // the only content in the returned data frame
    val written =
      testSqlContext.read.format("com.databricks.spark.avro").load(defaultParams("tempdir"))
    QueryTest.checkAnswer(written, expectedData)
  }

  test("Respect SaveMode.ErrorIfExists when table exists") {
    val rdd = sc.parallelize(expectedData.toSeq)
    val df = testSqlContext.createDataFrame(rdd, TestUtils.testSchema)

    // Check that SaveMode.ErrorIfExists throws an exception

    val errIfExistsWrapper = mockJdbcWrapper(defaultParams("url"), Seq.empty[Regex])

    (errIfExistsWrapper.tableExists _)
      .expects(*, "test_table")
      .returning(true)

    val errIfExistsSource = new DefaultSource(errIfExistsWrapper)
    intercept[Exception] {
      errIfExistsSource.createRelation(testSqlContext, SaveMode.ErrorIfExists, defaultParams, df)
    }
  }

  test("Do nothing when table exists if SaveMode = Ignore") {
    val rdd = sc.parallelize(expectedData.toSeq)
    val df = testSqlContext.createDataFrame(rdd, TestUtils.testSchema)

    // Check that SaveMode.Ignore does nothing

    val ignoreWrapper = mockJdbcWrapper(defaultParams("url"), Seq.empty[Regex])

    (ignoreWrapper.tableExists _)
      .expects(*, "test_table")
      .returning(true)

    // Note: Assertions covered by mocks
    val ignoreSource = new DefaultSource(ignoreWrapper)
    ignoreSource.createRelation(testSqlContext, SaveMode.Ignore, defaultParams, df)
  }

  test("Public Scala API rejects invalid parameter maps") {
    val invalidParams = Map("dbtable" -> "foo") // missing tempdir and url

    val rdd = sc.parallelize(expectedData)
    val df = testSqlContext.createDataFrame(rdd, TestUtils.testSchema)

    intercept[Exception] {
      df.saveAsRedshiftTable(invalidParams)
    }

    intercept[Exception] {
      testSqlContext.redshiftTable(invalidParams)
    }
  }

  test("DefaultSource has default constructor, required by Data Source API") {
    new DefaultSource()
  }
}
