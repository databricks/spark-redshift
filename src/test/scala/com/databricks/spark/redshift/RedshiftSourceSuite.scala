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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.jdbc.JDBCWrapper
import org.apache.spark.sql.sources._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.util.matching.Regex

class TestContext extends SparkContext("local", "RedshiftSourceSuite") {

  override val hadoopConfiguration: Configuration = {
    val conf = new Configuration(super.hadoopConfiguration)
    conf.set("fs.file.awsAccessKeyId", "awsAccessKeyId")
    conf.set("fs.file.awsSecretAccessKey", "awsSecretAccessKey")
    conf
  }

  /**
   * A text file containing fake unloaded Redshift data of all supported types
   */
  val testData = new File("src/test/resources/redshift_unload_data.txt").toURI.toString

  override def newAPIHadoopFile[K, V, F <: InputFormat[K, V]](
    path: String, fClass: Class[F], kClass: Class[K], vClass: Class[V],
    conf: Configuration = hadoopConfiguration): RDD[(K, V)] =
    super.newAPIHadoopFile[K, V, F](testData, fClass, kClass, vClass, conf)
}

/**
 * Tests main DataFrame loading and writing functionality
 */
class RedshiftSourceSuite
  extends FunSuite
  with Matchers
  with MockFactory
  with BeforeAndAfterAll {

  /**
   * Temporary folder for unloading data to
   */
  val tempDir: String = {
    val dir = File.createTempFile("spark_redshift_tests", "")
    dir.delete()
    dir.mkdirs()
    dir.toURI.toString
  }

  /**
   * Expected parsed output corresponding to the output of testData.
   */
  val expectedData =
    Array(
      Row(1.toByte, true, TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0), 1234152.123124981,
        1.0f, 42, 1239012341823719L, 23, "Unicode是樂趣", TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1)),
      Row(1.toByte, false, TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0), 0.0, 0.0f, 42, 1239012341823719L, -13, "asdf",
        TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
      Row(0.toByte, null, TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0), 0.0, -1.0f, 4141214, 1239012341823719L, null, "f",
        TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0)),
      Row(0.toByte, false, null, -1234152.123124981, 100000.0f, null, 1239012341823719L, 24, "___|_123", null),
      Row(List.fill(10)(null): _*))


  /**
   * Spark Context with hadoop file overridden to point at our local test data file for this suite,
   * no-matter what temp directory was generated and requested.
   */
  private var sc: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new TestContext
  }

  override def afterAll(): Unit = {
    val temp = new File(tempDir)
    val tempFiles = temp.listFiles()
    if(tempFiles != null) tempFiles foreach {
      case f => if(f != null) f.delete()
    }
    temp.delete()

    sc.stop()
    super.afterAll()
  }

  /**
   * Set up a mocked JDBCWrapper instance that expects a sequence of queries matching the given
   * regular expressions will be executed, and that the connection returned will be closed.
   */
  def mockJdbcWrapper(expectedUrl: String, expectedQueries: Seq[Regex]): JDBCWrapper = {
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
  def prepareUnloadTest(params: Map[String, String]) = {
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
    val params = Map(
      "url" -> "jdbc:postgresql://foo/bar",
      "tempdir" -> tempDir,
      "dbtable" -> "test_table"
    )

    val jdbcWrapper = prepareUnloadTest(params)
    val testSqlContext = new SQLContext(sc)

    // Assert that we've loaded and converted all data in the test file
    val source = new DefaultSource(jdbcWrapper)
    val relation = source.createRelation(testSqlContext, params)
    val df = testSqlContext.baseRelationToDataFrame(relation)

    df.collect() zip expectedData foreach {
      case (loaded, expected) =>
        loaded shouldBe expected
    }
  }

  test("DefaultSource supports simple column filtering") {
    val params = Map(
      "url" -> "jdbc:postgresql://foo/bar",
      "tempdir" -> tempDir,
      "dbtable" -> "test_table"
    )

    val jdbcWrapper = prepareUnloadTest(params)
    val testSqlContext = new SQLContext(sc)

    // Construct the source with a custom schema
    val source = new DefaultSource(jdbcWrapper)
    val relation = source.createRelation(testSqlContext, params, TestUtils.testSchema)

    val rdd = relation.asInstanceOf[PrunedFilteredScan].buildScan(Array("testByte", "testBool"), Array.empty[Filter])
    val prunedExpectedValues =
      Array(Row(1.toByte, true),
            Row(1.toByte, false),
            Row(0.toByte, null),
            Row(0.toByte, false),
            Row(null, null))

    rdd.collect() zip prunedExpectedValues foreach {
      case (loaded, expected) => loaded shouldBe expected
    }
  }

  test("DefaultSource supports user schema, pruned and filtered scans") {
    val params = Map(
      "url" -> "jdbc:postgresql://foo/bar",
      "tempdir" -> tempDir,
      "dbtable" -> "test_table"
    )

    val jdbcWrapper = prepareUnloadTest(params)
    val testSqlContext = new SQLContext(sc)

    // Construct the source with a custom schema
    val source = new DefaultSource(jdbcWrapper)
    val relation = source.createRelation(testSqlContext, params, TestUtils.testSchema)

    // Define a simple filter to only include a subset of rows
    val filters: Array[Filter] =
      Array(EqualTo("testBool", true),
            EqualTo("testString", "Unicode是樂趣"),
            GreaterThan("testDouble", 1000.0),
            LessThan("testDouble", Double.MaxValue),
            GreaterThanOrEqual("testFloat", 1.0f),
            LessThanOrEqual("testInt", 43))
    val rdd = relation.asInstanceOf[PrunedFilteredScan].buildScan(Array("testByte", "testBool"), filters)

    // We should now only have one matching row, with two columns
    val filteredExpectedValues = Array(Row(1, true))
    rdd.collect() zip filteredExpectedValues foreach {
      case (loaded, expected) => loaded shouldBe expected
    }
  }

  test("DefaultSource using 'query' supports user schema, pruned and filtered scans") {

    val params = Map(
      "url" -> "jdbc:postgresql://foo/bar",
      "tempdir" -> tempDir,
      "query" -> "select * from test_table"
    )

    val jdbcWrapper = prepareUnloadTest(params)
    val testSqlContext = new SQLContext(sc)

    // Construct the source with a custom schema
    val source = new DefaultSource(jdbcWrapper)
    val relation = source.createRelation(testSqlContext, params, TestUtils.testSchema)

    // Define a simple filter to only include a subset of rows
    val filters: Array[Filter] =
      Array(EqualTo("testBool", true),
        EqualTo("testString", "Unicode是樂趣"),
        GreaterThan("testDouble", 1000.0),
        LessThan("testDouble", Double.MaxValue),
        GreaterThanOrEqual("testFloat", 1.0f),
        LessThanOrEqual("testInt", 43))
    val rdd = relation.asInstanceOf[PrunedFilteredScan].buildScan(Array("testByte", "testBool"), filters)

    // We should now only have one matching row, with two columns
    val filteredExpectedValues = Array(Row(1, true))
    rdd.collect() zip filteredExpectedValues foreach {
      case (loaded, expected) => loaded shouldBe expected
    }
  }

  test("DefaultSource serializes data as Avro, then sends Redshift COPY command") {
    val testSqlContext = new SQLContext(sc)

    val jdbcUrl = "jdbc:postgresql://foo/bar"
    val params = Map(
      "url" -> jdbcUrl,
      "tempdir" -> tempDir,
      "dbtable" -> "test_table",
      "postactions" -> "GRANT SELECT ON %s TO jeremy",
      "diststyle" -> "KEY",
      "distkey" -> "testInt"
    )

    val rdd = sc.parallelize(expectedData.toSeq)
    val df = testSqlContext.createDataFrame(rdd, TestUtils.testSchema)

    val expectedCommands =
      Seq("DROP TABLE IF EXISTS test_table_staging_.*".r,
          "CREATE TABLE IF NOT EXISTS test_table_staging.* DISTSTYLE KEY DISTKEY \\(testInt\\).*".r,
          "COPY test_table_staging_.*".r,
          "GRANT SELECT ON test_table_staging.+ TO jeremy".r,
          "ALTER TABLE test_table RENAME TO test_table_backup_.*".r,
          "ALTER TABLE test_table_staging_.* RENAME TO test_table".r,
          "DROP TABLE test_table_backup.*".r)

    val jdbcWrapper = mockJdbcWrapper(jdbcUrl, expectedCommands)

    (jdbcWrapper.tableExists _)
      .expects(*, "test_table")
      .returning(true)
      .anyNumberOfTimes()

    (jdbcWrapper.schemaString _)
      .expects(*, jdbcUrl)
      .returning("schema")
      .anyNumberOfTimes()

    val relation = RedshiftRelation(jdbcWrapper, Parameters.mergeParameters(params), None)(testSqlContext)
    relation.asInstanceOf[InsertableRelation].insert(df, overwrite = true)

    // Make sure we wrote the data out ready for Redshift load, in the expected formats
    val written = testSqlContext.read.format("com.databricks.spark.avro").load(tempDir)
    written.collect() zip expectedData foreach {
      case (loaded, expected) =>
        loaded shouldBe expected
    }
  }

  test("Failed copies are handled gracefully when using a staging table") {
    val testSqlContext = new SQLContext(sc)

    val jdbcUrl = "jdbc:postgresql://foo/bar"
    val params = Map(
      "url" -> jdbcUrl,
      "tempdir" -> tempDir,
      "dbtable" -> "test_table",
      "usestagingtable" -> "true"
    )

    val rdd = sc.parallelize(expectedData.toSeq)
    val df = testSqlContext.createDataFrame(rdd, TestUtils.testSchema)

    val jdbcWrapper = mock[JDBCWrapper]
    val mockedConnection = mock[Connection]

    (jdbcWrapper.getConnector _)
      .expects(*, jdbcUrl, *)
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
      .expects(*, jdbcUrl)
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
    val testSqlContext = new SQLContext(sc)

    val jdbcUrl = "jdbc:postgresql://foo/bar"
    val params = Map(
      "url" -> jdbcUrl,
      "tempdir" -> tempDir,
      "dbtable" -> "test_table"
    )

    val rdd = sc.parallelize(expectedData.toSeq)
    val df = testSqlContext.createDataFrame(rdd, TestUtils.testSchema)

    val expectedCommands =
      Seq("CREATE TABLE IF NOT EXISTS test_table .*".r,
          "COPY test_table .*".r)

    val jdbcWrapper = mockJdbcWrapper(jdbcUrl, expectedCommands)

    (jdbcWrapper.tableExists _)
      .expects(*, "test_table")
      .returning(true)
      .anyNumberOfTimes()

    (jdbcWrapper.schemaString _)
      .expects(*, jdbcUrl)
      .returning("schema")
      .anyNumberOfTimes()

    val source = new DefaultSource(jdbcWrapper)
    val savedDf = source.createRelation(testSqlContext, SaveMode.Append, params, df)

    // This test is "appending" to an empty table, so we expect all our test data to be
    // the only content in the returned data frame
    val written = testSqlContext.read.format("com.databricks.spark.avro").load(tempDir)
    written.collect() zip expectedData foreach {
      case (loaded, expected) =>
        loaded shouldBe expected
    }
  }

  test("Respect SaveMode.ErrorIfExists when table exists") {
    val testSqlContext = new SQLContext(sc)

    val jdbcUrl = "jdbc:postgresql://foo/bar"
    val params = Map(
      "url" -> jdbcUrl,
      "tempdir" -> tempDir,
      "dbtable" -> "test_table"
    )

    val rdd = sc.parallelize(expectedData.toSeq)
    val df = testSqlContext.createDataFrame(rdd, TestUtils.testSchema)

    // Check that SaveMode.ErrorIfExists throws an exception

    val errIfExistsWrapper = mockJdbcWrapper(jdbcUrl, Seq.empty[Regex])

    (errIfExistsWrapper.tableExists _)
      .expects(*, "test_table")
      .returning(true)

    val errIfExistsSource = new DefaultSource(errIfExistsWrapper)
    intercept[Exception] {
      errIfExistsSource.createRelation(testSqlContext, SaveMode.ErrorIfExists, params, df)
    }
  }

  test("Do nothing when table exists if SaveMode = Ignore") {
    val testSqlContext = new SQLContext(sc)

    val jdbcUrl = "jdbc:postgresql://foo/bar"
    val params = Map(
      "url" -> jdbcUrl,
      "tempdir" -> tempDir,
      "dbtable" -> "test_table"
    )

    val rdd = sc.parallelize(expectedData.toSeq)
    val df = testSqlContext.createDataFrame(rdd, TestUtils.testSchema)

    // Check that SaveMode.Ignore does nothing

    val ignoreWrapper = mockJdbcWrapper(jdbcUrl, Seq.empty[Regex])

    (ignoreWrapper.tableExists _)
      .expects(*, "test_table")
      .returning(true)

    // Note: Assertions covered by mocks
    val ignoreSource = new DefaultSource(ignoreWrapper)
    ignoreSource.createRelation(testSqlContext, SaveMode.Ignore, params, df)
  }

  test("Public Scala API rejects invalid parameter maps") {
    val invalid = Map("dbtable" -> "foo") // missing tempdir and url

    val rdd = sc.parallelize(expectedData)
    val testSqlContext = new SQLContext(sc)
    val df = testSqlContext.createDataFrame(rdd, TestUtils.testSchema)

    intercept[Exception] {
      df.saveAsRedshiftTable(invalid)
    }

    intercept[Exception] {
      testSqlContext.redshiftTable(invalid)
    }
  }

  test("DefaultSource has default constructor, required by Data Source API") {
    new DefaultSource()
  }
}
