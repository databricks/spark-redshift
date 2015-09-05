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
import java.net.URI
import java.sql.{Connection, PreparedStatement, SQLException}

import scala.util.matching.Regex

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule
import org.mockito.Mockito
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.fs.s3native.S3NInMemoryFileSystem
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, Matchers}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import com.databricks.spark.redshift.Parameters.MergedParameters

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
  extends QueryTest
  with Matchers
  with MockFactory
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  /**
   * Spark Context with Hadoop file overridden to point at our local test data file for this suite,
   * no matter what temp directory was generated and requested.
   */
  private var sc: SparkContext = _

  private var testSqlContext: SQLContext = _

  private var expectedDataDF: DataFrame = _

  private var mockS3Client: AmazonS3Client = _

  private var s3FileSystem: FileSystem = _

  private val s3TempDir: String = "s3n://test-bucket/temp-dir/"

  // Parameters common to most tests. Some parameters are overridden in specific tests.
  private def defaultParams: Map[String, String] = Map(
    "url" -> "jdbc:redshift://foo/bar",
    "tempdir" -> s3TempDir,
    "dbtable" -> "test_table")

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new TestContext
    sc.hadoopConfiguration.set("fs.s3n.impl", classOf[S3NInMemoryFileSystem].getName)
    // We need to use a DirectOutputCommitter to work around an issue which occurs with renames
    // while using the mocked S3 filesystem.
    sc.hadoopConfiguration.set("mapred.output.committer.class",
      classOf[DirectOutputCommitter].getName)
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "test1")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "test2")
    // Configure a mock S3 client so that we don't hit errors when trying to access AWS in tests.
    // ScalaMock cannot mock classes which contain final methods, even if the classes themselves
    // are not final, so we use Mockito instead:
    mockS3Client = Mockito.mock(classOf[AmazonS3Client], Mockito.RETURNS_SMART_NULLS)
    Mockito.when(mockS3Client.getBucketLifecycleConfiguration(org.mockito.Matchers.any()))
      .thenReturn(new BucketLifecycleConfiguration().withRules(
        new Rule().withPrefix("").withStatus(BucketLifecycleConfiguration.ENABLED)
      ))
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    s3FileSystem = FileSystem.get(new URI(s3TempDir), sc.hadoopConfiguration)
    testSqlContext = new SQLContext(sc)
    expectedDataDF =
      testSqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    testSqlContext = null
    expectedDataDF = null
    FileSystem.closeAll()
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

    (jdbcWrapper.getConnector _).expects(*, expectedUrl).returning(mockedConnection)

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
  private def prepareUnloadTest(
      params: Map[String, String],
      expectedQueries: Seq[Regex]): JDBCWrapper = {
    val jdbcUrl = params("url")
    val jdbcWrapper = mockJdbcWrapper(jdbcUrl, expectedQueries)

    // We expect some extra calls to the JDBC wrapper,
    // to register the driver and retrieve the schema.
    (jdbcWrapper.registerDriver _)
      .expects(*)
      .anyNumberOfTimes()
    (jdbcWrapper.resolveTable _)
      .expects(jdbcUrl, "test_table")
      .returning(TestUtils.testSchema)
      .anyNumberOfTimes()

    jdbcWrapper
  }

  test("DefaultSource can load Redshift UNLOAD output to a DataFrame") {
    val expectedQuery = (
      "UNLOAD \\('SELECT \"testbyte\", \"testbool\", \"testdate\", \"testdouble\"," +
      " \"testfloat\", \"testint\", \"testlong\", \"testshort\", \"teststring\", " +
      "\"testtimestamp\" " +
      "FROM test_table '\\) " +
      "TO '.*' " +
      "WITH CREDENTIALS 'aws_access_key_id=test1;aws_secret_access_key=test2' " +
      "ESCAPE").r
    val jdbcWrapper = prepareUnloadTest(defaultParams, Seq(expectedQuery))

    // Assert that we've loaded and converted all data in the test file
    val source = new DefaultSource(jdbcWrapper, _ => mockS3Client)
    val relation = source.createRelation(testSqlContext, defaultParams)
    val df = testSqlContext.baseRelationToDataFrame(relation)
    checkAnswer(df, TestUtils.expectedData)
  }

  test("Can load output of Redshift queries") {
    // scalastyle:off
    val expectedJDBCQuery =
      """
        |UNLOAD \('SELECT "testbyte", "testbool" FROM
        |  \(select testbyte, testbool
        |    from test_table
        |    where teststring = \\'Unicode\\'\\'s樂趣\\'\) '\)
      """.stripMargin.lines.map(_.trim).mkString(" ").trim.r
    val query =
      """select testbyte, testbool from test_table where teststring = 'Unicode''s樂趣'"""
    // scalastyle:on
    val querySchema =
      StructType(Seq(StructField("testbyte", ByteType), StructField("testbool", BooleanType)))

    // Test with dbtable parameter that wraps the query in parens:
    {
      val params = defaultParams + ("dbtable" -> s"($query)")
      val jdbcWrapper = mockJdbcWrapper(params("url"), Seq(expectedJDBCQuery))
      (jdbcWrapper.registerDriver _)
        .expects(*)
        .anyNumberOfTimes()
      (jdbcWrapper.resolveTable _)
        .expects(params("url"), *)
        .returning(querySchema)
        .anyNumberOfTimes()
      // Note: Assertions covered by mocks
      val relation =
        new DefaultSource(jdbcWrapper, _ => mockS3Client).createRelation(testSqlContext, params)
      testSqlContext.baseRelationToDataFrame(relation).collect()
    }

    // Test with query parameter
    {
      val params = defaultParams - "dbtable" + ("query" -> query)
      val jdbcWrapper = mockJdbcWrapper(params("url"), Seq(expectedJDBCQuery))
      (jdbcWrapper.registerDriver _)
        .expects(*)
        .anyNumberOfTimes()
      (jdbcWrapper.resolveTable _)
        .expects(params("url"), *)
        .returning(querySchema)
        .anyNumberOfTimes()
      // Note: Assertions covered by mocks
      val relation =
        new DefaultSource(jdbcWrapper, _ => mockS3Client).createRelation(testSqlContext, params)
      testSqlContext.baseRelationToDataFrame(relation).collect()
    }
  }

  test("DefaultSource supports simple column filtering") {
    val expectedQuery = (
      "UNLOAD \\('SELECT \"testbyte\", \"testbool\" FROM test_table '\\) " +
      "TO '.*' " +
      "WITH CREDENTIALS 'aws_access_key_id=test1;aws_secret_access_key=test2' " +
      "ESCAPE").r
    val jdbcWrapper = prepareUnloadTest(defaultParams, Seq(expectedQuery))
    // Construct the source with a custom schema
    val source = new DefaultSource(jdbcWrapper, _ => mockS3Client)
    val relation = source.createRelation(testSqlContext, defaultParams, TestUtils.testSchema)

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), Array.empty[Filter])
    val prunedExpectedValues = Array(
      Row(1.toByte, true),
      Row(1.toByte, false),
      Row(0.toByte, null),
      Row(0.toByte, false),
      Row(null, null))
    assert(rdd.collect() === prunedExpectedValues)
  }

  test("DefaultSource supports user schema, pruned and filtered scans") {
    // scalastyle:off
    val expectedQuery = (
      "UNLOAD \\('SELECT \"testbyte\", \"testbool\" " +
        "FROM test_table " +
        "WHERE \"testbool\" = true " +
        "AND \"teststring\" = \\\\'Unicode\\\\'\\\\'s樂趣\\\\' " +
        "AND \"testdouble\" > 1000.0 " +
        "AND \"testdouble\" < 1.7976931348623157E308 " +
        "AND \"testfloat\" >= 1.0 " +
        "AND \"testint\" <= 43'\\) " +
      "TO '.*' " +
      "WITH CREDENTIALS 'aws_access_key_id=test1;aws_secret_access_key=test2' " +
      "ESCAPE").r
    // scalastyle:on
    val jdbcWrapper = prepareUnloadTest(defaultParams, Seq(expectedQuery))

    // Construct the source with a custom schema
    val source = new DefaultSource(jdbcWrapper, _ => mockS3Client)
    val relation = source.createRelation(testSqlContext, defaultParams, TestUtils.testSchema)

    // Define a simple filter to only include a subset of rows
    val filters: Array[Filter] = Array(
      EqualTo("testbool", true),
      // scalastyle:off
      EqualTo("teststring", "Unicode's樂趣"),
      // scalastyle:on
      GreaterThan("testdouble", 1000.0),
      LessThan("testdouble", Double.MaxValue),
      GreaterThanOrEqual("testfloat", 1.0f),
      LessThanOrEqual("testint", 43))
    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testbool"), filters)

    // Technically this assertion should check that the RDD only returns a single row, but
    // since we've mocked out Redshift our WHERE clause won't have had any effect.
    assert(rdd.collect().contains(Row(1, true)))
  }

  test("DefaultSource serializes data as Avro, then sends Redshift COPY command") {
    val params = defaultParams ++ Map(
      "postactions" -> "GRANT SELECT ON %s TO jeremy",
      "diststyle" -> "KEY",
      "distkey" -> "testint")

    val expectedCommands = Seq(
      "DROP TABLE IF EXISTS test_table_staging_.*".r,
      "CREATE TABLE IF NOT EXISTS test_table_staging.* DISTSTYLE KEY DISTKEY \\(testint\\).*".r,
      "COPY test_table_staging_.*".r,
      "GRANT SELECT ON test_table_staging.+ TO jeremy".r,
      "ALTER TABLE test_table RENAME TO test_table_backup_.*".r,
      "ALTER TABLE test_table_staging_.* RENAME TO test_table".r,
      "DROP TABLE IF EXISTS test_table_backup.*".r)

    val jdbcWrapper = mockJdbcWrapper(params("url"), expectedCommands)

    (jdbcWrapper.tableExists _)
      .expects(*, "test_table")
      .returning(true)
      .anyNumberOfTimes()

    (jdbcWrapper.schemaString _)
      .expects(*)
      .returning("schema")
      .anyNumberOfTimes()

    val relation = RedshiftRelation(
      jdbcWrapper, _ => mockS3Client, Parameters.mergeParameters(params), None)(testSqlContext)
    relation.asInstanceOf[InsertableRelation].insert(expectedDataDF, true)

    // Make sure we wrote the data out ready for Redshift load, in the expected formats.
    // The data should have been written to a random subdirectory of `tempdir`. Since we clear
    // `tempdir` between every unit test, there should only be one directory here.
    assert(s3FileSystem.listStatus(new Path(s3TempDir)).length === 1)
    val dirWithAvroFiles = s3FileSystem.listStatus(new Path(s3TempDir)).head.getPath.toUri.toString
    val written = testSqlContext.read.format("com.databricks.spark.avro").load(dirWithAvroFiles)
    checkAnswer(written, TestUtils.expectedDataWithConvertedTimesAndDates)
  }

  test("Cannot write table with column names that become ambiguous under case insensitivity") {
    val jdbcWrapper = mock[JDBCWrapper]
    val mockedConnection = mock[Connection]
    (jdbcWrapper.getConnector _)
      .expects(*, defaultParams("url"))
      .returning(mockedConnection)
    (mockedConnection.close _).expects()

    val schema = StructType(Seq(StructField("a", IntegerType), StructField("A", IntegerType)))
    val df = testSqlContext.createDataFrame(sc.emptyRDD[Row], schema)
    val writer = new RedshiftWriter(jdbcWrapper, _ => mockS3Client)

    intercept[IllegalArgumentException] {
      writer.saveToRedshift(testSqlContext, df, Parameters.mergeParameters(defaultParams))
    }
  }

  test("Failed copies are handled gracefully when using a staging table") {
    val params = defaultParams ++ Map("usestagingtable" -> "true")

    val jdbcWrapper = mock[JDBCWrapper]
    val mockedConnection = mock[Connection]

    (jdbcWrapper.getConnector _)
      .expects(*, params("url"))
      .returning(mockedConnection)

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
      .expects(*)
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

    val source = new DefaultSource(jdbcWrapper, _ => mockS3Client)
    intercept[Exception] {
      source.createRelation(testSqlContext, SaveMode.Overwrite, params, expectedDataDF)
    }
  }

  test("Append SaveMode doesn't destroy existing data") {
    val expectedCommands =
      Seq("CREATE TABLE IF NOT EXISTS test_table .*".r,
          "COPY test_table .*".r)

    val jdbcWrapper = mockJdbcWrapper(defaultParams("url"), expectedCommands)

    (jdbcWrapper.tableExists _)
      .expects(*, "test_table")
      .returning(true)
      .anyNumberOfTimes()

    (jdbcWrapper.schemaString _)
      .expects(*)
      .returning("schema")
      .anyNumberOfTimes()

    val source = new DefaultSource(jdbcWrapper, _ => mockS3Client)
    val savedDf =
      source.createRelation(testSqlContext, SaveMode.Append, defaultParams, expectedDataDF)

    // This test is "appending" to an empty table, so we expect all our test data to be
    // the only content in the returned data frame.
    // The data should have been written to a random subdirectory of `tempdir`. Since we clear
    // `tempdir` between every unit test, there should only be one directory here.
    assert(s3FileSystem.listStatus(new Path(s3TempDir)).length === 1)
    val dirWithAvroFiles = s3FileSystem.listStatus(new Path(s3TempDir)).head.getPath.toUri.toString
    val written = testSqlContext.read.format("com.databricks.spark.avro").load(dirWithAvroFiles)
    checkAnswer(written, TestUtils.expectedDataWithConvertedTimesAndDates)
  }

  test("configuring maxlength on string columns") {
    val longStrMetadata = new MetadataBuilder().putLong("maxlength", 512).build()
    val shortStrMetadata = new MetadataBuilder().putLong("maxlength", 10).build()
    val schema = StructType(
      StructField("long_str", StringType, metadata = longStrMetadata) ::
      StructField("short_str", StringType, metadata = shortStrMetadata) ::
      StructField("default_str", StringType) ::
      Nil)
    val df = testSqlContext.createDataFrame(sc.emptyRDD[Row], schema)
    val createTableCommand =
      DefaultRedshiftWriter.createTableSql(df, MergedParameters.apply(defaultParams)).trim
    val expectedCreateTableCommand =
      "CREATE TABLE IF NOT EXISTS test_table (long_str VARCHAR(512), short_str VARCHAR(10), " +
        "default_str TEXT)"
    assert(createTableCommand === expectedCreateTableCommand)
  }

  test("Respect SaveMode.ErrorIfExists when table exists") {
    val errIfExistsWrapper = mockJdbcWrapper(defaultParams("url"), Seq.empty[Regex])

    (errIfExistsWrapper.tableExists _)
      .expects(*, "test_table")
      .returning(true)

    val errIfExistsSource = new DefaultSource(errIfExistsWrapper, _ => mockS3Client)
    intercept[Exception] {
      errIfExistsSource.createRelation(
        testSqlContext, SaveMode.ErrorIfExists, defaultParams, expectedDataDF)
    }
  }

  test("Do nothing when table exists if SaveMode = Ignore") {
    val ignoreWrapper = mockJdbcWrapper(defaultParams("url"), Seq.empty[Regex])

    (ignoreWrapper.tableExists _)
      .expects(*, "test_table")
      .returning(true)

    // Note: Assertions covered by mocks
    val ignoreSource = new DefaultSource(ignoreWrapper, _ => mockS3Client)
    ignoreSource.createRelation(testSqlContext, SaveMode.Ignore, defaultParams, expectedDataDF)
  }

  test("Cannot save when 'query' parameter is specified instead of 'dbtable'") {
    val invalidParams = Map(
      "url" -> "jdbc:redshift://foo/bar",
      "tempdir" -> s3TempDir,
      "query" -> "select * from test_table")

    val e1 = intercept[IllegalArgumentException] {
      expectedDataDF.saveAsRedshiftTable(invalidParams)
    }
    assert(e1.getMessage.contains("dbtable"))
  }

  test("Public Scala API rejects invalid parameter maps") {
    val invalidParams = Map("dbtable" -> "foo") // missing tempdir and url

    val e1 = intercept[IllegalArgumentException] {
      expectedDataDF.saveAsRedshiftTable(invalidParams)
    }
    assert(e1.getMessage.contains("tempdir"))

    val e2 = intercept[IllegalArgumentException] {
      testSqlContext.redshiftTable(invalidParams)
    }
    assert(e2.getMessage.contains("tempdir"))
  }

  test("DefaultSource has default constructor, required by Data Source API") {
    new DefaultSource()
  }
}
