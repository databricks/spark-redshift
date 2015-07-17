package com.databricks.spark.redshift

import java.io.File
import java.sql.{Connection, PreparedStatement}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.jdbc.JDBCWrapper
import org.apache.spark.sql.sources._
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.util.matching.Regex

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
  val tempDir = {
    var dir = File.createTempFile("spark_redshift_tests", "")
    dir.delete()
    dir.mkdirs()
    dir.toURI.toString
  }

  /**
   * A text file containing fake unloaded Redshift data of all supported types
   */
  val testData = new File("src/test/resources/redshift_unload_data.txt").toURI.toString

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
    val testConfig = new SparkConf().setMaster("local[1]").setAppName("RedshiftSourceSuite")
    sc = new SparkContext(testConfig) {
      override def newAPIHadoopFile[K, V, F <: InputFormat[K, V]]
      (path: String, fClass: Class[F], kClass: Class[K],
       vClass: Class[V], conf: Configuration = hadoopConfiguration):
      RDD[(K, V)] = {
        super.newAPIHadoopFile[K, V, F](testData, fClass, kClass, vClass, conf)
      }
    }
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
    val wrapper = mock[JDBCWrapper]
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
    val jdbcUrl = params("jdbcurl")
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

    val params = Map("jdbcurl" -> "jdbc:postgresql://foo/bar",
      "tempdir" -> "tmp",
      "redshifttable" -> "test_table",
      "aws_access_key_id" -> "test1",
      "aws_secret_access_key" -> "test2")

    val jdbcWrapper = prepareUnloadTest(params)
    val testSqlContext = new SQLContext(sc)

    // Use the data source to create a relation with given test settings
    val source = new DefaultSource(jdbcWrapper)
    val relation = source.createRelation(testSqlContext, params)

    // Assert that we've loaded and converted all data in the test file
    val rdd = relation.asInstanceOf[TableScan].buildScan()
    rdd.collect() zip expectedData foreach {
      case (loaded, expected) =>
        loaded shouldBe expected
    }
  }

  test("DefaultSource supports simple column filtering") {

    val params = Map("jdbcurl" -> "jdbc:postgresql://foo/bar",
      "tempdir" -> "tmp",
      "redshifttable" -> "test_table",
      "aws_access_key_id" -> "test1",
      "aws_secret_access_key" -> "test2")

    val jdbcWrapper = prepareUnloadTest(params)
    val testSqlContext = new SQLContext(sc)

    // Construct the source with a custom schema
    val source = new DefaultSource(jdbcWrapper)
    val relation = source.createRelation(testSqlContext, params, TestUtils.testSchema)

    val rdd = relation.asInstanceOf[PrunedScan].buildScan(Array("testByte", "testBool"))
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

    val params = Map("jdbcurl" -> "jdbc:postgresql://foo/bar",
      "tempdir" -> "tmp",
      "redshifttable" -> "test_table",
      "aws_access_key_id" -> "test1",
      "aws_secret_access_key" -> "test2")

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
    val params =
      Map("jdbcurl" -> jdbcUrl,
          "tempdir" -> tempDir,
          "redshifttable" -> "test_table",
          "aws_access_key_id" -> "test1",
          "aws_secret_access_key" -> "test2")

    val rdd = sc.parallelize(expectedData.toSeq)
    val df = testSqlContext.createDataFrame(rdd, TestUtils.testSchema)

    val expectedCommands =
      Seq("DROP TABLE IF EXISTS test_table_staging_.*".r,
          "CREATE TABLE IF NOT EXISTS test_table_staging.*".r,
          "COPY test_table_staging_.*".r,
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
      .anyNumberOfTimes()

    val source = new DefaultSource(jdbcWrapper)
    val savedDf = source.createRelation(testSqlContext, SaveMode.Overwrite, params, df)

    // Make sure we wrote the data out ready for Redshift load, in the expected formats
    val written = testSqlContext.read.format("com.databricks.spark.avro").load(tempDir)
    written.collect() zip expectedData foreach {
      case (loaded, expected) =>
        loaded shouldBe expected
    }
  }


}
