package com.databricks.spark.redshift

import java.io.File
import java.sql.Connection

import com.databricks.spark.redshift.TestUtils._

import org.apache.spark.SparkContext
import org.apache.spark.sql.jdbc.JDBCWrapper
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext, Row}
import org.scalatest.{BeforeAndAfterAll, Matchers}
import scala.util.matching.Regex

class SchemaGenerationSuite extends MockDatabaseSuite with Matchers with BeforeAndAfterAll {
  // TODO: DRY ME

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
   * Expected parsed output corresponding to the output of testData.
   */
  val expectedData =
    Array(
      Row(1.toByte, true, toTimestamp(2015, 6, 1, 0, 0, 0), 1234152.123124981,
        1.0f, 42, 1239012341823719L, 23, "Unicode是樂趣", toTimestamp(2015, 6, 1, 0, 0, 0, 1)),
      Row(1.toByte, false, toTimestamp(2015, 6, 2, 0, 0, 0), 0.0, 0.0f, 42, 1239012341823719L, -13, "asdf",
        toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
      Row(0.toByte, null, toTimestamp(2015, 6, 3, 0, 0, 0), 0.0, -1.0f, 4141214, 1239012341823719L, null, "f",
        toTimestamp(2015, 6, 3, 0, 0, 0)),
      Row(0.toByte, false, null, -1234152.123124981, 100000.0f, null, 1239012341823719L, 24, "___|_123", null),
      Row(List.fill(10)(null): _*))

  var sc: SparkContext = _
  var testSqlContext: SQLContext = _
  var df: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    sc = new TestContext
    testSqlContext = new SQLContext(sc)

    df = testSqlContext.createDataFrame(sc.parallelize(expectedData), testSchema)
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

  test("generating the table creation SQL") {
    val expectedCommands = Seq("CREATE TABLE IF NOT EXISTS test_table .*".r)

    val mockWrapper = mock[JDBCWrapper]
    val mockedConnection = mock[Connection]
      //    val mockWrapper = mockJdbcWrapper("url", Seq.empty[Regex])

    val rsOutput:RedshiftWriter = new RedshiftWriter(mockWrapper)

    val params = Parameters.mergeParameters(TestUtils.params)

    rsOutput.createTableSql(df, params) should equal("CREATE TABLE IF NOT EXISTS test_table (testByte BYTE , testBool BOOLEAN , testDate DATE , testDouble DOUBLE PRECISION , testFloat REAL , testInt INTEGER , testLong BIGINT , testShort INTEGER , testString VARCHAR(10) , testTimestamp TIMESTAMP ) DISTSTYLE EVEN")
  }
}
