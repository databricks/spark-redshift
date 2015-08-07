package com.databricks.spark.redshift

import java.io.File
import java.sql.Connection

import com.databricks.spark.redshift.TestUtils._

import org.apache.spark.SparkContext
import org.apache.spark.sql.jdbc.JDBCWrapper
import org.apache.spark.sql.types._
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

  var sc: SparkContext = _
  var testSqlContext: SQLContext = _
  var df: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    sc = new TestContext
    testSqlContext = new SQLContext(sc)

    df = testSqlContext.createDataFrame(sc.parallelize(testData), testSchema)
  }

  override def afterAll(): Unit = {
    val temp = new File(tempDir)
    val tempFiles = temp.listFiles()
    if (tempFiles != null) tempFiles foreach {
      case f => if (f != null) f.delete()
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

    val rsOutput: RedshiftWriter = new RedshiftWriter(mockWrapper)

    val params = Parameters.mergeParameters(TestUtils.params)

    rsOutput.createTableSql(df, params) should equal("CREATE TABLE IF NOT EXISTS test_table (testByte BYTE , testBool BOOLEAN , testDate DATE , testDouble DOUBLE PRECISION , testFloat REAL , testInt INTEGER , testLong BIGINT , testShort INTEGER , testString VARCHAR(10) , testTimestamp TIMESTAMP ) DISTSTYLE EVEN")
  }

  test("Metaschema") {
    val enhancedDataframe = MetaSchema.computeEnhancedDf(df)

    enhancedDataframe.schema("testString").metadata.getLong("maxLength") should equal(10)
  }

  test("schema with multiple string columns") {
    val schema = StructType(
      Seq(
        makeField("col1", StringType),
        makeField("col2", StringType),
        makeField("col3", StringType),
        makeField("col4", IntegerType)
      )
    )

    val data = Array(
      Row(null, null, null, null),
      Row(null, "", "", 0),
      Row(null, "A longer string", "", 0),
      Row(null, "2", "", null)
    )

    val stringDf = testSqlContext.createDataFrame(sc.parallelize(data), schema)

    val enhancedDf = MetaSchema.computeEnhancedDf(stringDf)

    enhancedDf.schema("col1").metadata.getLong("maxLength") should equal(0)
    enhancedDf.schema("col2").metadata.getLong("maxLength") should equal(15)
    enhancedDf.schema("col3").metadata.getLong("maxLength") should equal(0)
  }
}
