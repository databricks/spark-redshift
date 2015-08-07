package com.databricks.spark.redshift

import java.sql.{SQLException, PreparedStatement, Connection}

import com.databricks.spark.redshift.TestUtils._
import org.apache.spark.sql.Row
import org.apache.spark.sql.jdbc.JDBCWrapper
import org.apache.spark.sql.types._

import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite

import scala.util.matching.Regex

class MockDatabaseSuite extends FunSuite with MockFactory {
  /**
   * Makes a field for the test schema
   */
  def makeField(name: String, typ: DataType) = {
    val md = (new MetadataBuilder).putString("name", name).build()
    StructField(name, typ, nullable = true, metadata = md)
  }

  /**
   * Simple schema that includes all data types we support
   */
  lazy val testSchema =
    StructType(
      Seq(
        makeField("testByte", ByteType),
        makeField("testBool", BooleanType),
        makeField("testDate", DateType),
        makeField("testDouble", DoubleType),
        makeField("testFloat", FloatType),
        makeField("testInt", IntegerType),
        makeField("testLong", LongType),
        makeField("testShort", ShortType),
        makeField("testString", StringType),
        makeField("testTimestamp", TimestampType))
    )

  /**
   * Expected parsed output corresponding to the output of testData.
   */
  val testData =
    Array(
      Row(1.toByte, true, toTimestamp(2015, 6, 1, 0, 0, 0), 1234152.123124981,
        1.0f, 42, 1239012341823719L, 23, "Unicode是樂趣", toTimestamp(2015, 6, 1, 0, 0, 0, 1)),
      Row(1.toByte, false, toTimestamp(2015, 6, 2, 0, 0, 0), 0.0, 0.0f, 42, 1239012341823719L, -13, "asdf",
        toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
      Row(0.toByte, null, toTimestamp(2015, 6, 3, 0, 0, 0), 0.0, -1.0f, 4141214, 1239012341823719L, null, "f",
        toTimestamp(2015, 6, 3, 0, 0, 0)),
      Row(0.toByte, false, null, -1234152.123124981, 100000.0f, null, 1239012341823719L, 24, "___|_123", null),
      Row(List.fill(10)(null): _*))

  def successfulStatement(pattern: Regex): PreparedStatement = {
    val mockedConnection = mock[Connection]

    val mockedStatement = mock[PreparedStatement]
    (mockedConnection.prepareStatement(_: String))
      .expects(where {(sql: String) => pattern.findFirstMatchIn(sql).nonEmpty})
      .returning(mockedStatement)
    (mockedStatement.execute _).expects().returning(true)

    mockedStatement
  }

  def failedStatement(pattern: Regex) : PreparedStatement = {
    val mockedConnection = mock[Connection]

    val mockedStatement = mock[PreparedStatement]
    (mockedConnection.prepareStatement(_: String))
      .expects(where {(sql: String) => pattern.findFirstMatchIn(sql).nonEmpty})
      .returning(mockedStatement)

    (mockedStatement.execute _)
      .expects()
      .throwing(new SQLException("Mocked Error"))

    mockedStatement
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
      .returning(testSchema)
      .anyNumberOfTimes()

    jdbcWrapper
  }
}
