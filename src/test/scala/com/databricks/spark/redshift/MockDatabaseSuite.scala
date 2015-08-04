package com.databricks.spark.redshift

import java.sql.{SQLException, PreparedStatement, Connection}

import com.databricks.spark.redshift.TestUtils._
import org.apache.spark.sql.jdbc.JDBCWrapper

import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite

import scala.util.matching.Regex

class MockDatabaseSuite extends FunSuite with MockFactory {
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
      .returning(TestUtils.testSchema)
      .anyNumberOfTimes()

    jdbcWrapper
  }
}
