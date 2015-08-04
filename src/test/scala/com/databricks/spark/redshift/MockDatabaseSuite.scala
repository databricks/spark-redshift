package com.databricks.spark.redshift

import java.sql.{PreparedStatement, Connection}

import com.databricks.spark.redshift.TestUtils._
import org.apache.spark.sql.jdbc.JDBCWrapper

import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite

import scala.util.matching.Regex

class MockDatabaseSuite extends FunSuite with MockFactory {
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
}
