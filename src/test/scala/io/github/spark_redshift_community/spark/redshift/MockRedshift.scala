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

package io.github.spark_redshift_community.spark.redshift

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}

import org.apache.spark.sql.types.StructType
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.Assertions._

import scala.collection.mutable
import scala.util.matching.Regex


/**
 * Helper class for mocking Redshift / JDBC in unit tests.
 */
class MockRedshift(
    jdbcUrl: String,
    existingTablesAndSchemas: Map[String, StructType],
    jdbcQueriesThatShouldFail: Seq[Regex] = Seq.empty) {

  private[this] val queriesIssued: mutable.Buffer[String] = mutable.Buffer.empty
  def getQueriesIssuedAgainstRedshift: Seq[String] = queriesIssued.toSeq

  private[this] val jdbcConnections: mutable.Buffer[Connection] = mutable.Buffer.empty

  val jdbcWrapper: JDBCWrapper = spy(new JDBCWrapper)

  private def createMockConnection(): Connection = {
    val conn = mock(classOf[Connection], RETURNS_SMART_NULLS)
    jdbcConnections.append(conn)
    when(conn.prepareStatement(anyString())).thenAnswer(new Answer[PreparedStatement] {
      override def answer(invocation: InvocationOnMock): PreparedStatement = {
        val query = invocation.getArguments()(0).asInstanceOf[String]
        queriesIssued.append(query)
        val mockStatement = mock(classOf[PreparedStatement], RETURNS_SMART_NULLS)
        if (jdbcQueriesThatShouldFail.forall(_.findFirstMatchIn(query).isEmpty)) {
          when(mockStatement.execute()).thenReturn(true)
          when(mockStatement.executeQuery()).thenReturn(
            mock(classOf[ResultSet], RETURNS_SMART_NULLS))
        } else {
          when(mockStatement.execute()).thenThrow(new SQLException(s"Error executing $query"))
          when(mockStatement.executeQuery()).thenThrow(new SQLException(s"Error executing $query"))
        }
        mockStatement
      }
    })
    conn
  }

  doAnswer(new Answer[Connection] {
      override def answer(invocation: InvocationOnMock): Connection = createMockConnection()
    }).when(jdbcWrapper)
      .getConnector(any[Option[String]](), same(jdbcUrl), any[Option[(String, String)]]())

  doAnswer(new Answer[Boolean] {
    override def answer(invocation: InvocationOnMock): Boolean = {
      existingTablesAndSchemas.contains(invocation.getArguments()(1).asInstanceOf[String])
    }
  }).when(jdbcWrapper).tableExists(any[Connection], anyString())

  doAnswer(new Answer[StructType] {
    override def answer(invocation: InvocationOnMock): StructType = {
      existingTablesAndSchemas(invocation.getArguments()(1).asInstanceOf[String])
    }
  }).when(jdbcWrapper).resolveTable(any[Connection], anyString())

  def verifyThatConnectionsWereClosed(): Unit = {
    jdbcConnections.foreach { conn =>
      verify(conn).close()
    }
  }

  def verifyThatRollbackWasCalled(): Unit = {
    jdbcConnections.foreach { conn =>
      verify(conn, atLeastOnce()).rollback()
    }
  }

  def verifyThatCommitWasNotCalled(): Unit = {
    jdbcConnections.foreach { conn =>
      verify(conn, never()).commit()
    }
  }

  def verifyThatExpectedQueriesWereIssued(expectedQueries: Seq[Regex]): Unit = {
    expectedQueries.zip(queriesIssued).foreach { case (expected, actual) =>
      if (expected.findFirstMatchIn(actual).isEmpty) {
        fail(
          s"""
             |Actual and expected JDBC queries did not match:
             |Expected: $expected
             |Actual: $actual
           """.stripMargin)
      }
    }
    if (expectedQueries.length > queriesIssued.length) {
      val missingQueries = expectedQueries.drop(queriesIssued.length)
      fail(s"Missing ${missingQueries.length} expected JDBC queries:" +
        s"\n${missingQueries.mkString("\n")}")
    } else if (queriesIssued.length > expectedQueries.length) {
      val extraQueries = queriesIssued.drop(expectedQueries.length)
      fail(s"Got ${extraQueries.length} unexpected JDBC queries:\n${extraQueries.mkString("\n")}")
    }
  }
}
