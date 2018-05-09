/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.redshift

import java.sql.{ResultSet, PreparedStatement, Connection, Driver, DriverManager, ResultSetMetaData, SQLException}
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ThreadFactory, Executors}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
 * Shim which exposes some JDBC helper functions. Most of this code is copied from Spark SQL, with
 * minor modifications for Redshift-specific features and limitations.
 */
private[redshift] class JDBCWrapper {

  private val log = LoggerFactory.getLogger(getClass)

  private val ec: ExecutionContext = {
    val threadFactory = new ThreadFactory {
      private[this] val count = new AtomicInteger()
      override def newThread(r: Runnable) = {
        val thread = new Thread(r)
        thread.setName(s"spark-redshift-JDBCWrapper-${count.incrementAndGet}")
        thread.setDaemon(true)
        thread
      }
    }
    ExecutionContext.fromExecutorService(Executors.newCachedThreadPool(threadFactory))
  }

  /**
   * Given a JDBC subprotocol, returns the name of the appropriate driver class to use.
   *
   * If the user has explicitly specified a driver class in their configuration then that class will
   * be used. Otherwise, we will attempt to load the correct driver class based on
   * the JDBC subprotocol.
   *
   * @param jdbcSubprotocol 'redshift' or 'postgresql'
   * @param userProvidedDriverClass an optional user-provided explicit driver class name
   * @return the driver class
   */
  private def getDriverClass(
      jdbcSubprotocol: String,
      userProvidedDriverClass: Option[String]): String = {
    userProvidedDriverClass.getOrElse {
      jdbcSubprotocol match {
        case "redshift" =>
          try {
            Utils.classForName("com.amazon.redshift.jdbc42.Driver").getName
          } catch {
            case _: ClassNotFoundException =>
              try {
                Utils.classForName("com.amazon.redshift.jdbc41.Driver").getName
              } catch {
                case _: ClassNotFoundException =>
                  try {
                    Utils.classForName("com.amazon.redshift.jdbc4.Driver").getName
                  } catch {
                    case e: ClassNotFoundException =>
                      throw new ClassNotFoundException(
                        "Could not load an Amazon Redshift JDBC driver; see the README for " +
                          "instructions on downloading and configuring the official Amazon driver.",
                        e
                      )
                  }
              }
          }
        case "postgresql" => "org.postgresql.Driver"
        case other => throw new IllegalArgumentException(s"Unsupported JDBC protocol: '$other'")
      }
    }
  }

  /**
   * Execute the given SQL statement while supporting interruption.
   * If InterruptedException is caught, then the statement will be cancelled if it is running.
   *
   * @return <code>true</code> if the first result is a <code>ResultSet</code>
   *         object; <code>false</code> if the first result is an update
   *         count or there is no result
   */
  def executeInterruptibly(statement: PreparedStatement): Boolean = {
    executeInterruptibly(statement, _.execute())
  }

  /**
   * Execute the given SQL statement while supporting interruption.
   * If InterruptedException is caught, then the statement will be cancelled if it is running.
   *
   * @return a <code>ResultSet</code> object that contains the data produced by the
   *         query; never <code>null</code>
   */
  def executeQueryInterruptibly(statement: PreparedStatement): ResultSet = {
    executeInterruptibly(statement, _.executeQuery())
  }

  private def executeInterruptibly[T](
      statement: PreparedStatement,
      op: PreparedStatement => T): T = {
    try {
      val future = Future[T](op(statement))(ec)
      try {
        Await.result(future, Duration.Inf)
      } catch {
        case e: SQLException =>
          // Wrap and re-throw so that this thread's stacktrace appears to the user.
          throw new SQLException("Exception thrown in awaitResult: ", e)
        case NonFatal(t) =>
          // Wrap and re-throw so that this thread's stacktrace appears to the user.
          throw new Exception("Exception thrown in awaitResult: ", t)
      }
    } catch {
      case e: InterruptedException =>
        try {
          statement.cancel()
          throw e
        } catch {
          case s: SQLException =>
            log.error("Exception occurred while cancelling query", s)
            throw e
        }
    }
  }

  /**
   * Takes a (schema, table) specification and returns the table's Catalyst
   * schema.
   *
   * @param conn A JDBC connection to the database.
   * @param table The table name of the desired table.  This may also be a
   *   SQL query wrapped in parentheses.
   *
   * @return A StructType giving the table's Catalyst schema.
   * @throws SQLException if the table specification is garbage.
   * @throws SQLException if the table contains an unsupported type.
   */
  def resolveTable(conn: Connection, table: String): StructType = {
    // It's important to leave the `LIMIT 1` clause in order to limit the work of the query in case
    // the underlying JDBC driver implementation implements PreparedStatement.getMetaData() by
    // executing the query. It looks like the standard Redshift and Postgres JDBC drivers don't do
    // this but we leave the LIMIT condition here as a safety-net to guard against perf regressions.
    val ps = conn.prepareStatement(s"SELECT * FROM $table LIMIT 1")
    try {
      val rsmd = executeInterruptibly(ps, _.getMetaData)
      val ncols = rsmd.getColumnCount
      val fields = new Array[StructField](ncols)
      var i = 0
      while (i < ncols) {
        val columnName = rsmd.getColumnLabel(i + 1)
        val dataType = rsmd.getColumnType(i + 1)
        val fieldSize = rsmd.getPrecision(i + 1)
        val fieldScale = rsmd.getScale(i + 1)
        val isSigned = rsmd.isSigned(i + 1)
        val nullable = rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
        val columnType = getCatalystType(dataType, fieldSize, fieldScale, isSigned)
        fields(i) = StructField(columnName, columnType, nullable)
        i = i + 1
      }
      new StructType(fields)
    } finally {
      ps.close()
    }
  }

  /**
   * Given a driver string and a JDBC url, load the specified driver and return a DB connection.
   *
   * @param userProvidedDriverClass the class name of the JDBC driver for the given url. If this
   *                                is None then `spark-redshift` will attempt to automatically
   *                                discover the appropriate driver class.
   * @param url the JDBC url to connect to.
   */
  def getConnector(
      userProvidedDriverClass: Option[String],
      url: String,
      credentials: Option[(String, String)]) : Connection = {
    val subprotocol = url.stripPrefix("jdbc:").split(":")(0)
    val driverClass: String = getDriverClass(subprotocol, userProvidedDriverClass)
    DriverRegistry.register(driverClass)
    val driverWrapperClass: Class[_] = if (SPARK_VERSION.startsWith("1.4")) {
      Utils.classForName("org.apache.spark.sql.jdbc.package$DriverWrapper")
    } else { // Spark 1.5.0+
      Utils.classForName("org.apache.spark.sql.execution.datasources.jdbc.DriverWrapper")
    }
    def getWrapped(d: Driver): Driver = {
      require(driverWrapperClass.isAssignableFrom(d.getClass))
      driverWrapperClass.getDeclaredMethod("wrapped").invoke(d).asInstanceOf[Driver]
    }
    // Note that we purposely don't call DriverManager.getConnection() here: we want to ensure
    // that an explicitly-specified user-provided driver class can take precedence over the default
    // class, but DriverManager.getConnection() might return a according to a different precedence.
    // At the same time, we don't want to create a driver-per-connection, so we use the
    // DriverManager's driver instances to handle that singleton logic for us.
    val driver: Driver = DriverManager.getDrivers.asScala.collectFirst {
      case d if driverWrapperClass.isAssignableFrom(d.getClass)
        && getWrapped(d).getClass.getCanonicalName == driverClass => d
      case d if d.getClass.getCanonicalName == driverClass => d
    }.getOrElse {
      throw new IllegalArgumentException(s"Did not find registered driver with class $driverClass")
    }
    val properties = new Properties()
    credentials.foreach { case(user, password) =>
      properties.setProperty("user", user)
      properties.setProperty("password", password)
    }
    driver.connect(url, properties)
  }

  /**
   * Compute the SQL schema string for the given Spark SQL Schema.
   */
  def schemaString(schema: StructType): String = {
    val sb = new StringBuilder()
    schema.fields.foreach { field => {
      val name = field.name
      val typ: String = if (field.metadata.contains("redshift_type")) {
        field.metadata.getString("redshift_type")
      } else {
        field.dataType match {
          case IntegerType => "INTEGER"
          case LongType => "BIGINT"
          case DoubleType => "DOUBLE PRECISION"
          case FloatType => "REAL"
          case ShortType => "INTEGER"
          case ByteType => "SMALLINT" // Redshift does not support the BYTE type.
          case BooleanType => "BOOLEAN"
          case StringType =>
            if (field.metadata.contains("maxlength")) {
              s"VARCHAR(${field.metadata.getLong("maxlength")})"
            } else {
              "TEXT"
            }
          case TimestampType => "TIMESTAMP"
          case DateType => "DATE"
          case t: DecimalType => s"DECIMAL(${t.precision},${t.scale})"
          case _ => throw new IllegalArgumentException(s"Don't know how to save $field to JDBC")
        }
      }

      val nullable = if (field.nullable) "" else "NOT NULL"
      val encoding = if (field.metadata.contains("encoding")) {
        s"ENCODE ${field.metadata.getString("encoding")}"
      } else {
        ""
      }
      sb.append(s""", "${name.replace("\"", "\\\"")}" $typ $nullable $encoding""".trim)
    }}
    if (sb.length < 2) "" else sb.substring(2)
  }

  /**
   * Returns true if the table already exists in the JDBC database.
   */
  def tableExists(conn: Connection, table: String): Boolean = {
    // Somewhat hacky, but there isn't a good way to identify whether a table exists for all
    // SQL database systems, considering "table" could also include the database name.
    Try {
      val stmt = conn.prepareStatement(s"SELECT 1 FROM $table LIMIT 1")
      executeInterruptibly(stmt, _.getMetaData).getColumnCount
    }.isSuccess
  }

  /**
   * Maps a JDBC type to a Catalyst type.
   *
   * @param sqlType - A field of java.sql.Types
   * @return The Catalyst type corresponding to sqlType.
   */
  private def getCatalystType(
      sqlType: Int,
      precision: Int,
      scale: Int,
      signed: Boolean): DataType = {
    // TODO: cleanup types which are irrelevant for Redshift.
    val answer = sqlType match {
      // scalastyle:off
      // Null Type
      case java.sql.Types.NULL          => null

      // Character Types
      case java.sql.Types.CHAR          => StringType
      case java.sql.Types.NCHAR         => StringType
      case java.sql.Types.NVARCHAR      => StringType
      case java.sql.Types.VARCHAR       => StringType

      // Datetime Types
      case java.sql.Types.DATE          => DateType
      case java.sql.Types.TIME          => TimestampType
      case java.sql.Types.TIMESTAMP     => TimestampType

      // Boolean Type
      case java.sql.Types.BOOLEAN       => BooleanType

      // Numeric Types
      case java.sql.Types.BIGINT        => if (signed) { LongType } else { DecimalType(20,0) }
      case java.sql.Types.DECIMAL
        if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.DECIMAL       => DecimalType(38, 18) // Spark 1.5.0 default
      case java.sql.Types.DOUBLE        => DoubleType
      case java.sql.Types.FLOAT         => FloatType
      case java.sql.Types.INTEGER       => if (signed) { IntegerType } else { LongType }
      case java.sql.Types.NUMERIC
        if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.NUMERIC       => DecimalType(38, 18) // Spark 1.5.0 default
      case java.sql.Types.REAL          => FloatType
      case java.sql.Types.SMALLINT      => IntegerType
      case java.sql.Types.TINYINT       => IntegerType
      // scalastyle:on
    }

    if (answer == null) throw new SQLException("Unsupported type " + sqlType)
    answer
  }
}

private[redshift] object DefaultJDBCWrapper extends JDBCWrapper
