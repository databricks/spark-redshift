package org.apache.spark.sql.jdbc

import java.sql.Connection
import java.util.Properties

import org.apache.spark.sql.DataFrame

/**
 * Hack to access some private JDBC SQL functionality.
 */
object RedshiftJDBCWrapper {
  def schemaString(dataFrame: DataFrame, url: String) = JDBCWriteDetails.schemaString(dataFrame, url)
  def registerDriver(driverClass: String) = DriverRegistry.register(driverClass)
  def resolveTable(jdbcUrl: String, table: String, properties: Properties) =
    JDBCRDD.resolveTable(jdbcUrl, table, properties)
  def getConnector(driver: String, url: String, properties: Properties) =
    JDBCRDD.getConnector(driver, url, properties)
  def tableExists(conn: Connection, table: String) = JdbcUtils.tableExists(conn, table)
}
