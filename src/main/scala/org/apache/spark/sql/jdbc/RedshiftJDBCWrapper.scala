package org.apache.spark.sql.jdbc

import java.util.Properties

import org.apache.spark.sql.DataFrame

object RedshiftJDBCWrapper {
  def schemaString(dataFrame: DataFrame, url: String) = JDBCWriteDetails.schemaString(dataFrame, url)
  def registerDriver(driverClass: String) = DriverRegistry.register(driverClass)
  def resolveTable(jdbcUrl: String, table: String, properties: Properties) =
    JDBCRDD.resolveTable(jdbcUrl, table, properties)
  def getConnector(driver: String, url: String, properties: Properties) =
    JDBCRDD.getConnector(driver, url, properties)
}
