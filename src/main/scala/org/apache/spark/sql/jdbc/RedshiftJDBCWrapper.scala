/*
 * Copyright 2015 TouchType Ltd
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

package org.apache.spark.sql.jdbc

import java.sql.Connection
import java.util.Properties

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * Hack to access some private JDBC SQL functionality
 */
class JDBCWrapper {
  def schemaString(dataFrame: DataFrame, url: String): String = {
    JDBCWriteDetails.schemaString(dataFrame, url)
  }

  def registerDriver(driverClass: String): Unit = {
    DriverRegistry.register(driverClass)
  }

  def resolveTable(jdbcUrl: String, table: String, properties: Properties): StructType = {
    JDBCRDD.resolveTable(jdbcUrl, table, properties)
  }

  def getConnector(driver: String, url: String, properties: Properties): () => Connection = {
    JDBCRDD.getConnector(driver, url, properties)
  }

  def tableExists(conn: Connection, table: String): Boolean = {
    JdbcUtils.tableExists(conn, table)
  }
}

object DefaultJDBCWrapper extends JDBCWrapper
