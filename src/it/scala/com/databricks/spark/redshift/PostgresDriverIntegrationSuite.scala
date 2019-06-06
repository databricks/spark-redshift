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

package com.databricks.spark.redshift

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
  * Basic integration tests with the Postgres JDBC driver.
  */
class PostgresDriverIntegrationSuite extends IntegrationSuiteBase {

  override def jdbcUrl: String = {
    super.jdbcUrl.replace("jdbc:redshift", "jdbc:postgresql")
  }

  // TODO (luca|COREML-825 Fix tests when using postgresql driver
  ignore("postgresql driver takes precedence for jdbc:postgresql:// URIs") {
    val conn = DefaultJDBCWrapper.getConnector(None, jdbcUrl, None)
    try {
      assert(conn.getClass.getName === "org.postgresql.jdbc4.Jdbc4Connection")
    } finally {
      conn.close()
    }
  }

  ignore("roundtrip save and load") {
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1)), 1),
      StructType(StructField("foo", IntegerType) :: Nil))
    testRoundtripSaveAndLoad(s"save_with_one_empty_partition_$randomSuffix", df)
  }
}
