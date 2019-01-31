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

import java.net.URI

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * This suite performs basic integration tests where the AWS credentials have been
 * encoded into the tempdir URI rather than being set in the Hadoop configuration.
 */
class AWSCredentialsInUriIntegrationSuite extends IntegrationSuiteBase {

  override protected val tempDir: String = {
    val uri = new URI(AWS_S3_SCRATCH_SPACE + randomSuffix + "/")
    new URI(
      uri.getScheme,
      s"$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY",
      uri.getHost,
      uri.getPort,
      uri.getPath,
      uri.getQuery,
      uri.getFragment).toString
  }


  // Override this method so that we do not set the credentials in sc.hadoopConf.
  override def beforeAll(): Unit = {
    assert(tempDir.contains("AKIA"), "tempdir did not contain AWS credentials")
    sc = new SparkContext("local", getClass.getSimpleName)
    conn = DefaultJDBCWrapper.getConnector(None, jdbcUrl, None)
  }

  test("roundtrip save and load") {
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1)), 1),
      StructType(StructField("foo", IntegerType) :: Nil))
    testRoundtripSaveAndLoad(s"roundtrip_save_and_load_$randomSuffix", df)
  }
}
