/*
 * Copyright 2016 Databricks
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

import java.sql.SQLException

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Integration tests for configuring Redshift to access S3 using Amazon IAM roles.
 */
class IAMIntegrationSuite extends IntegrationSuiteBase {

  private val IAM_ROLE_ARN: String = loadConfigFromEnv("STS_ROLE_ARN")

  // TODO (luca|issue #8) Fix IAM Authentication tests
  ignore("roundtrip save and load") {
    val tableName = s"iam_roundtrip_save_and_load$randomSuffix"
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
      StructType(StructField("a", IntegerType) :: Nil))
    try {
      write(df)
        .option("dbtable", tableName)
        .option("forward_spark_s3_credentials", "false")
        .option("aws_iam_role", IAM_ROLE_ARN)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = read
        .option("dbtable", tableName)
        .option("forward_spark_s3_credentials", "false")
        .option("aws_iam_role", IAM_ROLE_ARN)
        .load()
      assert(loadedDf.schema.length === 1)
      assert(loadedDf.columns === Seq("a"))
      checkAnswer(loadedDf, Seq(Row(1)))
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
    }
  }

  ignore("load fails if IAM role cannot be assumed") {
    val tableName = s"iam_load_fails_if_role_cannot_be_assumed$randomSuffix"
    try {
      val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("a", IntegerType) :: Nil))
      val err = intercept[SQLException] {
        write(df)
          .option("dbtable", tableName)
          .option("forward_spark_s3_credentials", "false")
          .option("aws_iam_role", IAM_ROLE_ARN + "-some-bogus-suffix")
          .mode(SaveMode.ErrorIfExists)
          .save()
      }
      assert(err.getCause.getMessage.contains("is not authorized to assume IAM Role"))
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
    }
  }
}
