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

package com.databricks.spark.redshift

import org.apache.hadoop.conf.Configuration
import org.scalatest.{FunSuite, Matchers}

/**
 * Check validation of parameter config
 */
class ParametersSuite extends FunSuite with Matchers {

  test("minimal valid parameter map is accepted") {
    val params =
      Map(
        "tempdir" -> "s3://foo/bar",
        "dbtable" -> "test_table",
        "url" -> "jdbc:postgresql://foo/bar")

    val mergedParams = Parameters.mergeParameters(params)

    mergedParams.tempPath should startWith(params("tempdir"))
    mergedParams.jdbcUrl shouldBe params("url")
    mergedParams.table shouldBe params("dbtable")
    mergedParams.tableOrQuery shouldBe params("dbtable")

    // Check that the defaults have been added
    Parameters.DEFAULT_PARAMETERS foreach {
      case (k, v) => mergedParams.parameters(k) shouldBe v
    }
  }

  test("new instances have distinct temp paths") {
    val params =
      Map(
        "tempdir" -> "s3://foo/bar",
        "dbtable" -> "test_table",
        "url" -> "jdbc:postgresql://foo/bar")

    val mergedParams1 = Parameters.mergeParameters(params)
    val mergedParams2 = Parameters.mergeParameters(params)

    mergedParams1.tempPath should not equal mergedParams2.tempPath
  }

  test("errors are thrown when mandatory parameters are not provided") {

    def checkMerge(params: Map[String, String]): Unit = {
      intercept[Exception] {
        Parameters.mergeParameters(params)
      }
    }

    checkMerge(Map("dbtable" -> "test_table", "url" -> "jdbc:postgresql://foo/bar"))
    checkMerge(Map("tempdir" -> "s3://foo/bar", "url" -> "jdbc:postgresql://foo/bar"))
    checkMerge(Map("dbtable" -> "test_table", "tempdir" -> "s3://foo/bar"))
  }

  test("credentials coded in tempdir overwrite hadoop configuration") {
    val params =
      Map(
        "tempdir" -> "s3://keyId1:secretKey1@foo/bar",
        "dbtable" -> "test_table",
        "url" -> "jdbc:postgresql://foo/bar")

    val hadoopConfiguration = new Configuration()
    hadoopConfiguration.set("fs.s3.awsAccessKeyId", "keyId2")
    hadoopConfiguration.set("fs.s3.awsSecretAccessKey", "secretKey2")

    val mergedParams = Parameters.mergeParameters(params)

    mergedParams.tempPathForRedshift(hadoopConfiguration) should fullyMatch
      "'s3://foo/bar/[0-9a-f\\-]+/' CREDENTIALS 'aws_access_key_id=keyId1;aws_secret_access_key=secretKey1'"
  }

  test("invalid options with both dbtable and query specified") {
    val params =
      Map(
        "tempdir" -> "s3://keyId1:secretKey1@foo/bar",
        "dbtable" -> "test_table",
        "query" -> "select * from test_table",
        "url" -> "jdbc:postgresql://foo/bar")

    intercept[Exception] {
      Parameters.mergeParameters(params)
    }
  }
}
