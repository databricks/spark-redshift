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

import org.scalatest.{FunSuite, Matchers}

/**
 * Check validation of parameter config
 */
class ParametersSuite extends FunSuite with Matchers {

  test("Minimal valid parameter map is accepted") {
    val params = Map(
      "tempdir" -> "s3://foo/bar",
      "dbtable" -> "test_schema.test_table",
      "url" -> "jdbc:redshift://foo/bar?user=user&password=password",
      "forward_spark_s3_credentials" -> "true")

    val mergedParams = Parameters.mergeParameters(params)

    mergedParams.rootTempDir should startWith (params("tempdir"))
    mergedParams.createPerQueryTempDir() should startWith (params("tempdir"))
    mergedParams.jdbcUrl shouldBe params("url")
    mergedParams.table shouldBe Some(TableName("test_schema", "test_table"))
    assert(mergedParams.forwardSparkS3Credentials)

    // Check that the defaults have been added
    (Parameters.DEFAULT_PARAMETERS - "forward_spark_s3_credentials").foreach {
      case (key, value) => mergedParams.parameters(key) shouldBe value
    }
  }

  test("createPerQueryTempDir() returns distinct temp paths") {
    val params = Map(
      "forward_spark_s3_credentials" -> "true",
      "tempdir" -> "s3://foo/bar",
      "dbtable" -> "test_table",
      "url" -> "jdbc:redshift://foo/bar?user=user&password=password")

    val mergedParams = Parameters.mergeParameters(params)

    mergedParams.createPerQueryTempDir() should not equal mergedParams.createPerQueryTempDir()
  }

  test("Errors are thrown when mandatory parameters are not provided") {
    def checkMerge(params: Map[String, String], err: String): Unit = {
      val e = intercept[IllegalArgumentException] {
        Parameters.mergeParameters(params)
      }
      assert(e.getMessage.contains(err))
    }
    val testURL = "jdbc:redshift://foo/bar?user=user&password=password"
    checkMerge(Map("dbtable" -> "test_table", "url" -> testURL), "tempdir")
    checkMerge(Map("tempdir" -> "s3://foo/bar", "url" -> testURL), "Redshift table name")
    checkMerge(Map("dbtable" -> "test_table", "tempdir" -> "s3://foo/bar"), "JDBC URL")
    checkMerge(Map("dbtable" -> "test_table", "tempdir" -> "s3://foo/bar", "url" -> testURL),
      "method for authenticating")
  }

  test("Must specify either 'dbtable' or 'query' parameter, but not both") {
    intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "forward_spark_s3_credentials" -> "true",
        "tempdir" -> "s3://foo/bar",
        "url" -> "jdbc:redshift://foo/bar?user=user&password=password"))
    }.getMessage should (include ("dbtable") and include ("query"))

    intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "forward_spark_s3_credentials" -> "true",
        "tempdir" -> "s3://foo/bar",
        "dbtable" -> "test_table",
        "query" -> "select * from test_table",
        "url" -> "jdbc:redshift://foo/bar?user=user&password=password"))
    }.getMessage should (include ("dbtable") and include ("query") and include("both"))

    Parameters.mergeParameters(Map(
      "forward_spark_s3_credentials" -> "true",
      "tempdir" -> "s3://foo/bar",
      "query" -> "select * from test_table",
      "url" -> "jdbc:redshift://foo/bar?user=user&password=password"))
  }

  test("Must specify credentials in either URL or 'user' and 'password' parameters, but not both") {
    intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "forward_spark_s3_credentials" -> "true",
        "tempdir" -> "s3://foo/bar",
        "query" -> "select * from test_table",
        "url" -> "jdbc:redshift://foo/bar"))
    }.getMessage should (include ("credentials"))

    intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "forward_spark_s3_credentials" -> "true",
        "tempdir" -> "s3://foo/bar",
        "query" -> "select * from test_table",
        "user" -> "user",
        "password" -> "password",
        "url" -> "jdbc:redshift://foo/bar?user=user&password=password"))
    }.getMessage should (include ("credentials") and include("both"))

    Parameters.mergeParameters(Map(
      "forward_spark_s3_credentials" -> "true",
      "tempdir" -> "s3://foo/bar",
      "query" -> "select * from test_table",
      "url" -> "jdbc:redshift://foo/bar?user=user&password=password"))
  }

  test("tempformat option is case-insensitive") {
    val params = Map(
      "forward_spark_s3_credentials" -> "true",
      "tempdir" -> "s3://foo/bar",
      "dbtable" -> "test_schema.test_table",
      "url" -> "jdbc:redshift://foo/bar?user=user&password=password")

    Parameters.mergeParameters(params + ("tempformat" -> "csv"))
    Parameters.mergeParameters(params + ("tempformat" -> "CSV"))

    intercept[IllegalArgumentException] {
      Parameters.mergeParameters(params + ("tempformat" -> "invalid-temp-format"))
    }
  }

  test("can only specify one Redshift to S3 authentication mechanism") {
    val e = intercept[IllegalArgumentException] {
      Parameters.mergeParameters(Map(
        "tempdir" -> "s3://foo/bar",
        "dbtable" -> "test_schema.test_table",
        "url" -> "jdbc:redshift://foo/bar?user=user&password=password",
        "forward_spark_s3_credentials" -> "true",
        "aws_iam_role" -> "role"))
    }
    assert(e.getMessage.contains("mutually-exclusive"))
  }
}
