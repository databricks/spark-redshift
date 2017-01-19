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

import scala.language.implicitConversions

import com.amazonaws.auth.{AWSSessionCredentials, BasicSessionCredentials, BasicAWSCredentials}
import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import com.databricks.spark.redshift.Parameters.MergedParameters

class AWSCredentialsUtilsSuite extends FunSuite {

  val baseParams = Map(
    "tempdir" -> "s3://foo/bar",
    "dbtable" -> "test_schema.test_table",
    "url" -> "jdbc:redshift://foo/bar?user=user&password=password")

  private implicit def string2Params(tempdir: String): MergedParameters = {
    Parameters.mergeParameters(baseParams ++ Map(
      "tempdir" -> tempdir,
      "forward_spark_s3_credentials" -> "true"))
  }

  test("credentialsString with regular keys") {
    val hadoopConfiguration: Configuration = new Configuration()
    val creds = new BasicAWSCredentials("ACCESSKEYID", "SECRET/KEY/WITH/SLASHES")
    val params =
      Parameters.mergeParameters(baseParams ++ Map("forward_spark_s3_credentials" -> "true"))
    assert(AWSCredentialsUtils.getRedshiftCredentialsString(params, creds, hadoopConfiguration) ===
      "aws_access_key_id=ACCESSKEYID;aws_secret_access_key=SECRET/KEY/WITH/SLASHES")
  }

  test("credentialsString with regular keys and encryption") {
    val hadoopConfiguration: Configuration = new Configuration()
    hadoopConfiguration.set("spark-redshift.master-sym-key", "test-master-sym-key")
    val creds = new BasicAWSCredentials("ACCESSKEYID", "SECRET/KEY/WITH/SLASHES")
    val params =
      Parameters.mergeParameters(
        baseParams ++ Map("forward_spark_s3_credentials" -> "true", "encryption" -> "true")
      )
    assert(AWSCredentialsUtils.getRedshiftCredentialsString(params, creds, hadoopConfiguration) ===
      "aws_access_key_id=ACCESSKEYID;" +
      "aws_secret_access_key=SECRET/KEY/WITH/SLASHES;" +
      "master_symmetric_key=test-master-sym-key")
  }

  test("credentialsString with STS temporary keys") {
    val hadoopConfiguration: Configuration = new Configuration()
    val params = Parameters.mergeParameters(baseParams ++ Map(
      "temporary_aws_access_key_id" -> "ACCESSKEYID",
      "temporary_aws_secret_access_key" -> "SECRET/KEY",
      "temporary_aws_session_token" -> "SESSION/Token"))
    assert(AWSCredentialsUtils.getRedshiftCredentialsString(params, null, hadoopConfiguration) ===
      "aws_access_key_id=ACCESSKEYID;aws_secret_access_key=SECRET/KEY;token=SESSION/Token")
  }

  test("credentialsString with STS temporary keys and encryption") {
    val hadoopConfiguration: Configuration = new Configuration()
    hadoopConfiguration.set("spark-redshift.master-sym-key", "test-master-sym-key")
    val params = Parameters.mergeParameters(baseParams ++ Map(
      "temporary_aws_access_key_id" -> "ACCESSKEYID",
      "temporary_aws_secret_access_key" -> "SECRET/KEY",
      "temporary_aws_session_token" -> "SESSION/Token",
      "encryption" -> "true"))
    assert(AWSCredentialsUtils.getRedshiftCredentialsString(params, null, hadoopConfiguration) ===
        "aws_access_key_id=ACCESSKEYID;" +
        "aws_secret_access_key=SECRET/KEY;" +
        "token=SESSION/Token;" +
        "master_symmetric_key=test-master-sym-key")
  }

  test("Configured IAM roles should take precedence") {
    val hadoopConfiguration: Configuration = new Configuration()
    val creds = new BasicSessionCredentials("ACCESSKEYID", "SECRET/KEY", "SESSION/Token")
    val iamRole = "arn:aws:iam::123456789000:role/redshift_iam_role"
    val params = Parameters.mergeParameters(baseParams ++ Map("aws_iam_role" -> iamRole))
    assert(AWSCredentialsUtils.getRedshiftCredentialsString(params, null, hadoopConfiguration) ===
      s"aws_iam_role=$iamRole")
  }

  test("Configured IAM roles should take precedence with encryption") {
    val hadoopConfiguration: Configuration = new Configuration()
    hadoopConfiguration.set("spark-redshift.master-sym-key", "test-master-sym-key")
    val creds = new BasicSessionCredentials("ACCESSKEYID", "SECRET/KEY", "SESSION/Token")
    val iamRole = "arn:aws:iam::123456789000:role/redshift_iam_role"
    val params = Parameters.mergeParameters(baseParams ++ Map(
      "aws_iam_role" -> iamRole,
      "encryption" -> "true"
    ))
    assert(AWSCredentialsUtils.getRedshiftCredentialsString(params, null, hadoopConfiguration) ===
        s"aws_iam_role=$iamRole;master_symmetric_key=test-master-sym-key")
  }

  test("AWSCredentials.load() STS temporary keys should take precedence") {
    val conf = new Configuration(false)
    conf.set("fs.s3.awsAccessKeyId", "CONFID")
    conf.set("fs.s3.awsSecretAccessKey", "CONFKEY")

    val params = Parameters.mergeParameters(baseParams ++ Map(
      "tempdir" -> "s3://URIID:URIKEY@bucket/path",
      "temporary_aws_access_key_id" -> "key_id",
      "temporary_aws_secret_access_key" -> "secret",
      "temporary_aws_session_token" -> "token"
    ))

    val creds = AWSCredentialsUtils.load(params, conf).getCredentials
    assert(creds.isInstanceOf[AWSSessionCredentials])
    assert(creds.getAWSAccessKeyId === "key_id")
    assert(creds.getAWSSecretKey === "secret")
    assert(creds.asInstanceOf[AWSSessionCredentials].getSessionToken === "token")
  }

  test("AWSCredentials.load() credentials precedence for s3:// URIs") {
    val conf = new Configuration(false)
    conf.set("fs.s3.awsAccessKeyId", "CONFID")
    conf.set("fs.s3.awsSecretAccessKey", "CONFKEY")

    {
      val creds = AWSCredentialsUtils.load("s3://URIID:URIKEY@bucket/path", conf).getCredentials
      assert(creds.getAWSAccessKeyId === "URIID")
      assert(creds.getAWSSecretKey === "URIKEY")
    }

    {
      val creds = AWSCredentialsUtils.load("s3://bucket/path", conf).getCredentials
      assert(creds.getAWSAccessKeyId === "CONFID")
      assert(creds.getAWSSecretKey === "CONFKEY")
    }

  }

  test("AWSCredentials.load() credentials precedence for s3n:// URIs") {
    val conf = new Configuration(false)
    conf.set("fs.s3n.awsAccessKeyId", "CONFID")
    conf.set("fs.s3n.awsSecretAccessKey", "CONFKEY")

    {
      val creds = AWSCredentialsUtils.load("s3n://URIID:URIKEY@bucket/path", conf).getCredentials
      assert(creds.getAWSAccessKeyId === "URIID")
      assert(creds.getAWSSecretKey === "URIKEY")
    }

    {
      val creds = AWSCredentialsUtils.load("s3n://bucket/path", conf).getCredentials
      assert(creds.getAWSAccessKeyId === "CONFID")
      assert(creds.getAWSSecretKey === "CONFKEY")
    }

  }

  test("AWSCredentials.load() credentials precedence for s3a:// URIs") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "CONFID")
    conf.set("fs.s3a.secret.key", "CONFKEY")

    {
      val creds = AWSCredentialsUtils.load("s3a://URIID:URIKEY@bucket/path", conf).getCredentials
      assert(creds.getAWSAccessKeyId === "URIID")
      assert(creds.getAWSSecretKey === "URIKEY")
    }

    {
      val creds = AWSCredentialsUtils.load("s3a://bucket/path", conf).getCredentials
      assert(creds.getAWSAccessKeyId === "CONFID")
      assert(creds.getAWSSecretKey === "CONFKEY")
    }

  }
}
