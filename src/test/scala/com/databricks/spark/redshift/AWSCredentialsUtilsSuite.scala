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

import com.amazonaws.AmazonClientException
import com.amazonaws.auth.{AWSSessionCredentials, BasicSessionCredentials, BasicAWSCredentials}
import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import com.databricks.spark.redshift.Parameters.MergedParameters

class AWSCredentialsUtilsSuite extends FunSuite {

  private implicit def string2Params(tempdir: String): MergedParameters = {
    MergedParameters(Map("tempdir" -> tempdir))
  }

  test("credentialsString with regular keys") {
    val creds = new BasicAWSCredentials("ACCESSKEYID", "SECRET/KEY/WITH/SLASHES")
    val params = MergedParameters(Map.empty)
    assert(AWSCredentialsUtils.getRedshiftCredentialsString(params, creds) ===
      "aws_access_key_id=ACCESSKEYID;aws_secret_access_key=SECRET/KEY/WITH/SLASHES")
  }

  test("credentialsString with STS temporary keys") {
    val creds = new BasicSessionCredentials("ACCESSKEYID", "SECRET/KEY", "SESSION/Token")
    val params = MergedParameters(Map.empty)
    assert(AWSCredentialsUtils.getRedshiftCredentialsString(params, creds) ===
      "aws_access_key_id=ACCESSKEYID;aws_secret_access_key=SECRET/KEY;token=SESSION/Token")
  }

  test("Configured IAM roles should take precedence") {
    val creds = new BasicSessionCredentials("ACCESSKEYID", "SECRET/KEY", "SESSION/Token")
    val iamRole = "arn:aws:iam::123456789000:role/redshift_iam_role"
    val params = MergedParameters(Map("aws_iam_role" -> iamRole))
    assert(AWSCredentialsUtils.getRedshiftCredentialsString(params, creds) ===
      s"aws_iam_role=$iamRole")
  }

  test("AWSCredentials.load() STS temporary keys should take precedence") {
    val conf = new Configuration(false)
    conf.set("fs.s3.awsAccessKeyId", "CONFID")
    conf.set("fs.s3.awsSecretAccessKey", "CONFKEY")

    val params = MergedParameters(Map(
      "tempdir" -> "s3://URIID:URIKEY@bucket/path",
      "temporary_aws_access_key_id" -> "key_id",
      "temporary_aws_secret_access_key" -> "secret",
      "temporary_aws_session_token" -> "token"
    ))

    val creds = AWSCredentialsUtils.load(params, conf)
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
      val creds = AWSCredentialsUtils.load("s3://URIID:URIKEY@bucket/path", conf)
      assert(creds.getAWSAccessKeyId === "URIID")
      assert(creds.getAWSSecretKey === "URIKEY")
    }

    {
      val creds = AWSCredentialsUtils.load("s3://bucket/path", conf)
      assert(creds.getAWSAccessKeyId === "CONFID")
      assert(creds.getAWSSecretKey === "CONFKEY")
    }

    // The s3:// protocol does not work with EC2 IAM instance profiles.
    val e = intercept[IllegalArgumentException] {
      AWSCredentialsUtils.load("s3://bucket/path", new Configuration(false))
    }
    assert(e.getMessage.contains("Key must be specified"))
  }

  test("AWSCredentials.load() credentials precedence for s3n:// URIs") {
    val conf = new Configuration(false)
    conf.set("fs.s3n.awsAccessKeyId", "CONFID")
    conf.set("fs.s3n.awsSecretAccessKey", "CONFKEY")

    {
      val creds = AWSCredentialsUtils.load("s3n://URIID:URIKEY@bucket/path", conf)
      assert(creds.getAWSAccessKeyId === "URIID")
      assert(creds.getAWSSecretKey === "URIKEY")
    }

    {
      val creds = AWSCredentialsUtils.load("s3n://bucket/path", conf)
      assert(creds.getAWSAccessKeyId === "CONFID")
      assert(creds.getAWSSecretKey === "CONFKEY")
    }

    // The s3n:// protocol does not work with EC2 IAM instance profiles.
    val e = intercept[IllegalArgumentException] {
      AWSCredentialsUtils.load("s3n://bucket/path", new Configuration(false))
    }
    assert(e.getMessage.contains("Key must be specified"))
  }

  test("AWSCredentials.load() credentials precedence for s3a:// URIs") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "CONFID")
    conf.set("fs.s3a.secret.key", "CONFKEY")

    {
      val creds = AWSCredentialsUtils.load("s3a://URIID:URIKEY@bucket/path", conf)
      assert(creds.getAWSAccessKeyId === "URIID")
      assert(creds.getAWSSecretKey === "URIKEY")
    }

    {
      val creds = AWSCredentialsUtils.load("s3a://bucket/path", conf)
      assert(creds.getAWSAccessKeyId === "CONFID")
      assert(creds.getAWSSecretKey === "CONFKEY")
    }

    // The s3a:// protocol supports loading of credentials from EC2 IAM instance profiles, but
    // our Travis integration tests will not be able to provide these credentials. In the meantime,
    // just check that this test fails because the AWS client fails to obtain those credentials.
    // TODO: refactor and mock to enable proper tests here.
    val e = intercept[AmazonClientException] {
      AWSCredentialsUtils.load("s3a://bucket/path", new Configuration(false))
    }
    assert(e.getMessage === "Unable to load credentials from Amazon EC2 metadata service" ||
      e.getMessage.contains("The requested metadata is not found at"))
  }
}
