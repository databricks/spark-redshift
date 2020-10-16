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

package io.github.spark_redshift_community.spark.redshift

import com.amazonaws.auth.{AWSSessionCredentials, BasicAWSCredentials, BasicSessionCredentials}
import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import scala.language.implicitConversions

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
    val creds = new BasicAWSCredentials("ACCESSKEYID", "SECRET/KEY/WITH/SLASHES")
    val params =
      Parameters.mergeParameters(baseParams ++ Map("forward_spark_s3_credentials" -> "true"))
    assert(AWSCredentialsUtils.getRedshiftCredentialsString(params, creds) ===
      "aws_access_key_id=ACCESSKEYID;aws_secret_access_key=SECRET/KEY/WITH/SLASHES")
  }

  test("credentialsString with STS temporary keys") {
    val params = Parameters.mergeParameters(baseParams ++ Map(
      "temporary_aws_access_key_id" -> "ACCESSKEYID",
      "temporary_aws_secret_access_key" -> "SECRET/KEY",
      "temporary_aws_session_token" -> "SESSION/Token"))
    assert(AWSCredentialsUtils.getRedshiftCredentialsString(params, null) ===
      "aws_access_key_id=ACCESSKEYID;aws_secret_access_key=SECRET/KEY;token=SESSION/Token")
  }

  test("Configured IAM roles should take precedence") {
    val creds = new BasicSessionCredentials("ACCESSKEYID", "SECRET/KEY", "SESSION/Token")
    val iamRole = "arn:aws:iam::123456789000:role/redshift_iam_role"
    val params = Parameters.mergeParameters(baseParams ++ Map("aws_iam_role" -> iamRole))
    assert(AWSCredentialsUtils.getRedshiftCredentialsString(params, null) ===
      s"aws_iam_role=$iamRole")
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
