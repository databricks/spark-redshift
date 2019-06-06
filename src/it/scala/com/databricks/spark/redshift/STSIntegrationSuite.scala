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

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials, STSAssumeRoleSessionCredentialsProvider}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.securitytoken.{AWSSecurityTokenServiceClient, AWSSecurityTokenServiceClientBuilder, model}
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Integration tests for accessing S3 using Amazon Security Token Service (STS) credentials.
 */
class STSIntegrationSuite extends IntegrationSuiteBase {

  private val STS_ROLE_ARN: String = loadConfigFromEnv("STS_ROLE_ARN")
  private var STS_ACCESS_KEY_ID: String = _
  private var STS_SECRET_ACCESS_KEY: String = _
  private var STS_SESSION_TOKEN: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val awsCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

    val stsClient = AWSSecurityTokenServiceClientBuilder
      .standard()
      .withRegion("us-east-1")
      .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
      .build()

    val roleRequest = new AssumeRoleRequest()
      .withDurationSeconds(900)
      .withRoleArn(STS_ROLE_ARN)
      .withRoleSessionName(s"spark-$randomSuffix")

    val creds = stsClient.assumeRole(roleRequest).getCredentials

//    val stsClient = new AWSSecurityTokenServiceClient(awsCredentials)
//    val assumeRoleRequest = new AssumeRoleRequest()
//    assumeRoleRequest.setDurationSeconds(900) // this is the minimum supported duration
//    assumeRoleRequest.setRoleArn(STS_ROLE_ARN)
//    assumeRoleRequest.setRoleSessionName(s"spark-$randomSuffix")
//    val creds = stsClient.assumeRole(assumeRoleRequest).getCredentials
    STS_ACCESS_KEY_ID = creds.getAccessKeyId
    STS_SECRET_ACCESS_KEY = creds.getSecretAccessKey
    STS_SESSION_TOKEN = creds.getSessionToken
  }

  // TODO (luca|COREML-822) Fix STS Authentication test
  ignore("roundtrip save and load") {
    val tableName = s"roundtrip_save_and_load$randomSuffix"
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
      StructType(StructField("a", IntegerType) :: Nil))
    try {
      write(df)
        .option("dbtable", tableName)
        .option("forward_spark_s3_credentials", "false")
        .option("temporary_aws_access_key_id", STS_ACCESS_KEY_ID)
        .option("temporary_aws_secret_access_key", STS_SECRET_ACCESS_KEY)
        .option("temporary_aws_session_token", STS_SESSION_TOKEN)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = read
        .option("dbtable", tableName)
        .option("forward_spark_s3_credentials", "false")
        .option("temporary_aws_access_key_id", STS_ACCESS_KEY_ID)
        .option("temporary_aws_secret_access_key", STS_SECRET_ACCESS_KEY)
        .option("temporary_aws_session_token", STS_SESSION_TOKEN)
        .load()
      assert(loadedDf.schema.length === 1)
      assert(loadedDf.columns === Seq("a"))
      checkAnswer(loadedDf, Seq(Row(1)))
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
    }
  }
}
