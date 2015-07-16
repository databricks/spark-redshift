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

import java.io.File
import java.net.URI
import java.util.UUID

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

/**
 * Various arbitrary helper functions
 */
object Utils {
  /**
   * Gets credentials from AWS default provider chain
   */
  def credentials() = (new DefaultAWSCredentialsProviderChain).getCredentials

  /**
   * Formats credentials as required for Redshift COPY and UNLOAD commands
   */
  def credentialsString() = {
    val awsCredentials = credentials()
    val accessKeyId = awsCredentials.getAWSAccessKeyId
    val secretAccessKey = awsCredentials.getAWSSecretKey

    s"aws_access_key_id=$accessKeyId;aws_secret_access_key=$secretAccessKey"
  }

  /**
   * Joins prefix URL a to path suffix b, and appends a trailing /, in order to create
   * a temp directory path for S3.
   */
  def joinUrls(a: String, b: String): String = {
    val aUri = new URI(a)
    val joinedPath = new File(aUri.getPath, b).toString
    new URI(aUri.getScheme, aUri.getHost, joinedPath, null).toString + "/"
  }

  /**
   * Redshift COPY and UNLOAD commands don't support s3n or s3a, but users may wish to use them
   * for data loads. This function converts the URL back to the s3:// format.
   */
  def fixS3Url(url: String) = {
    url.replaceAll("s3[an]://", "s3://")
  }

  /**
   * Creates a randomly named temp directory path for intermediate data
   */
  def makeTempPath(tempRoot: String) = Utils.joinUrls(tempRoot, UUID.randomUUID().toString)

}
