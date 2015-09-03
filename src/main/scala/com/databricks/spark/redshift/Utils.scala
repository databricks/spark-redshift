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

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3URI, AmazonS3Client}
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration
import org.apache.spark.Logging

/**
 * Various arbitrary helper functions
 */
private[redshift] object Utils extends Logging {
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
  def fixS3Url(url: String): String = {
    url.replaceAll("s3[an]://", "s3://")
  }

  /**
   * Creates a randomly named temp directory path for intermediate data
   */
  def makeTempPath(tempRoot: String): String = Utils.joinUrls(tempRoot, UUID.randomUUID().toString)

  /**
   * Checks whether the S3 bucket for the given UI has an object lifecycle configuration to
   * ensure cleanup of temporary files. If no applicable configuration is found, this method logs
   * a helpful warning for the user.
   */
  def checkThatBucketHasObjectLifecycleConfiguration(
      tempDir: String,
      awsCredentials: AWSCredentials): Unit = {
    try {
      // TODO: really, we should be accepting an S3 client in the constructor so that we can
      // mock out S3 in a unit test.
      val s3Client = new AmazonS3Client(awsCredentials)
      val s3URI = new AmazonS3URI(Utils.fixS3Url(tempDir))
      val bucket = s3URI.getBucket
      val bucketLifecycleConfiguration = s3Client.getBucketLifecycleConfiguration(bucket)
      val key = s3URI.getKey
      val someRuleMatchesTempDir = bucketLifecycleConfiguration.getRules.asScala.exists { rule =>
        // Note: this only checks that there is an active rule which matches the temp directory;
        // it does not actually check that the rule will delete the files. This check is still
        // better than nothing, though, and we can always improve it later.
        rule.getStatus == BucketLifecycleConfiguration.ENABLED && key.startsWith(rule.getPrefix)
      }
      if (!someRuleMatchesTempDir) {
        logWarning(s"The S3 bucket $bucket does not have an object lifecycle configuration to " +
          "ensure cleanup of temporary files. Consider configuring `tempdir` to point to a " +
          "bucket with an object lifecycle policy that automatically deletes files after an " +
          "expiration period. For more information, see " +
          "https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html")
      }
    } catch {
      case NonFatal(e) =>
        logWarning(
          "An error occurred while trying to read the S3 bucket lifecycle configuration", e)
    }
  }
}
