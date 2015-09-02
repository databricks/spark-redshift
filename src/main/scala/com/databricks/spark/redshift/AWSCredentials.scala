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

import com.amazonaws.auth.InstanceProfileCredentialsProvider
import org.apache.hadoop.conf.Configuration

/**
 * Helper class for working with AWS Credentials.
 */
private[redshift] case class AWSCredentials(
    accessKey: String,
    secretAccessKey: String,
    sessionToken: Option[String]) {

  /**
   * Generates a credentials string for Redshift.
   */
  def credentialsString: String = {
    val credentials = s"aws_access_key_id=$accessKey;aws_secret_access_key=$secretAccessKey"
    credentials + sessionToken.map { token => s";token=$token" }.getOrElse("")
  }

}

object AWSCredentials {

  def load(tempPath: String, hadoopConfiguration: Configuration): AWSCredentials = {
    // A good reference on Hadoop's configuration loading / precedence is
    // https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/site/markdown/tools/hadoop-aws/index.md
    val uri = new URI(tempPath)
    uri.getScheme match {
      case "s3" | "s3n" =>
        val creds = new S3Credentials()
        creds.initialize(uri, hadoopConfiguration)
        AWSCredentials(creds.getAccessKey, creds.getSecretAccessKey, sessionToken = None)
      case "s3a" =>
        // This matches what S3A does, with one exception: we don't support anonymous credentials.
        // First, try to parse from URI:
        Option(uri.getUserInfo).flatMap { userInfo =>
          if (userInfo.contains(":")) {
            val (accessKey, secretKey) = userInfo.splitAt(userInfo.indexOf(":"))
            Some(AWSCredentials(accessKey, secretKey, sessionToken = None))
          } else {
            None
          }
        }.orElse {
          // Next, try to read from configuration
          val accessKey = hadoopConfiguration.get("fs.s3a.access.key", null)
          val secretKey = hadoopConfiguration.get("fs.s3a.secret.key", null)
          if (accessKey != null && secretKey != null) {
            Some(AWSCredentials(accessKey, secretKey, sessionToken = None))
          } else {
            None
          }
        }.getOrElse {
          // Finally, fall back on the instance profile provider
          val creds = new InstanceProfileCredentialsProvider().getCredentials
          AWSCredentials(creds.getAWSAccessKeyId, creds.getAWSSecretKey, sessionToken = None)
        }
      case other =>
        throw new IllegalArgumentException("Unrecognized scheme $other; expected s3, s3n, or s3a")
    }
  }
}
