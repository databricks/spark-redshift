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

import com.amazonaws.auth.{BasicAWSCredentials, AWSCredentials, AWSSessionCredentials, InstanceProfileCredentialsProvider}
import org.apache.hadoop.conf.Configuration

import com.databricks.spark.redshift.Parameters.MergedParameters

private[redshift] object AWSCredentialsUtils {

  /**
    * Generates a credentials string for use in Redshift COPY and UNLOAD statements.
    * Favors a configured `aws_iam_role` if available in the parameters.
    */
  def getRedshiftCredentialsString(params: MergedParameters,
                                   awsCredentials: AWSCredentials): String = {
    params.iamRole
      .map { role => s"aws_iam_role=$role" }
      .getOrElse(
        awsCredentials match {
          case creds: AWSSessionCredentials =>
            s"aws_access_key_id=${creds.getAWSAccessKeyId};" +
              s"aws_secret_access_key=${creds.getAWSSecretKey};token=${creds.getSessionToken}"
          case creds =>
            s"aws_access_key_id=${creds.getAWSAccessKeyId};" +
              s"aws_secret_access_key=${creds.getAWSSecretKey}"
        })
  }

  def load(params: MergedParameters, hadoopConfiguration: Configuration): AWSCredentials = {
    params.temporaryAWSCredentials.getOrElse(loadFromURI(params.rootTempDir, hadoopConfiguration))
  }

  private def loadFromURI(tempPath: String, hadoopConfiguration: Configuration): AWSCredentials = {
    // scalastyle:off
    // A good reference on Hadoop's configuration loading / precedence is
    // https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/site/markdown/tools/hadoop-aws/index.md
    // scalastyle:on
    val uri = new URI(tempPath)
    uri.getScheme match {
      case "s3" | "s3n" =>
        val creds = new S3Credentials()
        creds.initialize(uri, hadoopConfiguration)
        new BasicAWSCredentials(creds.getAccessKey, creds.getSecretAccessKey)
      case "s3a" =>
        // This matches what S3A does, with one exception: we don't support anonymous credentials.
        // First, try to parse from URI:
        Option(uri.getUserInfo).flatMap { userInfo =>
          if (userInfo.contains(":")) {
            val Array(accessKey, secretKey) = userInfo.split(":")
            Some(new BasicAWSCredentials(accessKey, secretKey))
          } else {
            None
          }
        }.orElse {
          // Next, try to read from configuration
          val accessKey = hadoopConfiguration.get("fs.s3a.access.key", null)
          val secretKey = hadoopConfiguration.get("fs.s3a.secret.key", null)
          if (accessKey != null && secretKey != null) {
            Some(new BasicAWSCredentials(accessKey, secretKey))
          } else {
            None
          }
        }.getOrElse {
          // Finally, fall back on the instance profile provider
         new InstanceProfileCredentialsProvider().getCredentials
        }
      case other =>
        throw new IllegalArgumentException(s"Unrecognized scheme $other; expected s3, s3n, or s3a")
    }
  }
}
