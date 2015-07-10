package com.databricks.spark.redshift

import java.net.URI
import java.nio.file.Paths

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
  def joinUrls(a: String, b: String): String= {
    val aUri = new URI(a)
    new URI(aUri.getScheme, aUri.getHost, Paths.get(aUri.getPath, b).toString, null).toString + "/"
  }

}
