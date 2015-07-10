package com.databricks.spark.redshift

import java.net.URI
import java.nio.file.Paths

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

object Utils {

  def credentials() = (new DefaultAWSCredentialsProviderChain).getCredentials

  def credentialsString() = {
    val awsCredentials = credentials()
    val accessKeyId = awsCredentials.getAWSAccessKeyId
    val secretAccessKey = awsCredentials.getAWSSecretKey

    s"aws_access_key_id=$accessKeyId;aws_secret_access_key=$secretAccessKey"
  }

  def joinUrls(a: String, b: String): String= {
    val aUri = new URI(a)
    new URI(aUri.getScheme, aUri.getHost, Paths.get(aUri.getPath, b).toString, null).toString + "/"
  }

}
