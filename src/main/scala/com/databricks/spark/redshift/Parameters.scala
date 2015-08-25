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

import java.net.URI

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.hadoop.conf.Configuration

import org.apache.spark.Logging

/**
 * All user-specifiable parameters for spark-redshift, along with their validation rules and
 * defaults.
 */
private[redshift] object Parameters extends Logging {

  val DEFAULT_PARAMETERS: Map[String, String] = Map(
    // Notes:
    // * tempdir, dbtable and url have no default and they *must* be provided
    // * sortkeyspec has no default, but is optional
    // * distkey has no default, but is optional unless using diststyle KEY

    "jdbcdriver" -> "com.amazon.redshift.jdbc4.Driver",
    "overwrite" -> "false",
    "diststyle" -> "EVEN",
    "usestagingtable" -> "true",
    "postactions" -> ";",
    "avrocompression" -> ""
  )

  /**
   * Merge user parameters with the defaults, preferring user parameters if specified
   */
  def mergeParameters(userParameters: Map[String, String]): MergedParameters = {
    if (!userParameters.contains("tempdir")) {
      sys.error("'tempdir' is required for all Redshift loads and saves")
    }
    if (!userParameters.contains("dbtable")) {
      sys.error("You must specify a Redshift table name with 'dbtable' parameter")
    }
    if (!userParameters.contains("url")) {
      sys.error("A JDBC URL must be provided with 'url' parameter")
    }

    MergedParameters(DEFAULT_PARAMETERS ++ userParameters)
  }

  /**
   * Adds validators and accessors to string map
   */
  case class MergedParameters(parameters: Map[String, String]) {

    /**
     * A root directory to be used for intermediate data exchange, expected to be on S3, or
     * somewhere that can be written to and read from by Redshift. Make sure that AWS credentials
     * are available for S3.
     */
    private def tempDir: String = parameters("tempdir")

    /**
     * Each instance will create its own subdirectory in the tempDir, with a random UUID.
     */
    val tempPath: String = Utils.makeTempPath(tempDir)

    /**
     * The Redshift table to be used as the target when loading or writing data.
     */
    def table: String = parameters("dbtable")

    /**
     * A JDBC URL, of the format:
     *
     *    jdbc:subprotocol://host:port/database?user=username&password=password
     *
     * Where:
     *  - subprotocol can be postgresql or redshift, depending on which JDBC driver you have loaded.
     *    Note however that one Redshift-compatible driver must be on the classpath and match this
     *    URL.
     *  - host and port should point to the Redshift master node, so security groups and/or VPC will
     *    need to be configured to allow access from the Spark driver
     *  - database identifies a Redshift database name
     *  - user and password are credentials to access the database, which must be embedded in this
     *    URL for JDBC
     */
    def jdbcUrl: String = parameters("url")

    /**
     * The JDBC driver class name. This is used to make sure the driver is registered before
     * connecting over JDBC. Default is "com.amazon.redshift.jdbc4.Driver"
     */
    def jdbcDriver: String = parameters("jdbcdriver")

    /**
     * If true, when writing, replace any existing data. When false, append to the table instead.
     * Note that the table schema will need to be compatible with whatever you have in the DataFrame
     * you're writing. spark-redshift makes no attempt to enforce that - you'll just see Redshift
     * errors if they don't match.
     *
     * Defaults to false.
     */
    def overwrite: Boolean = parameters("overwrite").toBoolean

    /**
     * Set the Redshift table distribution style, which can be one of: EVEN, KEY or ALL. If you set
     * it to KEY, you'll also need to use the distkey parameter to set the distribution key.
     *
     * Default is EVEN.
     */
    def distStyle: Option[String] = parameters.get("diststyle")

    /**
     * The name of a column in the table to use as the distribution key when using DISTSTYLE KEY.
     * Not set by default, as default DISTSTYLE is EVEN.
     */
    def distKey: Option[String] = parameters.get("distkey")

    /**
     * A full Redshift SORTKEY specification. For full information, see latest Redshift docs:
     * http://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html
     *
     * Examples:
     *   SORTKEY (my_sort_column)
     *   COMPOUND SORTKEY (sort_col1, sort_col2)
     *   INTERLEAVED SORTKEY (sort_col1, sort_col2)
     *
     * Not set by default - table will be unsorted.
     *
     * Note: appending data to a table with a sort key only makes sense if you know that the data
     * being added will be after the data already in the table according to the sort order. Redshift
     * does not support random inserts according to sort order, so performance will degrade if you
     * try this.
     */
    def sortKeySpec: Option[String] = parameters.get("sortkeyspec")

    /**
     * When true, data is always loaded into a new temporary table when performing an overwrite.
     * This is to ensure that the whole load process succeeds before dropping any data from
     * Redshift, which can be useful if, in the event of failures, stale data is better than no data
     * for your systems.
     *
     * Defaults to true.
     */
    def useStagingTable: Boolean = parameters("usestagingtable").toBoolean

    /**
     * List of semi-colon separated SQL statements to run after successful write operations.
     * This can be useful for running GRANT operations to make your new tables readable to other
     * users and groups.
     *
     * If the action string contains %s, the table name will be substituted in, in case a staging
     * table is being used.
     *
     * Defaults to empty.
     */
    def postActions: Array[String] = parameters("postactions").split(";")

    /**
     * Looks up "aws_access_key_id" and "aws_secret_access_key" in the parameter map and generates a
     * credentials string for Redshift. If no credentials have been provided, this function will
     * instead try using the Hadoop Configuration `fs.* settings` for the provided tempDir scheme,
     * and if that also fails, it finally tries AWS DefaultCredentialsProviderChain, which makes
     * use of standard system properties, environment variables, or IAM role configuration if
     * available.
     */
    def credentialsString(configuration: Configuration): String = {
      val scheme = new URI(tempDir).getScheme
      val hadoopConfPrefix = s"fs.$scheme"

      val (accessKeyId, secretAccessKey) = {
        if (parameters.contains("aws_access_key_id")) {
          log.info("Using credentials provided in parameter map.")
          (parameters("aws_access_key_id"), parameters("aws_secret_access_key"))
        } else if (configuration.get(s"$hadoopConfPrefix.awsAccessKeyId") != null) {
          log.info(s"Using hadoopConfiguration credentials for scheme $scheme}")
          (configuration.get(s"$hadoopConfPrefix.awsAccessKeyId"),
            configuration.get(s"$hadoopConfPrefix.awsSecretAccessKey"))
        } else {
          try {
            log.info(
              "Using default provider chain for AWS credentials, as none provided explicitly.")
            val awsCredentials = (new DefaultAWSCredentialsProviderChain).getCredentials
            (awsCredentials.getAWSAccessKeyId, awsCredentials.getAWSSecretKey)
          } catch {
            case e: Exception =>
              throw new Exception("No credentials provided and unable to detect automatically.", e)
          }
        }
      }

      val credentials = s"aws_access_key_id=$accessKeyId;aws_secret_access_key=$secretAccessKey"

      if (parameters.contains("aws_security_token")) {
        val securityToken = parameters("aws_security_token")
        credentials + s";token=$securityToken"
      } else {
        credentials
      }
    }

    /**
     * When nonempty/non-null sets the compression codec to use for writing Avro data.
     *
     * Defaults to disabled (i.e. whatever is set in Hadoop config).
     */
    def avrocompression: String = parameters("avrocompression")
  }
}
