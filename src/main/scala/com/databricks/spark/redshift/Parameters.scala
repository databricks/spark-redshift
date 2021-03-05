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

import com.amazonaws.auth.{AWSCredentialsProvider, BasicSessionCredentials}

/**
 * All user-specifiable parameters for spark-redshift, along with their validation rules and
 * defaults.
 */
private[redshift] object Parameters {

  val DEFAULT_PARAMETERS: Map[String, String] = Map(
    // Notes:
    // * tempdir, dbtable and url have no default and they *must* be provided
    // * sortkeyspec has no default, but is optional
    // * distkey has no default, but is optional unless using diststyle KEY
    // * jdbcdriver has no default, but is optional
    // * sse_kms_key has no default, but is optional

    "forward_spark_s3_credentials" -> "false",
    "tempformat" -> "AVRO",
    "csvnullstring" -> "@NULL@",
    "overwrite" -> "false",
    "diststyle" -> "EVEN",
    "usestagingtable" -> "true",
    "preactions" -> ";",
    "postactions" -> ";"
  )

  val VALID_TEMP_FORMATS = Set("AVRO", "CSV", "CSV GZIP")

  /**
   * Merge user parameters with the defaults, preferring user parameters if specified
   */
  def mergeParameters(userParameters: Map[String, String]): MergedParameters = {
    if (!userParameters.contains("tempdir")) {
      throw new IllegalArgumentException("'tempdir' is required for all Redshift loads and saves")
    }
    if (userParameters.contains("tempformat") &&
        !VALID_TEMP_FORMATS.contains(userParameters("tempformat").toUpperCase)) {
      throw new IllegalArgumentException(
        s"""Invalid temp format: ${userParameters("tempformat")}; """ +
          s"valid formats are: ${VALID_TEMP_FORMATS.mkString(", ")}")
    }
    if (!userParameters.contains("url")) {
      throw new IllegalArgumentException("A JDBC URL must be provided with 'url' parameter")
    }
    if (!userParameters.contains("dbtable") && !userParameters.contains("query")) {
      throw new IllegalArgumentException(
        "You must specify a Redshift table name with the 'dbtable' parameter or a query with the " +
        "'query' parameter.")
    }
    if (userParameters.contains("dbtable") && userParameters.contains("query")) {
      throw new IllegalArgumentException(
        "You cannot specify both the 'dbtable' and 'query' parameters at the same time.")
    }
    val credsInURL = userParameters.get("url")
      .filter(url => url.contains("user=") || url.contains("password="))
    if (userParameters.contains("user") || userParameters.contains("password")) {
      if (credsInURL.isDefined) {
        throw new IllegalArgumentException(
          "You cannot specify credentials in both the URL and as user/password options")
        }
    } else if (credsInURL.isEmpty) {
      throw new IllegalArgumentException(
        "You must specify credentials in either the URL or as user/password options")
    }

    MergedParameters(DEFAULT_PARAMETERS ++ userParameters)
  }

  /**
   * Adds validators and accessors to string map
   */
  case class MergedParameters(parameters: Map[String, String]) {

    require(temporaryAWSCredentials.isDefined || iamRole.isDefined || forwardSparkS3Credentials,
      "You must specify a method for authenticating Redshift's connection to S3 (aws_iam_role," +
        " forward_spark_s3_credentials, or temporary_aws_*. For a discussion of the differences" +
        " between these options, please see the README.")

    require(Seq(
        temporaryAWSCredentials.isDefined,
        iamRole.isDefined,
        forwardSparkS3Credentials).count(_ == true) == 1,
      "The aws_iam_role, forward_spark_s3_credentials, and temporary_aws_*. options are " +
        "mutually-exclusive; please specify only one.")

    /**
     * A root directory to be used for intermediate data exchange, expected to be on S3, or
     * somewhere that can be written to and read from by Redshift. Make sure that AWS credentials
     * are available for S3.
     */
    def rootTempDir: String = parameters("tempdir")

    /**
     * The format in which to save temporary files in S3. Defaults to "AVRO"; the other allowed
     * values are "CSV" and "CSV GZIP" for CSV and gzipped CSV, respectively.
     */
    def tempFormat: String = parameters("tempformat").toUpperCase

    /**
     * The String value to write for nulls when using CSV.
     * This should be a value which does not appear in your actual data.
     */
    def nullString: String = parameters("csvnullstring")

    /**
     * Creates a per-query subdirectory in the [[rootTempDir]], with a random UUID.
     */
    def createPerQueryTempDir(): String = Utils.makeTempPath(rootTempDir)

    /**
     * The Redshift table to be used as the target when loading or writing data.
     */
    def table: Option[TableName] = parameters.get("dbtable").map(_.trim).flatMap { dbtable =>
      // We technically allow queries to be passed using `dbtable` as long as they are wrapped
      // in parentheses. Valid SQL identifiers may contain parentheses but cannot begin with them,
      // so there is no ambiguity in ignoring subqeries here and leaving their handling up to
      // the `query` function defined below.
      if (dbtable.startsWith("(") && dbtable.endsWith(")")) {
        None
      } else {
        Some(TableName.parseFromEscaped(dbtable))
      }
    }

    /**
     * The Redshift query to be used as the target when loading data.
     */
    def query: Option[String] = parameters.get("query").orElse {
      parameters.get("dbtable")
        .map(_.trim)
        .filter(t => t.startsWith("(") && t.endsWith(")"))
        .map(t => t.drop(1).dropRight(1))
    }

    /**
    * User and password to be used to authenticate to Redshift
    */
    def credentials: Option[(String, String)] = {
      for (
        user <- parameters.get("user");
        password <- parameters.get("password")
      ) yield (user, password)
    }

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
     * connecting over JDBC.
     */
    def jdbcDriver: Option[String] = parameters.get("jdbcdriver")

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
     * DEPRECATED: see PR #157.
     *
     * When true, data is always loaded into a new temporary table when performing an overwrite.
     * This is to ensure that the whole load process succeeds before dropping any data from
     * Redshift, which can be useful if, in the event of failures, stale data is better than no data
     * for your systems.
     *
     * Defaults to true.
     */
    def useStagingTable: Boolean = parameters("usestagingtable").toBoolean

    /**
     * Extra options to append to the Redshift COPY command (e.g. "MAXERROR 100").
     */
    def extraCopyOptions: String = parameters.get("extracopyoptions").getOrElse("")

    /**
      * Description of the table, set using the SQL COMMENT command.
      */
    def description: Option[String] = parameters.get("description")

    /**
      * List of semi-colon separated SQL statements to run before write operations.
      * This can be useful for running DELETE operations to clean up data
      *
      * If the action string contains %s, the table name will be substituted in, in case a staging
      * table is being used.
      *
      * Defaults to empty.
      */
    def preActions: Array[String] = parameters("preactions").split(";")

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
      * The IAM role that Redshift should assume for COPY/UNLOAD operations.
      */
    def iamRole: Option[String] = parameters.get("aws_iam_role")

    /**
     * If true then this library will automatically discover the credentials that Spark is
     * using to connect to S3 and will forward those credentials to Redshift over JDBC.
     */
    def forwardSparkS3Credentials: Boolean = parameters("forward_spark_s3_credentials").toBoolean

    /**
     * Temporary AWS credentials which are passed to Redshift. These only need to be supplied by
     * the user when Hadoop is configured to authenticate to S3 via IAM roles assigned to EC2
     * instances.
     */
    def temporaryAWSCredentials: Option[AWSCredentialsProvider] = {
      for (
        accessKey <- parameters.get("temporary_aws_access_key_id");
        secretAccessKey <- parameters.get("temporary_aws_secret_access_key");
        sessionToken <- parameters.get("temporary_aws_session_token")
      ) yield {
        AWSCredentialsUtils.staticCredentialsProvider(
          new BasicSessionCredentials(accessKey, secretAccessKey, sessionToken))
      }
    }

    /**
     * The AWS SSE-KMS key to use for encryption during UNLOAD operations instead of AWS's default encryption
     */
    def sseKmsKey: Option[String] = parameters.get("sse_kms_key")
  }
}
