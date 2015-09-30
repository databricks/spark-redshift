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

import com.amazonaws.auth.{AWSCredentials, BasicSessionCredentials}

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

    "overwrite" -> "false",
    "diststyle" -> "EVEN",
    "usestagingtable" -> "true",
    "postactions" -> ";"
  )

  /**
   * Merge user parameters with the defaults, preferring user parameters if specified
   */
  def mergeParameters(userParameters: Map[String, String]): MergedParameters = {
    if (!userParameters.contains("tempdir")) {
      throw new IllegalArgumentException("'tempdir' is required for all Redshift loads and saves")
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
    def rootTempDir: String = parameters("tempdir")

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
     * If true, when writing, replace any existing data. When false, append to the table instead.
     * Note that the table schema will need to be compatible with whatever you have in the DataFrame
     * you're writing. spark-redshift makes no attempt to enforce that - you'll just see Redshift
     * errors if they don't match.
     *
     * Defaults to false.
     */
    @deprecated("Use SaveMode instead", "0.5.0")
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
     * Temporary AWS credentials which are passed to Redshift. These only need to be supplied by
     * the user when Hadoop is configured to authenticate to S3 via IAM roles assigned to EC2
     * instances.
     */
    def temporaryAWSCredentials: Option[AWSCredentials] = {
      for (
        accessKey <- parameters.get("temporary_aws_access_key_id");
        secretAccessKey <- parameters.get("temporary_aws_secret_access_key");
        sessionToken <- parameters.get("temporary_aws_session_token")
      ) yield new BasicSessionCredentials(accessKey, secretAccessKey, sessionToken)
    }
  }
}
