package com.databricks.spark.redshift

/**
 * All user-specifiable parameters for spark-redshift, along with their validation rules and defaults
 */
private [redshift] object Parameters {

  val DEFAULT_PARAMETERS = Map(
    // Notes:
    // * tempdir, redshifttable and jdbcurl have no default and they *must* be provided
    // * sortkeyspec has no default, but is optional
    // * distkey has no default, but is optional unless using diststyle KEY

    "jdbcdriver" -> "org.postgresql.Driver",
    "overwrite" -> "false",
    "diststyle" -> "EVEN"
  )

  /**
   * Merge user parameters with the defaults, preferring user parameters if specified
   */
  def mergeParameters(userParameters: Map[String, String]) : MergedParameters =
    MergedParameters(DEFAULT_PARAMETERS ++ userParameters)

  /**
   * Adds validators and accessors to string map
   */
  case class MergedParameters(parameters: Map[String, String]) {

    private def tempDir = {
      parameters.getOrElse("tempdir", sys.error("'tempdir' is required for all Redshift loads and saves"))
    }

    lazy val tempPath = Utils.makeTempPath(tempDir)

    def table = {
      parameters.getOrElse("redshifttable", sys.error("You must specify a Redshift table name with 'redshifttable' parameter"))
    }

    def jdbcUrl = {
      parameters.getOrElse("jdbcurl", sys.error("A JDBC URL must be provided with 'jdbcurl' parameter"))
    }

    def jdbcDriver = parameters("jdbcdriver")

    def overwrite = parameters("overwrite").toBoolean

    def distStyle = parameters.get("diststyle")

    def distKey = parameters.get("distkey")

    def sortKeySpec = parameters.get("sortkeyspec")
  }
}
