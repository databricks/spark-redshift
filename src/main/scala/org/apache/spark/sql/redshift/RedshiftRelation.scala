package org.apache.spark.sql.redshift

import java.util.Properties

import com.amazonaws.auth.{AWSCredentials, DefaultAWSCredentialsProviderChain}
import com.databricks.spark.redshift.RedshiftInputFormat
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.jdbc.{DriverRegistry, JDBCRDD}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.{Row, SQLContext}

case class RedshiftRelation(table: String,
                            jdbcUrl: String,
                            tempPath: String)
                           (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Logging {

  val POSTGRES_DRIVER_CLASS = "org.postgresql.Driver"

  override def schema = {
    DriverRegistry.register(POSTGRES_DRIVER_CLASS)
    JDBCRDD.resolveTable(jdbcUrl, table, new Properties())
  }

  val getConnection = JDBCRDD.getConnector(POSTGRES_DRIVER_CLASS, jdbcUrl, new Properties())

  override def buildScan(): RDD[Row] = {
    val awsCredentials = credentials()
    unloadToTemp(awsCredentials)
    makeRdd()
  }

  def unloadToTemp(creds: AWSCredentials): Unit = {
    val conn = getConnection()
    val statement = conn.prepareStatement(unloadStmnt(creds))
    statement.execute()
  }

  def unloadStmnt(creds: AWSCredentials) : String = {
    val accessKeyId = creds.getAWSAccessKeyId
    val secretAccessKey = creds.getAWSSecretKey
    val credsString = s"aws_access_key_id=$accessKeyId;aws_secret_access_key=$secretAccessKey"
    val query = s"SELECT * FROM $table"

    s"UNLOAD ('$query') TO '$tempPath' WITH CREDENTIALS '$credsString' ESCAPE ALLOWOVERWRITE"
  }

  def credentials() = (new DefaultAWSCredentialsProviderChain).getCredentials

  def makeRdd(): RDD[Row] = {
    val sc = sqlContext.sparkContext
    val rdd = sc.newAPIHadoopFile(tempPath.replace("s3://", "s3n://"), classOf[RedshiftInputFormat],
      classOf[java.lang.Long], classOf[Array[String]], sc.hadoopConfiguration)
    rdd.values.map(_.map(f => if (f.isEmpty) null else f)).map(x => Row(x: _*))
  }
}
