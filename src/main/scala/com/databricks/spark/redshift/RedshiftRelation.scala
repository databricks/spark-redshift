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

import java.io.InputStreamReader
import java.lang
import java.net.URI

import scala.collection.JavaConverters._

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3URI}
import com.eclipsesource.json.Json
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SQLContext}
import org.slf4j.LoggerFactory

import com.databricks.spark.redshift.Parameters.MergedParameters

/**
 * Data Source API implementation for Amazon Redshift database tables
 */
private[redshift] case class RedshiftRelation(
    jdbcWrapper: JDBCWrapper,
    s3ClientFactory: AWSCredentials => AmazonS3Client,
    params: MergedParameters,
    userSchema: Option[StructType])
    (@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation {

  private val log = LoggerFactory.getLogger(getClass)

  if (sqlContext != null) {
    Utils.assertThatFileSystemIsNotS3BlockFileSystem(
      new URI(params.rootTempDir), sqlContext.sparkContext.hadoopConfiguration)
  }

  private val tableNameOrSubquery =
    params.query.map(q => s"($q)").orElse(params.table.map(_.toString)).get

  override lazy val schema: StructType = {
    userSchema.getOrElse {
      val tableNameOrSubquery =
        params.query.map(q => s"($q)").orElse(params.table.map(_.toString)).get
      val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
      try {
        jdbcWrapper.resolveTable(conn, tableNameOrSubquery)
      } finally {
        conn.close()
      }
    }
  }

  override def toString: String = s"RedshiftRelation($tableNameOrSubquery)"

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val saveMode = if (overwrite) {
      SaveMode.Overwrite
    } else {
      SaveMode.Append
    }
    val writer = new RedshiftWriter(jdbcWrapper, s3ClientFactory)
    writer.saveToRedshift(sqlContext, data, saveMode, params)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filterNot(filter => FilterPushdown.buildFilterExpression(schema, filter).isDefined)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    if (requiredColumns.isEmpty) {
      // In the special case where no columns were requested, issue a `count(*)` against Redshift
      // rather than unloading data.
      val whereClause = FilterPushdown.buildWhereClause(schema, filters)
      val countQuery = s"SELECT count(*) FROM $tableNameOrSubquery $whereClause"
      log.info(countQuery)
      val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
      try {
        val results = jdbcWrapper.executeQueryInterruptibly(conn.prepareStatement(countQuery))
        if (results.next()) {
          val numRows = results.getLong(1)
          val parallelism = sqlContext.getConf("spark.sql.shuffle.partitions", "200").toInt
          val emptyRow = Row.empty
          sqlContext.sparkContext.parallelize(1L to numRows, parallelism).map(_ => emptyRow)
        } else {
          throw new IllegalStateException("Could not read count from Redshift")
        }
      } finally {
        conn.close()
      }
    } else {
      val creds = AWSCredentialsUtils.load(params, sqlContext.sparkContext.hadoopConfiguration)
      // Unload data from Redshift into a temporary directory in S3:
      Utils.checkThatBucketHasObjectLifecycleConfiguration(params.rootTempDir, s3ClientFactory(creds))
      val tempDir = params.createPerQueryTempDir()
      val unloadSql = buildUnloadStmt(requiredColumns, filters, tempDir)
      log.info(unloadSql)
      val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
      try {
        jdbcWrapper.executeInterruptibly(conn.prepareStatement(unloadSql))
      } finally {
        conn.close()
      }
      // Read the MANIFEST file to get the list of S3 part files that were written by Redshift.
      // We need to use a manifest in order to guard against S3's eventually-consistent listings.
      val filesToRead: Seq[String] = {
        val cleanedTempDirUri =
          Utils.fixS3Url(Utils.removeCredentialsFromURI(URI.create(tempDir)).toString)
        val s3URI = new AmazonS3URI(cleanedTempDirUri)
        val s3Client = s3ClientFactory(creds)
        val is = s3Client.getObject(s3URI.getBucket, s3URI.getKey + "manifest").getObjectContent
        val s3Files = try {
          val entries = Json.parse(new InputStreamReader(is)).asObject().get("entries").asArray()
          entries.iterator().asScala.map(_.asObject().get("url").asString()).toSeq
        } finally {
          is.close()
        }
        // The filenames in the manifest are of the form s3://bucket/key, without credentials.
        // If the S3 credentials were originally specified in the tempdir's URI, then we need to
        // reintroduce them here
        s3Files.map { file =>
          tempDir.stripSuffix("/") + '/' + file.stripPrefix(cleanedTempDirUri).stripPrefix("/")
        }
      }
      // Create a DataFrame to read the unloaded data:
      val rdd: RDD[(lang.Long, Array[String])] = {
        val rdds = filesToRead.map { file =>
          sqlContext.sparkContext.newAPIHadoopFile(
            file,
            classOf[RedshiftInputFormat],
            classOf[java.lang.Long],
            classOf[Array[String]])
        }.toArray
        sqlContext.sparkContext.union(rdds)
      }
      val prunedSchema = pruneSchema(schema, requiredColumns)
      rdd.values.mapPartitions { iter =>
        val converter: Array[String] => Row = Conversions.createRowConverter(prunedSchema)
        iter.map(converter)
      }
    }
  }

  private def buildUnloadStmt(
      requiredColumns: Array[String],
      filters: Array[Filter],
      tempDir: String): String = {
    assert(!requiredColumns.isEmpty)
    // Always quote column names:
    val columnList = requiredColumns.map(col => s""""$col"""").mkString(", ")
    val whereClause = FilterPushdown.buildWhereClause(schema, filters)
    val creds = AWSCredentialsUtils.load(params, sqlContext.sparkContext.hadoopConfiguration)
    val credsString: String = AWSCredentialsUtils.getRedshiftCredentialsString(params, creds)
    val query = {
      // Since the query passed to UNLOAD will be enclosed in single quotes, we need to escape
      // any backslashes and single quotes that appear in the query itself
      val escapedTableNameOrSubqury = tableNameOrSubquery.replace("\\", "\\\\").replace("'", "\\'")
      s"SELECT $columnList FROM $escapedTableNameOrSubqury $whereClause"
    }
    // We need to remove S3 credentials from the unload path URI because they will conflict with
    // the credentials passed via `credsString`.
    val fixedUrl = Utils.fixS3Url(Utils.removeCredentialsFromURI(new URI(tempDir)).toString)

    s"UNLOAD ('$query') TO '$fixedUrl' WITH CREDENTIALS '$credsString' ESCAPE MANIFEST"
  }

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }
}
