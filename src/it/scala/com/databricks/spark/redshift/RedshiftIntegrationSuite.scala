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
import java.sql.Connection
import java.util.Properties

import scala.util.Random

import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, Matchers}
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{AnalysisException, Row, SQLContext, SaveMode}
import org.apache.spark.sql.hive.test.TestHiveContext

/**
 * End-to-end tests which run against a real Redshift cluster.
 */
class RedshiftIntegrationSuite
  extends QueryTest
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  private def loadConfigFromEnv(envVarName: String): String = {
    Option(System.getenv(envVarName)).getOrElse {
      fail(s"Must set $envVarName environment variable")
    }
  }

  // The following configurations must be set in order to run these tests. In Travis, these
  // environment variables are set using Travis's encrypted environment variables feature:
  // http://docs.travis-ci.com/user/environment-variables/#Encrypted-Variables

  // JDBC URL listed in the AWS console (should not contain username and password).
  private val AWS_REDSHIFT_JDBC_URL: String = loadConfigFromEnv("AWS_REDSHIFT_JDBC_URL")
  private val AWS_REDSHIFT_USER: String = loadConfigFromEnv("AWS_REDSHIFT_USER")
  private val AWS_REDSHIFT_PASSWORD: String = loadConfigFromEnv("AWS_REDSHIFT_PASSWORD")
  private val AWS_ACCESS_KEY_ID: String = loadConfigFromEnv("TEST_AWS_ACCESS_KEY_ID")
  private val AWS_SECRET_ACCESS_KEY: String = loadConfigFromEnv("TEST_AWS_SECRET_ACCESS_KEY")
  // Path to a directory in S3 (e.g. 's3n://bucket-name/path/to/scratch/space').
  private val AWS_S3_SCRATCH_SPACE: String = loadConfigFromEnv("AWS_S3_SCRATCH_SPACE")
  require(AWS_S3_SCRATCH_SPACE.contains("s3n"), "must use s3n:// URL")

  private val jdbcUrl: String = {
    s"$AWS_REDSHIFT_JDBC_URL?user=$AWS_REDSHIFT_USER&password=$AWS_REDSHIFT_PASSWORD"
  }

  /**
   * Random suffix appended appended to table and directory names in order to avoid collisions
   * between separate Travis builds.
   */
  private val randomSuffix: String = Math.abs(Random.nextLong()).toString

  private val tempDir: String = AWS_S3_SCRATCH_SPACE + randomSuffix + "/"

  /**
   * Spark Context with Hadoop file overridden to point at our local test data file for this suite,
   * no-matter what temp directory was generated and requested.
   */
  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _
  private var conn: Connection = _

  private val test_table: String = s"test_table_$randomSuffix"
  private val test_table2: String = s"test_table2_$randomSuffix"
  private val test_table3: String = s"test_table3_$randomSuffix"

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new SparkContext("local", "RedshiftSourceSuite")
    sc.hadoopConfiguration.set("fs.s3.awsAccessKeyId", AWS_ACCESS_KEY_ID)
    sc.hadoopConfiguration.set("fs.s3.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY_ID)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)

    conn = DefaultJDBCWrapper.getConnector(
      "com.amazon.redshift.jdbc4.Driver", jdbcUrl, new Properties())()

    conn.prepareStatement("drop table if exists test_table").executeUpdate()
    conn.prepareStatement("drop table if exists test_table2").executeUpdate()
    conn.prepareStatement("drop table if exists test_table3").executeUpdate()
    conn.commit()

    def createTable(tableName: String): Unit = {
      conn.createStatement().executeUpdate(
        s"""
           |create table $tableName (
           |testbyte int2,
           |testbool boolean,
           |testdate date,
           |testdouble float8,
           |testfloat float4,
           |testint int4,
           |testlong int8,
           |testshort int2,
           |teststring varchar(256),
           |testtimestamp timestamp
           |)
      """.stripMargin
      )
      // scalastyle:off
      conn.createStatement().executeUpdate(
        s"""
           |insert into $tableName values
           |(null, null, null, null, null, null, null, null, null, null),
           |(0, null, '2015-07-03', 0.0, -1.0, 4141214, 1239012341823719, null, 'f', '2015-07-03 00:00:00.000'),
           |(0, false, null, -1234152.12312498, 100000.0, null, 1239012341823719, 24, '___|_123', null),
           |(1, false, '2015-07-02', 0.0, 0.0, 42, 1239012341823719, -13, 'asdf', '2015-07-02 00:00:00.000'),
           |(1, true, '2015-07-01', 1234152.12312498, 1.0, 42, 1239012341823719, 23, 'Unicode''s樂趣', '2015-07-01 00:00:00.001')
         """.stripMargin
      )
      // scalastyle:on
      conn.commit()
    }

    createTable(test_table)
    createTable(test_table2)
    createTable(test_table3)
  }

  override def afterAll(): Unit = {
    try {
      val fs = FileSystem.get(URI.create(tempDir), sc.hadoopConfiguration)
      fs.delete(new Path(tempDir), true)
      fs.close()
    } finally {
      try {
        conn.prepareStatement(s"drop table if exists $test_table").executeUpdate()
        conn.prepareStatement(s"drop table if exists $test_table2").executeUpdate()
        conn.prepareStatement(s"drop table if exists $test_table3").executeUpdate()
        conn.commit()
        conn.close()
      } finally {
        try {
          sc.stop()
        } finally {
          super.afterAll()
        }
      }
    }
  }

  override def beforeEach(): Unit = {
    sqlContext = new TestHiveContext(sc)
    sqlContext.sql(
      s"""
         | create temporary table test_table(
         |   testbyte tinyint,
         |   testbool boolean,
         |   testdate date,
         |   testdouble double,
         |   testfloat float,
         |   testint int,
         |   testlong bigint,
         |   testshort smallint,
         |   teststring string,
         |   testtimestamp timestamp
         | )
         | using com.databricks.spark.redshift
         | options(
         |   url \"$jdbcUrl\",
         |   tempdir \"$tempDir\",
         |   dbtable \"$test_table\"
         | )
       """.stripMargin
    ).collect()

    sqlContext.sql(
      s"""
         | create temporary table test_table2(
         |   testbyte smallint,
         |   testbool boolean,
         |   testdate date,
         |   testdouble double,
         |   testfloat float,
         |   testint int,
         |   testlong bigint,
         |   testshort smallint,
         |   teststring string,
         |   testtimestamp timestamp
         | )
         | using com.databricks.spark.redshift
         | options(
         |   url \"$jdbcUrl\",
         |   tempdir \"$tempDir\",
         |   dbtable \"$test_table2\"
         | )
       """.stripMargin
    ).collect()

    sqlContext.sql(
      s"""
         | create temporary table test_table3(
         |   testbyte smallint,
         |   testbool boolean,
         |   testdate date,
         |   testdouble double,
         |   testfloat float,
         |   testint int,
         |   testlong bigint,
         |   testshort smallint,
         |   teststring string,
         |   testtimestamp timestamp
         | )
         | using com.databricks.spark.redshift
         | options(
         |   url \"$jdbcUrl\",
         |   tempdir \"$tempDir\",
         |   dbtable \"$test_table3\"
         | )
       """.stripMargin
    ).collect()
  }

  test("DefaultSource can load Redshift UNLOAD output to a DataFrame") {
    checkAnswer(
      sqlContext.sql("select * from test_table"),
      TestUtils.expectedData)
  }

  test("Can load output when 'dbtable' is a subquery wrapped in parentheses") {
    // scalastyle:off
    val query =
      s"""
        |(select testbyte, testbool
        |from $test_table
        |where testbool = true
        | and teststring = 'Unicode''s樂趣'
        | and testdouble = 1234152.12312498
        | and testfloat = 1.0
        | and testint = 42)
      """.stripMargin
    // scalastyle:on
    val loadedDf = sqlContext.read
      .format("com.databricks.spark.redshift")
      .option("url", jdbcUrl)
      .option("dbtable", query)
      .option("tempdir", tempDir)
      .load()
    checkAnswer(loadedDf, Seq(Row(1, true)))
  }

  test("Can load output when 'query' is specified instead of 'dbtable'") {
    // scalastyle:off
    val query =
      s"""
        |select testbyte, testbool
        |from $test_table
        |where testbool = true
        | and teststring = 'Unicode''s樂趣'
        | and testdouble = 1234152.12312498
        | and testfloat = 1.0
        | and testint = 42
      """.stripMargin
    // scalastyle:on
    val loadedDf = sqlContext.read
      .format("com.databricks.spark.redshift")
      .option("url", jdbcUrl)
      .option("query", query)
      .option("tempdir", tempDir)
      .load()
    checkAnswer(loadedDf, Seq(Row(1, true)))
  }

  test("DefaultSource supports simple column filtering") {
    checkAnswer(
      sqlContext.sql("select testbyte, testbool from test_table"),
      Seq(
        Row(null, null),
        Row(0.toByte, null),
        Row(0.toByte, false),
        Row(1.toByte, false),
        Row(1.toByte, true)))
  }

  test("query with pruned and filtered scans") {
    // scalastyle:off
    checkAnswer(
      sqlContext.sql(
        """
          |select testbyte, testbool
          |from test_table
          |where testbool = true
          | and teststring = "Unicode's樂趣"
          | and testdouble = 1234152.12312498
          | and testfloat = 1.0
          | and testint = 42
        """.stripMargin),
      Seq(Row(1, true)))
    // scalastyle:on
  }

  test("roundtrip save and load") {
    val tableName = s"roundtrip_save_and_load_$randomSuffix"
    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema)
        .write
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("tempdir", tempDir)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = sqlContext.read
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("tempdir", tempDir)
        .load()
      checkAnswer(loadedDf, TestUtils.expectedData)
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }

  test("SaveMode.Overwrite with non-existent table") {
    val tableName = s"overwrite_non_existent_table$randomSuffix"
    try {
      assert(!DefaultJDBCWrapper.tableExists(conn, tableName))
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema)
        .write
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("tempdir", tempDir)
        .mode(SaveMode.Overwrite)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = sqlContext.read
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("tempdir", tempDir)
        .load()
      checkAnswer(loadedDf, TestUtils.expectedData)
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }

  // TODO:test overwrite that fails.

  test("Append SaveMode doesn't destroy existing data") {
    val extraData = Seq(
      Row(2.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L,
        24.toShort, "___|_123", null))

    sqlContext.createDataFrame(sc.parallelize(extraData), TestUtils.testSchema).write
      .format("com.databricks.spark.redshift")
      .option("url", jdbcUrl)
      .option("dbtable", test_table3)
      .option("tempdir", tempDir)
      .mode(SaveMode.Append)
      .saveAsTable(test_table3)

    checkAnswer(
      sqlContext.sql("select * from test_table3"),
      TestUtils.expectedData ++ extraData)
  }

  test("Respect SaveMode.ErrorIfExists when table exists") {
    val rdd = sc.parallelize(TestUtils.expectedData.toSeq)
    val df = sqlContext.createDataFrame(rdd, TestUtils.testSchema)
    df.registerTempTable(test_table) // to ensure that the table already exists

    // Check that SaveMode.ErrorIfExists throws an exception
    intercept[AnalysisException] {
      df.write
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("dbtable", test_table)
        .option("tempdir", tempDir)
        .mode(SaveMode.ErrorIfExists)
        .saveAsTable(test_table)
    }
  }

  test("Do nothing when table exists if SaveMode = Ignore") {
    val rdd = sc.parallelize(TestUtils.expectedData.drop(1))
    val df = sqlContext.createDataFrame(rdd, TestUtils.testSchema)
    df.write
      .format("com.databricks.spark.redshift")
      .option("url", jdbcUrl)
      .option("dbtable", test_table)
      .option("tempdir", tempDir)
      .mode(SaveMode.Ignore)
      .saveAsTable(test_table)

    // Check that SaveMode.Ignore does nothing
    checkAnswer(
      sqlContext.sql("select * from test_table"),
      TestUtils.expectedData)
  }
}
