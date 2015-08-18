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

import java.io.File
import java.sql.Connection
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.jdbc.DefaultJDBCWrapper
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

/**
 * End-to-end tests which run against a real Redshift cluster.
 */
class RedshiftIntegrationSuite extends FunSuite with Matchers with BeforeAndAfterAll {
  val jdbcUrl = s"${System.getenv("aws_redshift_jdbc_url")}?" +
    s"user=${System.getenv("aws_redshift_user")}&password=${System.getenv("aws_redshift_password")}"
  val tempDir = System.getenv("aws_s3_scratch_space")

  val accessKeyId = System.getenv("aws_access_key_id")
  val secretAccessKey = System.getenv("aws_secret_access_key")

  /**
   * Expected parsed output corresponding to the output of testData.
   */
  val expectedData = Array(
    Row(List.fill(10)(null): _*),
    Row(0.toByte, null, TestUtils.toDate(2015, 6, 3), 0.0, -1.0f, 4141214, 1239012341823719L, null, "f",
      TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0)),
    Row(0.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L, 24.toShort, "___|_123", null),
    Row(1.toByte, false, TestUtils.toDate(2015, 6, 2), 0.0, 0.0f, 42, 1239012341823719L, -13.toShort, "asdf",
      TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
    Row(1.toByte, true, TestUtils.toDate(2015, 6, 1), 1234152.12312498,
      1.0f, 42, 1239012341823719L, 23.toShort, "Unicode's樂趣", TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1))
  )


  /**
   * Spark Context with hadoop file overridden to point at our local test data file for this suite,
   * no-matter what temp directory was generated and requested.
   */
  private var sc: SparkContext = _
  private var conn: Connection = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new SparkContext("local", "RedshiftSourceSuite")

    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKeyId)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretAccessKey)

    conn = DefaultJDBCWrapper.getConnector("org.postgresql.Driver", jdbcUrl, new Properties())()

    conn.prepareStatement("drop table if exists test_table").executeUpdate()
    conn.prepareStatement("drop table if exists test_table2").executeUpdate()
    conn.prepareStatement("drop table if exists test_table3").executeUpdate()
    conn.commit()

    def createTable(tableName: String): Unit = {
      conn.prepareStatement(
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
      ).executeUpdate()

      conn.prepareStatement(
        s"""
           |insert into $tableName values (
           |null, null, null, null, null, null, null, null, null, null)
        """.stripMargin
      ).executeUpdate()

      conn.prepareStatement(
        s"""
           |insert into $tableName values (
           |0, null, '2015-07-03', 0.0, -1.0, 4141214, 1239012341823719, null, 'f', '2015-07-03 00:00:00.000')
        """.stripMargin
      ).executeUpdate()

      conn.prepareStatement(
        s"""
           |insert into $tableName values (
           |0, false, null, -1234152.12312498, 100000.0, null, 1239012341823719, 24, '___|_123', null)
        """.stripMargin
      ).executeUpdate()

      conn.prepareStatement(
        s"""
           |insert into $tableName values (
           |1, false, '2015-07-02', 0.0, 0.0, 42, 1239012341823719, -13, 'asdf', '2015-07-02 00:00:00.000')
        """.stripMargin
      ).executeUpdate()

      conn.prepareStatement(
        s"""
           |insert into $tableName values (
           |1, true, '2015-07-01', 1234152.12312498, 1.0, 42,
           |1239012341823719, 23, 'Unicode''s樂趣', '2015-07-01 00:00:00.001')
        """.stripMargin
      ).executeUpdate()

      conn.commit()
    }

    createTable("test_table")
    createTable("test_table2")
    createTable("test_table3")

    conn.commit()
  }

  override def afterAll(): Unit = {
    val temp = new File(tempDir)
    val tempFiles = temp.listFiles()
    if(tempFiles != null) tempFiles foreach {
      case f => if(f != null) f.delete()
    }
    temp.delete()

    conn.prepareStatement("drop table if exists test_table").executeUpdate()
    conn.prepareStatement("drop table if exists test_table2").executeUpdate()
    conn.prepareStatement("drop table if exists test_table3").executeUpdate()
    conn.commit()
    conn.close()

    sc.stop()
    super.afterAll()
  }

  test("DefaultSource can load Redshift UNLOAD output to a DataFrame") {
    val sqlContext = new SQLContext(sc)
    sqlContext.sql(
      s"""
         |create temporary table test_table(
         |  testbyte tinyint,
         |  testbool boolean,
         |  testdate date,
         |  testdouble double,
         |  testfloat float,
         |  testint int,
         |  testlong bigint,
         |  testshort smallint,
         |  teststring string,
         |  testtimestamp timestamp
         |)
         |using com.databricks.spark.redshift
         |options(
         |  url \"$jdbcUrl\",
         |  tempdir \"$tempDir\",
         |  dbtable \"test_table\"
         |)
       """.stripMargin
    ).collect()

    sqlContext.sql("select * from test_table order by testbyte, testbool").collect()
      .zip(expectedData).foreach {
      case (loaded, expected) => loaded shouldBe expected
    }
  }

  test("DefaultSource supports simple column filtering") {
    val sqlContext = new SQLContext(sc)
    sqlContext.sql(
      s"""
         |create temporary table test_table(
         |  testbyte tinyint,
         |  testbool boolean,
         |  testdate date,
         |  testdouble double,
         |  testfloat float,
         |  testint int,
         |  testlong bigint,
         |  testshort smallint,
         |  teststring string,
         |  testtimestamp timestamp
         |)
         |using com.databricks.spark.redshift
         |options(
         |  url \"$jdbcUrl\",
         |  tempdir \"$tempDir\",
         |  dbtable \"test_table\"
         |)
       """.stripMargin
    )

    val prunedExpectedValues = Array(
      Row(null, null),
      Row(0.toByte, null),
      Row(0.toByte, false),
      Row(1.toByte, false),
      Row(1.toByte, true)
    )

    sqlContext.sql("select testbyte, testbool from test_table order by testbyte, testbool").collect()
      .zip(prunedExpectedValues).foreach {
      case (loaded, expected) => loaded shouldBe expected
    }
  }

  test("DefaultSource supports user schema, pruned and filtered scans") {
    val sqlContext = new SQLContext(sc)
    sqlContext.sql(
      s"""
         |create temporary table test_table(
         |  testbyte tinyint,
         |  testbool boolean,
         |  testdate date,
         |  testdouble double,
         |  testfloat float,
         |  testint int,
         |  testlong bigint,
         |  testshort smallint,
         |  teststring string,
         |  testtimestamp timestamp
         |)
         |using com.databricks.spark.redshift
         |options(
         |  url \"$jdbcUrl\",
         |  tempdir \"$tempDir\",
         |  dbtable \"test_table\"
         |)
       """.stripMargin
    )

    // We should now only have one matching row, with two columns
    val filteredExpectedValues = Array(Row(1, true))
    sqlContext.sql(
      """
        |select testbyte, testbool
        |from test_table
        |where testbool = true
        | and teststring = "Unicode's樂趣"
        | and testdouble = 1234152.12312498
        | and testfloat = 1.0
        | and testint = 42
      """.stripMargin
    ).collect().zip(filteredExpectedValues).foreach {
      case (loaded, expected) => loaded shouldBe expected
    }
  }

  test("DefaultSource using 'query' supports user schema, pruned and filtered scans") {
    val sqlContext = new SQLContext(sc)
    sqlContext.sql(
      s"""
         |create temporary table test_table(
         |  testbyte tinyint,
         |  testbool boolean,
         |  testdate date,
         |  testdouble double,
         |  testfloat float,
         |  testint int,
         |  testlong bigint,
         |  testshort smallint,
         |  teststring string,
         |  testtimestamp timestamp
         |)
         |using com.databricks.spark.redshift
         |options(
         |  url \"$jdbcUrl\",
         |  tempdir \"$tempDir\",
         |  query \"select * from test_table\"
         |)
       """.stripMargin
    )

    // We should now only have one matching row, with two columns
    val filteredExpectedValues = Array(Row(1, true))

    sqlContext.sql(
      """
        |select testbyte, testbool
        |from test_table
        |where testbool = true
        | and teststring = "Unicode's樂趣"
        | and testdouble = 1234152.12312498
        | and testfloat = 1.0
        | and testint = 42
      """.stripMargin
    ).collect().zip(filteredExpectedValues).foreach {
      case (loaded, expected) => loaded shouldBe expected
    }
  }

  test("DefaultSource serializes data as Avro, then sends Redshift COPY command") {
    val extraData = Array(
      Row(2.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L, 24.toShort, "___|_123", null))

    val sqlContext = new SQLContext(sc)
    sqlContext.sql(
      s"""
         |create temporary table test_table2(
         |  testbyte smallint,
         |  testbool boolean,
         |  testdate date,
         |  testdouble double,
         |  testfloat float,
         |  testint int,
         |  testlong bigint,
         |  testshort smallint,
         |  teststring string,
         |  testtimestamp timestamp
         |)
         |using com.databricks.spark.redshift
         |options(
         |  url \"$jdbcUrl\",
         |  tempdir \"$tempDir\",
         |  dbtable \"test_table2\"
         |)
       """.stripMargin
    )

    val rdd = sc.parallelize(extraData.toSeq)
    val df = sqlContext.createDataFrame(rdd, TestUtils.testSchema)
    df.write.format("com.databricks.spark.redshift").mode(SaveMode.Overwrite).insertInto("test_table2")

    sqlContext.sql("select * from test_table2").collect()
      .zip(extraData).foreach {
      case (loaded, expected) => loaded shouldBe expected
    }
  }

  test("Append SaveMode doesn't destroy existing data") {
    val extraData = Array(
      Row(2.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L, 24.toShort, "___|_123", null))

    val sqlContext = new SQLContext(sc)
    sqlContext.sql(
      s"""
         |create temporary table test_table3(
         |  testbyte smallint,
         |  testbool boolean,
         |  testdate date,
         |  testdouble double,
         |  testfloat float,
         |  testint int,
         |  testlong bigint,
         |  testshort smallint,
         |  teststring string,
         |  testtimestamp timestamp
         |)
         |using com.databricks.spark.redshift
         |options(
         |  url \"$jdbcUrl\",
         |  tempdir \"$tempDir\",
         |  dbtable \"test_table3\"
         |)
       """.stripMargin
    )

    val rdd = sc.parallelize(extraData.toSeq)
    val df = sqlContext.createDataFrame(rdd, TestUtils.testSchema)
    df.write.format("com.databricks.spark.redshift").mode(SaveMode.Append).saveAsTable("test_table3")

    sqlContext.sql("select * from test_table3 order by testbyte, testbool").collect()
      .zip(expectedData ++ extraData).foreach {
      case (loaded, expected) => loaded shouldBe expected
    }
  }

  test("Respect SaveMode.ErrorIfExists when table exists") {
    val sqlContext = new SQLContext(sc)
    sqlContext.sql(
      s"""
         |create temporary table test_table(
         |  testbyte smallint,
         |  testbool boolean,
         |  testdate date,
         |  testdouble double,
         |  testfloat float,
         |  testint int,
         |  testlong bigint,
         |  testshort smallint,
         |  teststring string,
         |  testtimestamp timestamp
         |)
         |using com.databricks.spark.redshift
         |options(
         |  url \"$jdbcUrl\",
         |  tempdir \"$tempDir\",
         |  dbtable \"test_table\"
         |)
       """.stripMargin
    )

    val rdd = sc.parallelize(expectedData.toSeq)
    val df = sqlContext.createDataFrame(rdd, TestUtils.testSchema)

    // Check that SaveMode.ErrorIfExists throws an exception
    intercept[Exception] {
      df.write.format("com.databricks.spark.redshift").mode(SaveMode.ErrorIfExists).saveAsTable("test_table")
    }
  }

  test("Do nothing when table exists if SaveMode = Ignore") {
    val sqlContext = new SQLContext(sc)
    sqlContext.sql(
      s"""
         |create temporary table test_table(
         |  testbyte smallint,
         |  testbool boolean,
         |  testdate date,
         |  testdouble double,
         |  testfloat float,
         |  testint int,
         |  testlong bigint,
         |  testshort smallint,
         |  teststring string,
         |  testtimestamp timestamp
         |)
         |using com.databricks.spark.redshift
         |options(
         |  url \"$jdbcUrl\",
         |  tempdir \"$tempDir\",
         |  dbtable \"test_table\"
         |)
       """.stripMargin
    )

    val rdd = sc.parallelize(expectedData.toSeq)
    val df = sqlContext.createDataFrame(rdd, TestUtils.testSchema)
    df.write.format("com.databricks.spark.redshift").mode(SaveMode.Ignore).saveAsTable("test_table")

    // Check that SaveMode.Ignore does nothing
    sqlContext.sql("select * from test_table order by testbyte, testbool").collect()
      .zip(expectedData).foreach {
      case (loaded, expected) => loaded shouldBe expected
    }
  }
}