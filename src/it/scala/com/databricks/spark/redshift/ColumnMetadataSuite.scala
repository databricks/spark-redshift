/*
 * Copyright 2016 Databricks
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

import java.sql.SQLException

import org.apache.spark.sql.{SaveMode, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType, MetadataBuilder}

/**
 * End-to-end tests of features which depend on per-column metadata (such as comments, maxlength).
 */
class ColumnMetadataSuite extends IntegrationSuiteBase {

  test("configuring maxlength on string columns") {
    val tableName = s"configuring_maxlength_on_string_column_$randomSuffix"
    try {
      val metadata = new MetadataBuilder().putLong("maxlength", 512).build()
      val schema = StructType(
        StructField("x", StringType, metadata = metadata) :: Nil)
      write(sqlContext.createDataFrame(sc.parallelize(Seq(Row("a" * 512))), schema))
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), Seq(Row("a" * 512)))
      // This append should fail due to the string being longer than the maxlength
      intercept[SQLException] {
        write(sqlContext.createDataFrame(sc.parallelize(Seq(Row("a" * 513))), schema))
          .option("dbtable", tableName)
          .mode(SaveMode.Append)
          .save()
      }
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
    }
  }

  test("configuring compression on columns") {
    val tableName = s"configuring_compression_on_columns_$randomSuffix"
    try {
      val metadata = new MetadataBuilder().putString("encoding", "LZO").build()
      val schema = StructType(
        StructField("x", StringType, metadata = metadata) :: Nil)
      write(sqlContext.createDataFrame(sc.parallelize(Seq(Row("a" * 128))), schema))
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), Seq(Row("a" * 128)))
      val encodingDF = sqlContext.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable",
          s"""(SELECT "column", lower(encoding) FROM pg_table_def WHERE tablename='$tableName')""")
        .load()
      checkAnswer(encodingDF, Seq(Row("x", "lzo")))
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
    }
  }

  test("configuring comments on columns") {
    val tableName = s"configuring_comments_on_columns_$randomSuffix"
    try {
      val metadata = new MetadataBuilder().putString("description", "Hello Column").build()
      val schema = StructType(
        StructField("x", StringType, metadata = metadata) :: Nil)
      write(sqlContext.createDataFrame(sc.parallelize(Seq(Row("a" * 128))), schema))
        .option("dbtable", tableName)
        .option("description", "Hello Table")
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      checkAnswer(read.option("dbtable", tableName).load(), Seq(Row("a" * 128)))
      val tableDF = sqlContext.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", s"(SELECT pg_catalog.obj_description('$tableName'::regclass))")
        .load()
      checkAnswer(tableDF, Seq(Row("Hello Table")))
      val commentQuery =
        s"""
           |(SELECT c.column_name, pgd.description
           |FROM pg_catalog.pg_statio_all_tables st
           |INNER JOIN pg_catalog.pg_description pgd
           |   ON (pgd.objoid=st.relid)
           |INNER JOIN information_schema.columns c
           |   ON (pgd.objsubid=c.ordinal_position AND c.table_name=st.relname)
           |WHERE c.table_name='$tableName')
         """.stripMargin
      val columnDF = sqlContext.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", commentQuery)
        .load()
      checkAnswer(columnDF, Seq(Row("x", "Hello Column")))
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
    }
  }
}
