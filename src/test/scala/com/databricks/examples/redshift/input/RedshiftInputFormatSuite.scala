/*
 * Copyright 2014 Databricks
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
package com.databricks.examples.redshift.input

import java.io.{DataOutputStream, File, FileOutputStream}

import scala.language.implicitConversions

import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import com.databricks.examples.redshift.input.RedshiftInputFormat._

class RedshiftInputFormatSuite extends FunSuite with BeforeAndAfterAll {

  import com.databricks.examples.redshift.input.RedshiftInputFormatSuite._

  private var sc: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new SparkContext("local", this.getClass.getName)
  }

  override def afterAll(): Unit = {
    sc.stop()
    super.afterAll()
  }

  private def writeToFile(contents: String, file: File): Unit = {
    val bytes = contents.getBytes
    val out = new DataOutputStream(new FileOutputStream(file))
    out.write(bytes, 0, bytes.length)
    out.close()
  }

  private def escape(records: Set[Seq[String]], delimiter: Char): String = {
    require(delimiter != '\\' && delimiter != '\n')
    records.map { r =>
      r.map { f =>
        f.replace("\\", "\\\\")
          .replace("\n", "\\\n")
          .replace(delimiter, "\\" + delimiter)
      }.mkString(delimiter)
    }.mkString("", "\n", "\n")
  }

  private final val KEY_BLOCK_SIZE = "fs.local.block.size"

  private final val TAB = '\t'

  private val records = Set(
    Seq("a\n", DEFAULT_DELIMITER + "b\\"),
    Seq("c", TAB + "d"),
    Seq("\ne", "\\\\f"))

  private def withTempDir(func: File => Unit): Unit = {
    val dir = Files.createTempDir()
    dir.deleteOnExit()
    func(dir)
  }

  test("default delimiter") {
    withTempDir { dir =>
      val escaped = escape(records, DEFAULT_DELIMITER)
      writeToFile(escaped, new File(dir, "part-00000"))

      val conf = new Configuration
      conf.setLong(KEY_BLOCK_SIZE, 4)

      val rdd = sc.newAPIHadoopFile(dir.toString, classOf[RedshiftInputFormat],
        classOf[java.lang.Long], classOf[Array[String]], conf)
      assert(rdd.partitions.size > records.size) // so there exist at least one empty partition
      println("############" + rdd.values.map(_.toSeq).glom().map(_.toSeq).collect().toSeq)
      val actual = rdd.values.map(_.toSeq).collect()
      assert(actual.size === records.size)
      assert(actual.toSet === records)
    }
  }

  test("customized delimiter") {
    withTempDir { dir =>
      val escaped = escape(records, TAB)
      writeToFile(escaped, new File(dir, "part-00000"))

      val conf = new Configuration
      conf.setLong(KEY_BLOCK_SIZE, 4)
      conf.set(KEY_DELIMITER, TAB)

      val rdd = sc.newAPIHadoopFile(dir.toString, classOf[RedshiftInputFormat],
        classOf[java.lang.Long], classOf[Array[String]], conf)
      assert(rdd.partitions.size > records.size) // so there exist at least one empty partitions

      val actual = rdd.values.map(_.toSeq).collect()
      assert(actual.size === records.size)
      assert(actual.toSet === records)
    }
  }

  test("schema parser") {
    withTempDir { dir =>
      val testRecords = Set(
        Seq("a\n", 1, 1.0),
        Seq("b", 2, 2.0))
      val escaped = escape(testRecords.map(_.map(_.toString)), DEFAULT_DELIMITER)
      writeToFile(escaped, new File(dir, "part-00000"))

      val conf = new Configuration
      conf.setLong(KEY_BLOCK_SIZE, 4)

      val sqlContext = new SQLContext(sc)

      val srdd = sqlContext.redshiftFile(dir.toString, "name varchar(10) id integer score float")
      val expectedSchema = StructType(Seq(
        StructField("name", StringType, nullable = true),
        StructField("id", IntegerType, nullable = true),
        StructField("score", DoubleType, nullable = true)))
      assert(srdd.schema === expectedSchema)
      val parsed = srdd.map { case Row(name: String, id: Int, score: Double) =>
        Seq(name, id, score)
      }.collect().toSet

      assert(parsed === testRecords)
    }
  }
}

object RedshiftInputFormatSuite {

  implicit def charToString(c: Char): String = c.toString
}
