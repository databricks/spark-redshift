package com.databricks.spark.redshift

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, FunSuite}

class SchemaGenerationSuite extends FunSuite with Matchers with MockFactory with BeforeAndAfterAll {
  /**
   * Expected parsed output corresponding to the output of testData.
   */
  val expectedData =
    Array(
      Row(1.toByte, true, TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0), 1234152.123124981,
        1.0f, 42, 1239012341823719L, 23, "Unicode是樂趣", TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1)),
      Row(1.toByte, false, TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0), 0.0, 0.0f, 42, 1239012341823719L, -13, "asdf",
        TestUtils.toTimestamp(2015, 6, 2, 0, 0, 0, 0)),
      Row(0.toByte, null, TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0), 0.0, -1.0f, 4141214, 1239012341823719L, null, "f",
        TestUtils.toTimestamp(2015, 6, 3, 0, 0, 0)),
      Row(0.toByte, false, null, -1234152.123124981, 100000.0f, null, 1239012341823719L, 24, "___|_123", null),
      Row(List.fill(10)(null): _*))

  var sc: SparkContext = _
  var testSqlContext: SQLContext = _
  var df: DataFrame = _

  def varcharCol(meta:Metadata): String = {
    val maxLength:Long = meta.getLong("maxLength")

    maxLength match {
      case _:Long => s"VARCHAR($maxLength)"
      case _ => "VARCHAR(255)"
    }
  }

  /**
   * Compute the schema string for this RDD.
   */
  def schemaString(df: DataFrame): String = {
    val sb = new StringBuilder()

    df.schema.fields foreach { field => {
      val name = field.name
      val typ: String =
        field match {
          case StructField(_, IntegerType, _, _) => "INTEGER"
          case StructField(_, LongType, _, _) => "BIGINT"
          case StructField(_, DoubleType, _, _) => "DOUBLE PRECISION"
          case StructField(_, FloatType, _, _) => "REAL"
          case StructField(_, ShortType, _, _) => "INTEGER"
          case StructField(_, ByteType, _, _) => "BYTE"
          case StructField(_, BooleanType, _, _) => "BOOLEAN"
          case StructField(_, StringType, _, metadata) => varcharCol(metadata)
          case StructField(_, BinaryType, _, _) => "BLOB"
          case StructField(_, TimestampType, _, _) => "TIMESTAMP"
          case StructField(_, DateType, _, _) => "DATE"
          case StructField(_, t:DecimalType, _, _) => s"DECIMAL(${t.precision}},${t.scale}})"
          case _ => throw new IllegalArgumentException(s"Don't know how to save $field to JDBC")
        }
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }
    }
    if (sb.length < 2) "" else sb.substring(2)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    sc = new TestContext
    testSqlContext = new SQLContext(sc)

    df = testSqlContext.createDataFrame(sc.parallelize(expectedData), TestUtils.testSchema)
  }

  override def afterAll(): Unit = {
    sc.stop()
    super.afterAll()
  }

  test("Schema inference") {
    val enhancedDf:DataFrame = Conversions.injectMetaSchema(testSqlContext, df)

    schemaString(enhancedDf) should equal("testByte BYTE , testBool BOOLEAN , testDate DATE , testDouble DOUBLE PRECISION , testFloat REAL , testInt INTEGER , testLong BIGINT , testShort INTEGER , testString VARCHAR(10) , testTimestamp TIMESTAMP ")
  }
}
