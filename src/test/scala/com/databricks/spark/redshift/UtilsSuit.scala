package com.databricks.spark.redshift

import org.scalatest.{Matchers, FunSuite}

/**
 * Unit tests for helper functions
 */
class UtilsSuit extends FunSuite with Matchers {

  test("joinUrls preserves protocol information") {
    Utils.joinUrls("s3n://foo/bar/", "/baz") shouldBe "s3n://foo/bar/baz/"
  }

  test("fixUrl produces Redshift-compatible equivalents") {
    Utils.fixS3Url("s3a://foo/bar/12345") shouldBe "s3://foo/bar/12345"
    Utils.fixS3Url("s3n://foo/bar/baz") shouldBe "s3://foo/bar/baz"
  }

  test("temp paths are random subdirectories of root") {
    val root = "s3n://temp/"
    val firstTempPath = Utils.makeTempPath(root)

    Utils.makeTempPath(root) should (startWith (root) and endWith ("/")
      and not equal root and not equal firstTempPath)
  }
}
