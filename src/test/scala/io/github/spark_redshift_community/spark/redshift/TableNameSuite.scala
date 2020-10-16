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

package io.github.spark_redshift_community.spark.redshift

import org.scalatest.FunSuite

class TableNameSuite extends FunSuite {
  test("TableName.parseFromEscaped") {
    assert(TableName.parseFromEscaped("foo.bar") === TableName("foo", "bar"))
    assert(TableName.parseFromEscaped("foo") === TableName("PUBLIC", "foo"))
    assert(TableName.parseFromEscaped("\"foo\"") === TableName("PUBLIC", "foo"))
    assert(TableName.parseFromEscaped("\"\"\"foo\"\"\".bar") === TableName("\"foo\"", "bar"))
    // Dots (.) can also appear inside of valid identifiers.
    assert(TableName.parseFromEscaped("\"foo.bar\".baz") === TableName("foo.bar", "baz"))
    assert(TableName.parseFromEscaped("\"foo\"\".bar\".baz") === TableName("foo\".bar", "baz"))
  }

  test("TableName.toString") {
    assert(TableName("foo", "bar").toString === """"foo"."bar"""")
    assert(TableName("PUBLIC", "bar").toString === """"PUBLIC"."bar"""")
    assert(TableName("\"foo\"", "bar").toString === "\"\"\"foo\"\"\".\"bar\"")
  }
}
