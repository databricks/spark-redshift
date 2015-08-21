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

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{UTF8String, StructType}

/**
 * Helper methods for pushing filters into Redshift queries.
 */
private[redshift] object FilterPushdown {
  /**
   * Build a SQL WHERE clause for the given filters. If a filter cannot be pushed down then no
   * condition will be added to the WHERE clause. If none of the filters can be pushed down then
   * an empty string will be returned.
   *
   * @param schema the schema of the table being queried
   * @param filters an array of filters, the conjunction of which is the filter condition for the
   *                scan.
   */
  def buildWhereClause(schema: StructType, filters: Seq[Filter]): String = {
    val filterClauses = filters.collect {
      case EqualTo(attr, value) => s"${sqlQuote(attr)} = ${compileValue(value)}"
      case LessThan(attr, value) => s"${sqlQuote(attr)} < ${compileValue(value)}"
      case GreaterThan(attr, value) => s"${sqlQuote(attr)} > ${compileValue(value)}"
      case LessThanOrEqual(attr, value) => s"${sqlQuote(attr)} <= ${compileValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"${sqlQuote(attr)} >= ${compileValue(value)}"
    }.mkString(" AND ")

    if (filterClauses.isEmpty) "" else "WHERE " + filterClauses
  }

  private def sqlQuote(identifier: String) = s""""$identifier""""

  private def compileValue(value: Any): Any = value match {
    case stringValue: String => s"\\'${escapeSql(stringValue)}\\'"
    case stringValue: UTF8String => s"\\'${escapeSql(stringValue.toString())}\\'"
    case v => v
  }

  private def escapeSql(value: String): String =
    if (value == null) null else value.replace("'", "\\'\\'")
}
