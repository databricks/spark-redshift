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

import java.io.File
import java.net.URI
import java.util.UUID

/**
 * Various arbitrary helper functions
 */
object Utils {
  /**
   * Joins prefix URL a to path suffix b, and appends a trailing /, in order to create
   * a temp directory path for S3.
   */
  def joinUrls(a: String, b: String): String = {
    val uri = new URI(a)
    val joinedPath = new File(uri.getPath, b).toString
    s"${new URI(uri.getScheme, uri.getUserInfo, uri.getHost, uri.getPort, joinedPath, uri.getQuery, uri.getFragment)}/"
  }

  /**
   * Creates a randomly named temp directory path for intermediate data
   */
  def makeTempPath(tempRoot: String): String = Utils.joinUrls(tempRoot, UUID.randomUUID().toString)
}
