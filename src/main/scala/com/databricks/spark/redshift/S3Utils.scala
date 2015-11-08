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

import com.amazonaws.services.s3.{AmazonS3URI, AmazonS3Client}
import com.amazonaws.services.s3.model.{DeleteObjectsRequest, S3ObjectSummary}

private[redshift] object S3Utils {

  /**
   * Get all files in the S3 bucket whose paths match the given prefix.
   */
  def listS3Objects(
      s3Client: AmazonS3Client,
      bucketName: String,
      prefix: String): Iterator[S3ObjectSummary] = {
    var listing = s3Client.listObjects(bucketName, prefix)
    var objectSummariesIterator = listing.getObjectSummaries.iterator()
    new Iterator[S3ObjectSummary] {
      override def hasNext: Boolean = {
        if (objectSummariesIterator.hasNext) {
          true
        } else if (listing.isTruncated) {
          listing = s3Client.listNextBatchOfObjects(listing)
          objectSummariesIterator = listing.getObjectSummaries.iterator()
          objectSummariesIterator.hasNext
        } else {
          false
        }
      }
      override def next(): S3ObjectSummary = objectSummariesIterator.next()
    }
  }

  def deleteS3Objects(
      s3Client: AmazonS3Client,
      bucketName: String,
      keysToDelete: Seq[String]): Unit = {
    // According to http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
    // we can only delete up to 1000 keys per request:
    keysToDelete.grouped(1000).foreach { batchOfKeys =>
      s3Client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(batchOfKeys: _*))
    }
  }

  def toS3URI(uri: String): AmazonS3URI = {
    new AmazonS3URI(
      Utils.fixS3Url(
        Utils.removeCredentialsFromURI(URI.create(uri)).toString).stripSuffix("/"))
  }
}
