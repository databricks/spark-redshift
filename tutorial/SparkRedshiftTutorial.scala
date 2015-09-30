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

package com.databricks.spark.redshift.tutorial

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SaveMode
import java.io.File
import org.apache.spark.sql.types.{ StructType, StructField, 
                                    StringType, IntegerType, 
                                    LongType, DecimalType }

/**
 * Source code accompanying the spark-redshift tutorial. 
 * The following parameters need to be passed
 * 1. AWS Access Key
 * 2. AWS Secret Access Key
 * 3. Redshift Database Name
 * 4. Redshift UserId
 * 5. Redshift Password
 * 6. Redshift URL (Ex. swredshift.czac2vcs84ci.us-east-1.redshift.amazonaws.com:5439
 * 
 */
object SparkRedshiftTutorial {
  /*
   * For Windows Users only
   * 1. Download contents from link 
   *      https://github.com/srccodes/hadoop-common-2.2.0-bin/archive/master.zip
   * 2. Unzip the file in step 1 into your %HADOOP_HOME%/bin. 
   *        In the example below  %HADOOP_HOME%=C:/MySoftware/hdphome
   * 3. After step 2 %HADOOP_HOME/bin should contain winutils.exe
   * 
   */   
	if (System.getProperty("os.name").startsWith("Windows")) {
		val hdphome: File = new File("C:/MySoftware/hdphome");
		System.setProperty("hadoop.home.dir", hdphome.getAbsolutePath().toString())
	}

	def main(args: Array[String]): Unit = {
		val awsAccessKey = args(0)
		val awsSecretKey = args(1)
		val rsDbName = args(2)
		val rsUser = args(3)
		val rsPassword = args(4)
    /** Sample Redshift URL is swredshift.czac2vcs84ci.us-east-1.redshift.amazonaws.com:5439 */
		val rsURL = args(5)  
		val jdbcURL = s"""jdbc:redshift://$rsURL/$rsDbName?user=$rsUser&password=$rsPassword"""
    println(jdbcURL)
		val sc = new SparkContext(new SparkConf().setAppName("SparkSQL").setMaster("local"))

		val tempS3Dir = "s3n://redshift-spark/temp/"
		sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", awsAccessKey)
		sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", awsSecretKey)

		val sqlContext = new org.apache.spark.sql.SQLContext(sc)

		import sqlContext.implicits._
    /** Load from a table */
		val eventsDF = sqlContext.read
			.format("com.databricks.spark.redshift")
			.option("url", jdbcURL) 
			.option("tempdir", tempS3Dir) 
			.option("dbtable", "event") 
			.load()
		eventsDF.show()
		eventsDF.printSchema()

    /** Load from a query */
		val salesQuery = """select salesid,listid,sellerid,buyerid,
                        eventid,dateid,qtysold,pricepaid,commission 
                        from sales 
                        order by saletime desc LIMIT 10000"""
		val salesDF = sqlContext.read
			.format("com.databricks.spark.redshift")
			.option("url", jdbcURL)
			.option("tempdir", tempS3Dir)
			.option("query", salesQuery)
			.load()
		salesDF.show()

		val eventQuery = "select * from event"
		val eventDF = sqlContext.read
			.format("com.databricks.spark.redshift")
			.option("url", jdbcURL) 
			.option("tempdir", tempS3Dir)
			.option("query", eventQuery)
			.load()

    /** Register 'event' table as 'myevent' in the Spark Environment */
		eventsDF.registerTempTable("myevent") 

    /** Save to a redshift table from a table registered in spark */
		sqlContext.sql("select * from myevent where eventid<=1000").withColumnRenamed("eventid", "id")
			.write.format("com.databricks.spark.redshift")
			.option("url", jdbcURL)
			.option("tempdir", tempS3Dir)
			.option("dbtable", "redshiftevent")
			.mode(SaveMode.Overwrite)
			.save()
    
    /** Demonstration of Append capability */
		sqlContext.sql("select * from myevent where eventid>1000").withColumnRenamed("eventid", "id")
			.write.format("com.databricks.spark.redshift")
			.option("url", jdbcURL) 
			.option("tempdir", tempS3Dir)
			.option("dbtable", "redshiftevent")
			.mode(SaveMode.Append) 
			.save()

		/** Demonstration of Inter-operability */
		val salesAGGQuery = """select sales.eventid as id,sum(qtysold) as totalqty,sum(pricepaid)  as salesamt
                           from sales
                           group by (sales.eventid)
                           """
		val salesAGGDF = sqlContext.read
			.format("com.databricks.spark.redshift")
			.option("url", jdbcURL) 
			.option("tempdir", tempS3Dir) 
			.option("query", salesAGGQuery) 
			.load()
		salesAGGDF.registerTempTable("salesagg")

    /*
     * Join two DataFrame instances. Each could be sourced from any 
     * compatible Data Source
     */
		val salesAGGDF2 = salesAGGDF.join(eventsDF, salesAGGDF("id") === eventsDF("eventid"))
			.select("id", "eventname", "totalqty", "salesamt")

    /** Create a custom schema */
		val schema = StructType(List(StructField("eventid", IntegerType, false),
			StructField("eventname", StringType, false),
			StructField("qtysold", LongType, false),
			StructField("revenue", DecimalType(38, 2), false)))
      
    /** Apply custom schema on existing DataFrame */
		val salesAGGDFWithSchema = sqlContext.createDataFrame(salesAGGDF2.rdd, schema)
		salesAGGDFWithSchema.registerTempTable("redshift_sales_agg")

    /** Save with custom schema */
		sqlContext.sql("select * from redshift_sales_agg")
			.write.format("com.databricks.spark.redshift")
			.option("url", jdbcURL)
			.option("tempdir", tempS3Dir)
			.option("dbtable", "redshift_sales_agg")
			.mode(SaveMode.Overwrite)
			.save()


	}
}