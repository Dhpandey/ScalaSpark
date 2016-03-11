package apache.accesslogsSQL

import apache.utility.AccessLogs
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by dheeraj on 3/10/16.
 */

/**
 * Uses SparkSql
 * 1. get data and parse using utility class to create RDD
 * 2. Calculates content size, min max and average
 * 3.Counts Response code
 * 4 Counts IPAddress and show that is accessed more then 10 times
 * 5.Counts Endpoints and order them on the basis of count using custom ordering class
 * Note :CustomOrdering and AccessLogParser are two utilities used.
 *
 * How to Run:
 * spark-submit --class apache.accesslogsSQL.ServerLogAnalyzerSQL --master
 * local ScalaSpark/Scala1/target/scala-2.10/Scala1-assembly-1.0.jar > output.txt
 */
class ServerLogAnalyzerSQL {
  def contentSize(sqlContext: SQLContext) = {
    val contentSize = sqlContext.sql("SELECT SUM(contentSize), COUNT(*),MIN(contentSize)," +
      " MAX(contentSize)" + " from AccessLogTable").first()

    println("Content SIze :: Average : %s , Min:  %s Max : %s"
      .format(contentSize.getLong(0) / contentSize.getLong(1), contentSize(2), contentSize(3)))
  }

  def responseCodeCount(sqLContext: SQLContext) {
    val responseCount = sqLContext.sql("SELECT responseCode,COUNT(*) from AccessLogTable" +
      " GROUP BY responseCode LIMIT 1000").collect()

    println( s"""ResponseCode :: ${responseCount.mkString("[", ",", "]")}""")
  }

  def inAddressFilter(sqlContext: SQLContext) = {
    val result = sqlContext.sql("SELECT ipAddr, COUNT(*) AS total from AccessLogTable " +
      "GROUP BY ipAddr HAVING total>1")
      //.map(row => row.getString(0))  // just to collect only IP
      .map(row =>(row.getString(0),row.getLong(1)))
      .collect()
    println( s"""IP address :: ${result.mkString("[", ",", "]")}""")

  }

  def countAndOrderEndPoints(sqlContext: SQLContext) = {
    val result = sqlContext.sql("SELECT endPoint, COUNT(*) AS total from AccessLogTable " +
      "GROUP BY endPoint ORDER BY total DESC LIMIT 10 ")
      .map(row => (row.getString(0), row.getLong(1)))
      .collect()
    println( s"""End Point Calculation :: ${result.mkString("[", ",", "]")}""")
  }

}

object ServerLogAnalyzerSQL {
  def main(args: Array[String]) {

    val logObj = new ServerLogAnalyzerSQL
    val sparkContext = new SparkContext("local", "sql log Analyzer",
      new SparkConf().setAppName("Sql Log Analyzer"))

    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._
    val logs = sparkContext.textFile("data/serverLogs.txt").map(log => AccessLogs.logParser(log)).toDF()
    logs.registerTempTable("AccessLogTable")
    sqlContext.cacheTable("AccessLogTable")

    logObj.contentSize(sqlContext)
    logObj.responseCodeCount(sqlContext)
    logObj.inAddressFilter(sqlContext)
    logObj.countAndOrderEndPoints(sqlContext)

  }
}