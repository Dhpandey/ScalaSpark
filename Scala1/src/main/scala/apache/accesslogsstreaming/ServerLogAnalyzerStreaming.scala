package apache.accesslogsstreaming

import apache.utility.{AccessLogs, CustomOrdering}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Duration}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by dheeraj on 3/10/16.
 */

/**
 * Uses Spark Streaming
 * 1. get data and parse using utility class to create RDD
 * 2. Calculates content size, min max and average
 * 3.Counts Response code
 * 4 Counts IPAddress and show that is accessed more then 10 times
 * 5.Counts Endpoints and order them on the basis of count using custom ordering class
 * Note :CustomOrdering and AccessLogParser are two utilities used.
 *
 * How to run :
 * start Apache server /lampp/wampp/xampp
 * tail -f [LOG_FILE] | nc -lk 9999
 * cat apache_log_file >> [YOUR_LOG_FILE]
 *
 * spark-submit --class apache.accesslogstreaming.ServerLogAnalyzerStreaming --master
 * local ScalaSpark/Scala1/target/scala-2.10/Scala1-assembly-1.0.jar > output.txt
 *
 */
class ServerLogAnalyzerStreaming {

  def calcContentSize(log: RDD[AccessLogs]) = {
    val size = log.map(log => log.contentSize).cache()
    val average = size.reduce(_ + _) / size.count()
    println("ContentSize:: Average :: " + average + " " +
      " || Maximum :: " + size.max() + "  || Minimum ::" + size.min())
  }

  def responseCodeCount(log: RDD[AccessLogs]) = {
    val responseCount = log.map(log => (log.responseCode, 1))
      .reduceByKey(_ + _)
      .take(1000)
    println( s"""ResponseCodes Count : ${responseCount.mkString("[", ",", "]")} """)
  }

  def ipAddressFilter(log: RDD[AccessLogs]) = {
    val result = log.map(log => (log.ipAddr, 1))
      .reduceByKey(_ + _)
      .filter(count => count._2 > 1)
     // .map(_._1).take(10)
      .collect()

    println( s"""Ip Addresses :: ${result.mkString("[", ",", "]")}""")
  }

  def manageEndPoints(log: RDD[AccessLogs]) = {
    val result = log.map(log => (log.endPoint, 1))
      .reduceByKey(_ + _)
      .top(10)(CustomOrdering.SecondValueSorting)

    println( s"""EndPoints :: ${result.mkString("[", ",", "]")}""")
  }
}

object ServerLogAnalyzerStreaming {
  val WINDOW_LENGTH = new Duration(30 * 1000)
  val SLIDE_INTERVAL = new Duration(10 * 1000)

  def main(args: Array[String]) {

    val logObj = new ServerLogAnalyzerStreaming
    val context = new SparkContext("local", "Analyzer", new SparkConf().setAppName("Server Log Analyzer"))
    val streamingContext = new StreamingContext(context, SLIDE_INTERVAL)
    val logLinesStream = streamingContext.socketTextStream("localhost", 9999)
    val logs = logLinesStream.map(logFile => AccessLogs.logParser(logFile)).cache()
    val windowDataStream = logs.window(WINDOW_LENGTH)

    windowDataStream.foreachRDD(logs => {
      if (logs.count() == 0) {
        println("Your stream has zero log !!!!!")
      } else {
        logObj.calcContentSize(logs)
        logObj.responseCodeCount(logs)
        logObj.ipAddressFilter(logs)
        logObj.manageEndPoints(logs)
      }
    })
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
