import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by dheeraj on 3/7/16.
 */
object ApacheLogs {
  def main(args: Array[String]) {
    val output = "output"
    val sc = new SparkContext("local", "wordcount", new SparkConf())
    try {
      val input = sc.textFile("access_log");
      val count = Methods.count(input)
  //    sc.parallelize(List(count)).saveAsTextFile(output) //convert String to RDD
      val pageNotFoundErrors = Methods.countPageNotFound(input);
     // sc.parallelize(List(pageNotFoundErrors)).saveAsTextFile(output)
      pageNotFoundErrors.saveAsTextFile(output)
    }
    finally {
      sc.stop()
    }

  }
}

object Methods {
  def count(input: RDD[String]): String = {
    input.count().toString
  }

  def countPageNotFound(input: RDD[String]) = {
    input.flatMap(line => line.split("\n")).filter(line => line.contains("200") && line.contains("mailman"))
  }
}


