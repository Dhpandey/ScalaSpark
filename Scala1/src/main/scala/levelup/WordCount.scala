package levelup

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by dheeraj on 3/4/16.
 */
object WordCount {
  def main(args: Array[String]) = {
    val output ="output/wordcount"
    val sc = new SparkContext("local", "WordCount", new SparkConf().setAppName(" Word Count"))
    try {
      val input = sc.textFile("input.csv").map(line => line.toUpperCase())
      val result = input.flatMap(line=>line.split(","))
        .map(word=>(word,1))
        .reduceByKey(_+_)
        result.saveAsTextFile(output)
      println("Word Count Problem")
    } finally {
      sc.stop()
    }
  }
}
