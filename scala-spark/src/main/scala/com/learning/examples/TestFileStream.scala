package com.learning.examples

import org.apache.spark.SparkConf;
import org.apache.spark.streaming._;

object TestFileStream {

  def main(args: Array[String]): Unit = {
    println("Hello")
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val ssc = new StreamingContext(conf,Seconds(10))
    val file = ssc.textFileStream("C:\\spark\\hema\\stream\\")
    val counts = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.print()
    ssc.start()
    ssc.awaitTermination()
  }

}