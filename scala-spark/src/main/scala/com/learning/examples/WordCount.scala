package com.learning.examples
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;


object WordCount {
  def main(args: Array[String]): Unit = {
    println("Hello")
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val file1 = "C:\\Java-eclipse\\Hema\\WC-Input.txt"
    val textFile = sc.textFile(file1,2).cache()
    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile("C:\\Java-eclipse\\Hema\\scalaoutN")
    sc.stop();
  }


}