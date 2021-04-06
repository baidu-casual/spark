package main

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark").setMaster("local");
    val sc = new SparkContext(conf)

    val data = Array("Gaurav", "Rohan", "Anuj", "Aryan", "Rahul", "Rohan", "Rohan", "Gaurav", "Karan")
    val distData = sc.parallelize(data)
    val pairs = distData.map(s => (s, 1))
    val counts = pairs.reduceByKey((a, b) => a + b)
    println("\n\n\n\n\nThe Data is : "+distData.collect+"\n\n\n\n\n")
    println("\n\n\n\n\nThe Data is : "+data+"\n\n\n\n\n")
    println("\n\n\n\n\nThe Elements are : "+pairs.collect+"\n\n\n\n\n")
    println("\n\n"+counts.collect+"\n\n")
  }
}