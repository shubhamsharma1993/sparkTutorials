package org.text.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
  
  def main(args : Array[String]) = {
    
    val conf = new SparkConf()
    .setAppName("WordCount")
    .setMaster("local")
    
    // create spark context and passing configuration setting
    val sc = new SparkContext(conf)
    
    
    //create RDD from food text file
    
    val text = sc.textFile("food.txt")
    .flatMap { line => 
      line.split(" ")  
    }.map { word => 
      (word,1)
    }.reduceByKey(_ + _)
    .saveAsTextFile("food.count.txt")    
  }
  
}