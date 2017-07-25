package com.learning.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lgrcyanny on 16/4/5.
 */
object WordCount {
  val conf = new SparkConf().setAppName("WordCountTest")
  val sc = new SparkContext(conf)
  def main(args: Array[String]) {
    val path = if (args.length == 1) args(0) else "./README.md"
    val data = sc.textFile(path, 10)
    val res = data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()
    res.take(10).foreach(println)
  }
}
