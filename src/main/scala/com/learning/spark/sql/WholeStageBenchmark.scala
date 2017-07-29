package com.learning.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by lgrcyanny on 17/7/27.
  */
object WholeStageBenchmark {
  val spark = SparkSession.builder().appName(this.getClass.getName).getOrCreate()
  import spark.implicits._

  def benchmark(name: String)(f: => Unit) = {
    val startTime = System.nanoTime()
    f
    val endTime = System.nanoTime()
    println(s"$name in ${(endTime - startTime).toDouble / 1000000000.0}s")
  }

  def sumValues() = {
    benchmark("Sum Values") {
      spark.range(1000 * 1000 * 1000).selectExpr("sum(id)").show
    }
  }

  def joinValues() = {
    benchmark("joinValues") {
      val df1 = spark.range(1000 * 1000 * 1000)
      val df2 = spark.range(1000)
      val joined = df1.join(df2, "id")
      joined.explain(true)
      val joinedCount = joined.count()
      println(s"Joined count: $joinedCount")
      // == Parsed Logical Plan ==
      //    'Join UsingJoin(Inner,List(id))
      //    :- Range (0, 1000000000, step=1, splits=Some(4))
      //    +- Range (0, 1000, step=1, splits=Some(4))
      //
      //    == Analyzed Logical Plan ==
      //    id: bigint
      //    Project [id#24L]
      //    +- Join Inner, (id#24L = id#27L)
      //    :- Range (0, 1000000000, step=1, splits=Some(4))
      //    +- Range (0, 1000, step=1, splits=Some(4))
      //
      //    == Optimized Logical Plan ==
      //    Project [id#24L]
      //    +- Join Inner, (id#24L = id#27L)
      //    :- Range (0, 1000000000, step=1, splits=Some(4))
      //    +- Range (0, 1000, step=1, splits=Some(4))
      //
      //    == Physical Plan ==
      //      Project [id#24L]
      //    +- BroadcastHashJoin [id#24L], [id#27L], Inner, BuildRight
      //    :- Range (0, 1000000000, step=1, splits=4)
      //    +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]))
      //    +- Range (0, 1000, step=1, splits=4)
    }
  }

  def benchmark1() = {
    spark.conf.set("spark.sql.codegen.wholeStage", false)
    println("spark.sql.codegen.wholeStage=false")
    // Sum Values in 14.815269276s
    sumValues()
    spark.conf.set("spark.sql.codegen.wholeStage", true)
    println("spark.sql.codegen.wholeStage=true")
    // Sum Values in 0.877105465s
    sumValues()
  }

  def benchmark2() = {
    spark.conf.set("spark.sql.codegen.wholeStage", false)
    println("spark.sql.codegen.wholeStage=false")
    // joinValues in 30.123704777s
    joinValues()
    spark.conf.set("spark.sql.codegen.wholeStage", true)
    println("spark.sql.codegen.wholeStage=true")
    // joinValues in 1.569773731s
    joinValues()
  }

  def main(args: Array[String]): Unit = {
    benchmark1()
    benchmark2()
  }

}
