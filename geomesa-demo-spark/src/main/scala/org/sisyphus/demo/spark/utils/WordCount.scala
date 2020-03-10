package org.sisyphus.demo.spark.utils

import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("WorCount")
      .master("local[1]")
      .getOrCreate()

    val file = this.getClass.getClassLoader.getResource("file").getPath

    sparkSession.sparkContext.textFile(file).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).foreach(println(_))
  }
}
