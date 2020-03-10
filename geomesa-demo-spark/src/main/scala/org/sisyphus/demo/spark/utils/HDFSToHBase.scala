package org.sisyphus.demo.spark.utils

import org.apache.spark.sql.SparkSession
import org.locationtech.geomesa.spark.jts._

object HDFSToHBase {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("testSpark")
      .config("spark.sql.crossJoin.enabled", "true")
      .master("local[2]")
      .getOrCreate()
      .withJTS

    val df = sparkSession.read.csv("D:\\data\\geomesa\\spatio_temporal_data.log")
    //    val df = sparkSession.read.csv("/home/user/data/2018fusion.txt")

    df.createOrReplaceTempView("df0")

    val df1 = sparkSession.sql("select _c0 name, cast(_c1 as double) lng, cast(_c2 as double) lat,cast(_c3 as timestamp) time from df0 limit 100")

    df1.createOrReplaceTempView("df1")

    val df2 = sparkSession.sql("select name, time, st_point(cast(lng as decimal(8,2)), cast(lat as decimal(8,2))) geom from df1")

    df2.show(false)

    df2.printSchema()

    // 写入HBase
    val dsParams = Map("hbase.catalog" -> "geomesa_HDFSToHBase",
      "hbase.zookeepers" -> "hadoop001:2181")


    df2
//      .withColumn("__fid__", $"name") // 指定索引id
      .write
      .format("geomesa") // 指定DataStore类型
      .options(dsParams)
      .option("geomesa.feature", "batch") // 指定图层名称
      .save()


  }
}
