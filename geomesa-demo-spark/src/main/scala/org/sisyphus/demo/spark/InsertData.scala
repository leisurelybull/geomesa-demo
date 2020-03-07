package org.sisyphus.demo.spark

import org.apache.spark.sql.SparkSession
import org.locationtech.geomesa.spark.jts._

/**
 * Created by sweet on 2019/8/29.
 */
object InsertData {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("testSpark")
      .config("spark.sql.crossJoin.enabled", "true")
      .master("local[2]")
      .getOrCreate()
      .withJTS

    val df = sparkSession.read.csv("D:\\data\\2018fusion.txt")
    //    val df = sparkSession.read.csv("/home/user/data/2018fusion.txt")

    df.createOrReplaceTempView("df0")

    val df1 = sparkSession.sql("select _c2 mmsi, cast(_c3 as double) lng, cast(_c4 as double) lat from df0 limit 100")

    df1.createOrReplaceTempView("df1")

    val df2 = sparkSession.sql("select st_point(cast(lng as decimal(8,2)), cast(lat as decimal(12,8))) geom from df1")

    df2.show(false)

    df2.printSchema()

    // 写入HBase
    val dsParams = Map("hbase.catalog" -> "geomesa_hbase",
      "hbase.zookeepers" -> "hadoop001:2181")

    df2.write
      .format("geomesa")
      .options(dsParams)
      .option("geomesa.feature", "geom")
      .save()
  }
}
