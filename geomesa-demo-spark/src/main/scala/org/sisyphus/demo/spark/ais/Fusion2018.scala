package org.sisyphus.demo.spark.ais

import org.apache.spark.sql.SparkSession
import org.locationtech.geomesa.spark.jts._

object test {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("testSpark")
      .config("spark.sql.crossJoin.enabled", "true")
      .master("local[2]")
      .getOrCreate()
      .withJTS

    val aisData = this.getClass.getClassLoader.getResource("2018fusion.txt").getPath

    val df = sparkSession.read.csv(aisData)

//    df.show()
//    df.printSchema()

    df.createOrReplaceTempView("df0")

    val df1 = sparkSession.sql("select _c2 mmsi, cast(_c3 as double) lng, cast(_c4 as double) lat from df0 limit 100")

//    df1.show()
//    df1.printSchema()

    df1.createOrReplaceTempView("df1")

    val df2 = sparkSession.sql("select mmsi, st_point(cast(lng as decimal(8,2)), cast(lat as decimal(8,2))) geom from df1")

    df2.show(false)
    df2.printSchema()

    // 写入HBase
    val dsParams = Map("hbase.catalog" -> "fusion",
      "hbase.zookeepers" -> "hadoop001:2181")


    df2
//      .withColumn("__fid__", $"name") // 指定索引id
      .write
      .format("geomesa") // 指定DataStore类型
      .options(dsParams)
      .option("geomesa.feature", "2018") // 指定图层名称
      .save()


  }
}
