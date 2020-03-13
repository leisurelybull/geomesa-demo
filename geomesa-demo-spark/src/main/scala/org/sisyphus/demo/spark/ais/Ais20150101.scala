package org.sisyphus.demo.spark.ais

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.locationtech.geomesa.spark.jts._

/**
 * 存储20150101的ais数据到geomesa hbase
 */
object Ais20150101 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("2018fusion")
      .config("spark.sql.crossJoin.enabled", "true")
      .master("local[2]")
      .getOrCreate()
      .withJTS

    val aisData = this.getClass.getClassLoader.getResource("AIS_2015_01_Zone01.csv").getPath

    val df = sparkSession.read
      // 自动推断列类型
      .option("inferSchema", "true")
      // 指定一个指示空值的字符串
      .option("nullvalue", "0")
      // 当设置为 true 时，第一行文件将被用来命名列，而不包含在数据中
      .option("header", "true")
      .csv(aisData)

    //    df.show()
    //    df.printSchema()

    df.createOrReplaceTempView("df0")

    val df1 = sparkSession.sql("select cast(mmsi as varchar(10)) mmsi , basedatetime as time, st_point(lon,lat) geom, " +
      "sog, cog, heading, vesselname, imo, callsign, vesseltype, status, length, width, draft, cargo from df0")

    df1.show()
    df1.printSchema()

    // 写入HBase
    val dsParams = Map("hbase.catalog" -> "ais",
      "hbase.zookeepers" -> "hadoop001:2181")

    import sparkSession.implicits._  // 隐式类型转换
    df1
      .withColumn("__fid__", $"mmsi") // 指定索引id
      .write
//      .mode(SaveMode.Overwrite)
      .format("geomesa") // 指定DataStore类型
      .options(dsParams)
      .option("geomesa.feature", "20150101") // 指定图层名称
      .save()
  }
}
