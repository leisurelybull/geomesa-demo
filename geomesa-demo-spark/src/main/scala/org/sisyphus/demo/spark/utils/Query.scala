package org.sisyphus.demo.spark.utils

import org.apache.spark.sql.SparkSession
import org.locationtech.geomesa.spark.jts._

object Query {
  def main(args: Array[String]): Unit = {
    // DataStore params to a GeoMesa HBase table
    val dsParams = Map("hbase.catalog" -> "ais",
      "hbase.zookeepers" -> "hadoop001:2181")

    // Create SparkSession
    val sparkSession = SparkSession.builder()
      .appName("testSpark")
      .config("spark.sql.crossJoin.enabled", "true")
      .master("local[*]")
      .getOrCreate()
      .withJTS

    // Create DataFrame using the "geomesa" format
    val dataFrame = sparkSession.read
      .format("geomesa")
      .options(dsParams)
      .option("geomesa.feature", "20150101")
      .load()

    dataFrame.createOrReplaceTempView("table")

    val res = sparkSession.sql("select * from table")
    res.show(false)
    res.printSchema()

//    sparkSession.sql("select * from table where st_covers(st_makeBBOX(-78.5, 37.5, -78.0, 38.0),Where) " +
//      "and Who = 'Bierce' and When > '2014-07-01T00:00:00.000Z' and When < '2014-09-30T23:59:59.999Z'")
////    sparkSession.sql("select * from table")
//      .show(false)
  }
}
