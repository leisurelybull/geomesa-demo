package org.sisyphus.demo.spark.person

import java.sql.Timestamp

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.locationtech.geomesa.spark.jts._

/**
 * 使用spark streaming处理kafka过来的数据
 */
object SparkStreaming {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      // hadoop001:2181 test streamingtopic 1
      println("Usage: ImoocStatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

//    // 测试步骤一：测试数据接收
//    messages.map(_._2).count().print

    // 测试步骤二：数据清洗

    val log = messages.map(_._2)

    val cleanData = log.map(line => {
      val datas = line.split("\t")
      val name = datas(0)
      val longitude = datas(1).toDouble
      val latitude = datas(2).toDouble
      val time = Timestamp.valueOf(datas(3))

      PersonPoint(name, longitude, latitude, time)
    })

    //    println(cleanData)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate().withJTS
    import sparkSession.implicits._

    // 写入HBase
    val dsParams = Map("hbase.catalog" -> "geomesa_SparkToHBase",
      "hbase.zookeepers" -> "hadoop001:2181")

    val rdd = cleanData.foreachRDD(rdd => {
      val personPointDF = rdd.toDF()
      personPointDF.createOrReplaceTempView("PersonPoint")

      val df2 = sparkSession.sql("select name, time, " +
        "st_point(cast(longitude as decimal(8,2)), cast(latitude as decimal(8,2))) geom " +
        "from PersonPoint")

      df2.show(true)
      df2.printSchema()

      df2
        .withColumn("__fid__", $"name") // 指定索引id
        .write
        .format("geomesa") // 指定DataStore类型
        .options(dsParams)
        .option("geomesa.feature", "stream") // 指定图层名称
        .save()
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
