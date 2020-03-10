package org.sisyphus.demo.spark.person

import java.sql.Timestamp

/**
 * 清洗后的点信息
 *
 * @param name 人名
 * @param longitude 经度
 * @param latitude 纬度
 * @param time 时间
 */
case class PersonPoint(name: String, longitude: Double, latitude: Double, time: Timestamp)
