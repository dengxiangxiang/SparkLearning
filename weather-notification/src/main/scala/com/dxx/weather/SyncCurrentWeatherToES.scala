package com.dxx.weather

import java.text.SimpleDateFormat
import java.util.Date

import json.{JsonArray, JsonObject}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.mutable.ListBuffer

object SyncCurrentWeatherToES {
  def main(args: Array[String]): Unit = {
    val sc: SparkSession = SparkSession.builder()
      .appName(WeatherToESMain.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val lineDS: Dataset[String] = sc.read.textFile("/Users/xxdeng/Documents/Work/WeatherNotification/synccurrentweather.log.formatted")
    import sc.implicits._

    val recordStringDS: Dataset[String] =
      lineDS
        .map(decode(_))
        .filter(_ != null)


    val esConf: Map[String, String] = Map(
      "es.http.timeout" -> "5m",
      "es.http.retries" -> "50",
      "es.nodes.wan.only" -> "true",
      "es.nodes.discovery" -> "false",
      "es.batch.size.bytes" -> "10mb",
      "es.batch.write.retry.wait" -> "120s",
      "es.batch.write.retry.count" -> "3",
      "es.index.auto.create" -> "true",
      "es.nodes" -> "myPersonalMac",
      "es.port" -> "9200")

    EsSpark.saveJsonToEs(recordStringDS.rdd, "sync-currentweather/_doc", esConf)
  }


  // format:
  // 2021-05-26 20:05:01,2021-05-26 20:08:55,b1myyndd6171b66us8rtyys3v.json.gz
  def decode(line: String) = {

    var result: String = null;
    try {

      val array: Array[String] = line.split(",")

      val starttime: String = array(0)
      val startTimestamp: Long = getTimestamp(starttime)

      val endtime: String = array(1)
      val endTimestamp: Long = getTimestamp(endtime)

      val filename: String = array(2)

      val js = new JsonObject()

      js.put("starttime",Long.box(startTimestamp))
      js.put("endtime", Long.box(endTimestamp))
      js.put("filename", filename)

      result = js.toString


    } catch {
      case e: Exception => {
        println(e)
      }
    }

    result

  }

  // format: 2021-04-29T19:40:45.715Z
  def getTimestamp(dateString: String): Long = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date: Date = dateFormat.parse(dateString)
    val timestamp: Long = date.getTime
    timestamp
  }


}
