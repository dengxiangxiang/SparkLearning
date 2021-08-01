package com.dxx.weather

import java.text.SimpleDateFormat
import java.util.Date

import json.{JsonArray, JsonObject}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.mutable.ListBuffer

object WeatherCityToES {
  def main(args: Array[String]): Unit = {
    val sc: SparkSession = SparkSession.builder()
      .appName(WeatherToESMain.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val lineDS: Dataset[String] = sc.read.textFile("/Users/xxdeng/Documents/Work/OpenTerra_Weather_Data/weather-city.json")
    import sc.implicits._

    val recordStringDS: Dataset[String] =
      lineDS
        .map(decode(_))
        .filter(_ != null)

    //    recordStringDS.collect().foreach(println)

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

    EsSpark.saveJsonToEs(recordStringDS.rdd, "weather-city", esConf)
  }


  // format:
  // {"city_id":"10oqsidv96xe3cn4750y3xneb", "location_id":"dcc30c0qt7w04b0leajad7tie", "lastupdated":"2021-04-14 23:26:10", "name":"Escuintla,  Guatemala", "city":"Escuintla", "statecode":"", "postalcode":"", "countrycode":"GT", "geotype":"Point", "lat":-90.47, "lon":14.18, "weather_type":"WeatherCity", "status":"active"}
  def decode(line: String) = {

    var result: String = null;
    try {

      val js = new JsonObject(line)
      val lat: String = js.getString("lon")
      val lon: String = js.getString("lat")

      val location = new JsonObject()
      location.put("lat",lat)
      location.put("lon",lon)
      js.put("location",location)

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
    val dateString0: String = dateString.replace('T', ' ')
    val dateString1: String = dateString0.replace("Z", "")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val date: Date = dateFormat.parse(dateString1)
    val timestamp: Long = date.getTime
    timestamp
  }


  def main0(args: Array[String]): Unit = {
    val s = "2021-05-002T06:12:13.487Z"
    val ts: Long = getTimestamp(s)
  }

}
