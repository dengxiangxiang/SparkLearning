package com.dxx.weather

import java.text.SimpleDateFormat
import java.util.Date

import json.{JsonArray, JsonObject}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.mutable.ListBuffer

object WeatherToESMain {

  def main(args: Array[String]): Unit = {
    val sc: SparkSession = SparkSession.builder()
      .appName(WeatherToESMain.getClass.getName)
      .master("local[12]")
      .getOrCreate()
    //    val lineDS: Dataset[String] = sc.read.textFile("/Users/xxdeng/Documents/study/sparkParent/weather-notification/src/main/resources/test.txt")
    val lineDS: Dataset[String] = sc.read.textFile("/Users/xxdeng/Documents/Work/WeatherNotification/csv/secondbatch/xa*.csv")
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

    EsSpark.saveJsonToEs(recordStringDS.rdd, "weather-notification/_doc", esConf)
  }


  // format:
  // "us-east-1","2021-05-05T07:38:00.110Z","ObjectCreated:Put","publish/HourlyForecast/c73fnmry1apcccwn2k7aqzq1t.json.gz"
  def decode(line: String) = {

    var result: String = null;
    try {

      val array: Array[String] = line.replace("\"", "").split(",")
      val awsRegion: String = array(0)
      val eventTime: String = array(1)
      val eventName: String = array(2)
      val objectKey: String = array(3)

      val index0: Int = objectKey.lastIndexOf('/')
      val directory: String = objectKey.substring(0, index0)
      val objectName: String = objectKey.substring(index0 + 1)

      val eventTimestamp: Long = getTimestamp(eventTime)

      val js = new JsonObject()
      js.put("awsRegion", awsRegion)
      js.put("eventTime", eventTime)
      js.put("eventName", eventName)
      js.put("@timestamp", Long.box(eventTimestamp))
      js.put("directory", directory)
      js.put("objectName", objectName)

      result = js.toString


    } catch {
      case e: Exception => {
        println(e)
      }
    }

    result

  }

  @deprecated
  def decode0(line: String) = {

    var list = ListBuffer[String]()

    try {

      val js = new JsonObject(line)
      val messageString: String = js.getString("Message")

      val messageJsonObject = new JsonObject(messageString)
      val recordsJsonArray: JsonArray = messageJsonObject.getJsonArray("Records")

      val length: Int = recordsJsonArray.length()

      // do flatmap
      for (i <- 0 until recordsJsonArray.length) {

        val record: JsonObject = recordsJsonArray.getJsonObject(i)

        {
          val s3JsonObj: JsonObject = record.getJsonObject("s3")
          val objectJsonObj: JsonObject = s3JsonObj.getJsonObject("object")

          //format: publish/HourlyForecast/evbhda9x4ftixi2474lw6b7an.json.gz
          val keyString: String = objectJsonObj.getString("key")
          val index: Int = keyString.lastIndexOf('/')
          val dir: String = keyString.substring(0, index)
          record.put("dir", dir)
        }

        val eventTimeString: String = record.getString("eventTime")
        val eventTimestamp: Long = getTimestamp(eventTimeString)
        record.put("@timestamp", Long.box(eventTimestamp))

        val notificationTime: String = js.getString("Timestamp")
        val notificationTimestamp: Long = getTimestamp(notificationTime)

        val notificationDelay = notificationTimestamp - eventTimestamp
        record.put("notificationDelay", Long.box(notificationDelay))

        println(record.toString)

        list += record.toString
      }
    } catch {
      case e: Exception => {
        val cause: Throwable = e.getCause
        e.printStackTrace()
        println(e)
      }
    }

    list

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
