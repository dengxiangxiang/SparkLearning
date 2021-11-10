package com.dxx.weather

import json.JsonObject
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.spark.rdd.EsSpark
import util.ESClient

import java.text.SimpleDateFormat
import java.util.Date

object WeatherAlertToES {
  def main(args: Array[String]): Unit = {
    val sc: SparkSession = SparkSession.builder()
      .appName(WeatherToESMain.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val lineDS: Dataset[String] = sc.read.textFile("/Users/xxdeng/Documents/Work/weatherAlert/SirisProcessLog/WeatherAlert.log.backup")
    import sc.implicits._

    val recordStringDS: Dataset[(String, String, String)] =
      lineDS
        .filter(!StringUtils.isBlank(_))
        .map(decode(_))
        .filter(_._3 != null)


    //    val esConf: Map[String, String] = Map(
    //      "es.http.timeout" -> "5m",
    ////      "es.http.retries" -> "50",
    //      "es.nodes.wan.only" -> "true",
    //      "es.nodes.discovery" -> "false",
    ////      "es.batch.size.bytes" -> "10mb",
    ////      "es.batch.write.retry.wait" -> "120s",
    ////      "es.batch.write.retry.count" -> "3",
    //      "es.index.auto.create" -> "true",
    //      "es.nodes" -> "myPersonalMac",
    //      "es.port" -> "9200")
    //
    //    EsSpark.saveJsonToEs(recordStringDS.rdd, "weather-alert/_doc", esConf)

    recordStringDS.foreachPartition(ite => {
      val client = ESClient.restClient()
      ite.foreach(s => {
        try {

          val status = s._2
          if (status.equals("active")) {
            val request = new IndexRequest("weather-alert-unique");
            request.id(s._1).source(s._3, XContentType.JSON);

            client.index(request, RequestOptions.DEFAULT)
          }

        } catch {
          case e: Exception => {
            println("id: " + s._1 + "- -" + e.getMessage)
            println("source:"+s._3)
          }
        }
      })

      client.close()
    })
  }


  // format:
  // 2021-09-15 00:22:19.575 [WeatherAlert--0] [INFO ] [WeatherAlertLog] - {"name":"SMALL CRAFT ADVISORY REMAINS IN EFFECT UNTIL 3 AM PDT MONDAY... ...GALE WATCH REMAINS IN EFFECT FROM LATE SUNDAY NIGHT THROUGH MONDAY EVENING","geo":{"coordinates":[[[[-125.80942480099998,40.46001998400004],[-125.77658479099995,40.66216944300004],[-125.70427311699996,40.84988301100003],[-125.62118687499998,40.97963501700008],[-125.55054488599995,41.29142022900004],[-125.58762878699997,41.46327490500005],[-125.68044891999995,41.67579037100006],[-125.79359836099997,41.77765977700005],[-124.493200334,41.781580731000076],[-124.46152132799995,41.68946466700004],[-124.36703114899996,41.62214790200005],[-124.30089434899998,41.433496071000036],[-124.34381671099999,41.24867401300003],[-124.39956129199999,41.14387003400003],[-124.36509925399997,40.96512114300003],[-124.60223132399994,40.57483861900005],[-124.64182930399994,40.44336731000004],[-124.64196815999998,40.44115309700004],[-125.80942480099998,40.46001998400004]]]],"type":"MultiPolygon"},"activeAt":"2021-09-13T10:00:00.000Z","expireAt":"2021-09-14T07:00:00.000Z","icaoSite":"KEKA","phenomenon":"GL","phenomenonText":"Gale","reportText":"* WHAT...For the Small Craft Advisory, north winds 20 to 30 kt \n  with gusts up to 35 kt and seas 6 to 11 feet. For the Gale \n  Watch, north winds 25 to 35 kt with gusts up to 40 kt and seas \n  10 to 13 feet possible.\n\n* WHERE...Pt St George to Cape Mendocino 10 to 60 nm.\n\n* WHEN...For the Small Craft Advisory, until 3 AM PDT Monday. \n  For the Gale Watch, from late Sunday night through Monday \n  evening.\n\n* IMPACTS...Strong winds can cause hazardous seas which could \n  capsize or damage vessels and reduce visibility.","significance":"A","significanceText":"Watch","color":"#FFC0CB","instructions":"Mariners should consider altering plans to avoid possible\nhazardous conditions.  Remain in port, seek safe harbor, alter\ncourse, and/or secure the vessel for severe wind and seas.","type":"WeatherAlert","_id":"3p2zqvv9u8s9b5e607b60suu5","_status":"active","lastUpdated":"2021-09-13T16:56:12.497Z"}
  def decode(line: String): (String, String, String) = {
    var result: String = null;
    var id: String = null;
    var status: String = null;
    try {
      val array: Array[String] = line.split("\\[WeatherAlertLog\\] - ")
      val alertJs = array(1)

      val jsonObject = new JsonObject(alertJs)

      id = jsonObject.getString("_id")
      jsonObject.remove("_id")
      jsonObject.put("alertId", id)

      status = jsonObject.getString("_status")
      jsonObject.remove("_status")
      jsonObject.put("status", status)

      result = jsonObject.toString

    } catch {
      case e: Exception => {
        println(e)
      }
    }

    (id, status, result)
  }

  // format: 2021-04-29T19:40:45.715Z
  def getTimestamp(dateString: String): Long = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date: Date = dateFormat.parse(dateString)
    val timestamp: Long = date.getTime
    timestamp
  }
}
