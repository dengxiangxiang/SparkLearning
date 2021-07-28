package com.dxx.analysis

import java.text.SimpleDateFormat
import java.util.Date

import com.dxx.model.AccessToken
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark
import org.json.JSONObject

import scala.collection.mutable

object AmsLogMain {
  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("ams-log-analysis")
      .getOrCreate()

    import ss.implicits._

    val lineDS: Dataset[String] = ss.read.textFile("/home/tnuser/logs/AMS/localhost_access_log.2021-05-01.txt")
//val lineDS: Dataset[String] = ss.read.textFile("/home/tnuser/logs/AMS/localhost_access_log.sample")
     val jsonRDD: Dataset[String] = lineDS
       .map(getJsonString)
       .filter(_!=null)


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

    EsSpark.saveJsonToEs(jsonRDD.rdd, "ams-analysis1/_doc", esConf)

  }

  //10.191.100.145 - - [23/Apr/2021:00:00:00 -0700] "GET /ams/monitor/ping HTTP/1.1" 200 65 0
  //10.191.100.170 - - [23/Apr/2021:00:00:00 -0700] "GET /ams/v1/validate/json?api_key=key&api_signature=sig&access_token=token HTTP/1.1" 200 267 1
  //10.191.100.170 - - [23/Apr/2021:00:00:00 -0700] "GET /ams/v1/validate/json?api_key=key&api_signature=sig&access_token= HTTP/1.1" 200 267 1
  //10.191.100.145 - - [23/Apr/2021:03:11:08 -0700] "GET /ams/v1/validate/user?secure_token=v001:6OLJT434RI1ND0X90OXZILB3U/2FABF619LZUMNX9LN5XRZGW6K:EI7T584NPI57OLL7VNYTRB0KT:1694884850600:d4931ca94f9ba090eec8d7531700390d HTTP/1.1" 200 541 1

  def getJsonString(log: String): String = {
    try{


    val jo = new JSONObject()
    val index1: Int = log.indexOf(' ')
    val clientIp: String = log.substring(0, index1)
    jo.put( "clientIp" , clientIp)


    val index2: Int = log.indexOf('[')
    val index3: Int = log.indexOf(']')
    val dateString: String = log.substring(index2 + 1, index3)
    val timestamp: Long = getTimestamp(dateString)
    jo.put( "@timestamp" , timestamp)

    val index4: Int = log.indexOf('\"')
    val index5: Int = log.indexOf('\"',index4 + 1 )
    val httpRequest: String = log.substring(index4+1, index5)
    val requestArr: Array[String] = httpRequest.split(' ')
    val method: String = requestArr(0)
    val url: String = requestArr(1)
    val httpVersion: String = requestArr(2)
    val urlArr: Array[String] = url.split('?')
    val apiPath: String = urlArr(0)
    jo.put( "api_path" , apiPath)
    if (urlArr.length > 1) {
      val queryParameters: String = urlArr(1)
      val queryParas: Array[String] = queryParameters.split('&')
      val tuples: mutable.IndexedSeq[(String, String)] =
        queryParas
          .seq
          .map(_.split('='))
          .filter(_.length==2)
          .map(arr => (arr(0), arr(1)))

      val queryParaKVs: Map[String, String] = tuples.toMap[String, String]

      val apiKey: Option[String] = queryParaKVs.get("api_key")
      jo.put( "api_key" , apiKey.getOrElse(null))

      val signature: Option[String] = queryParaKVs.get("api_signature")
      jo.put( "api_signature" , signature.getOrElse(null))

      val accessTokenString: Option[String] = queryParaKVs.get("access_token")
      jo.put( "accessToken" , accessTokenString.getOrElse(null))
      if (accessTokenString.isDefined) {
        val userName: String = AccessToken.fetchUserName(accessTokenString.get)
        jo.put( "access_token_user",userName)
      }

      val secureTokenString: Option[String] = queryParaKVs.get("secure_token")
      jo.put( "secure_token" , secureTokenString.getOrElse(null))

    }

    val lastString: String = log.substring(index5 + 2)
    val arr: Array[String] = lastString.split(" ")
    val statusCode: Int = arr(0).toInt
    jo.put("status_code" ,statusCode)

    val responseSize: Int = arr(1).toInt
    jo.put("response_size",responseSize)
    val duration: Int = arr(2).toInt
    jo.put("duration",duration)

    jo.toString
    }catch {
      case e:Exception=>{
        println(log)
        println(e)
        null
      }
    }

  }


  // format: 23/Apr/2021:00:00:00 -0700
  def getTimestamp(dateString: String): Long = {
    val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss ZZZ")
    val date: Date = dateFormat.parse(dateString)
    val timestamp: Long = date.getTime
    timestamp
  }


}
