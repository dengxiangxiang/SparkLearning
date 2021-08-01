package com.dxx.weather


import json.JsonObject
import org.apache.commons.lang.StringUtils
import org.apache.parquet.format.event.Consumers.Consumer

import java.io.{BufferedReader, FileInputStream, FileReader}
import java.text.SimpleDateFormat
import java.util
import java.util.{Comparator, Date}

object WeatherRadarNotification {
  //2021-07-07 18:35:23.941
  //"2021-07-04 20:06:47.371"
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def main(args: Array[String]): Unit = {
    val file = "/Users/xxdeng/Documents/Work/GeotiffDemo/target/classes/weatherradarupdates.log"

    val bufferedReader = new BufferedReader(new FileReader(file))

    val dateList = new util.ArrayList[Date]()

    var line = bufferedReader.readLine()
    while (!StringUtils.isBlank(line)) {
      //
      dateList.add(getDate(line))
      line = bufferedReader.readLine();
    }

    dateList.sort(new Comparator[Date] {
      override def compare(o1: Date, o2: Date): Int = {
        (o1.getTime - o2.getTime).toInt
      }
    })

    dateList.forEach(new java.util.function.Consumer[Date]() {
      override def accept(t: Date): Unit = {
        println(dateFormat.format(t))
      }
    })

  }


  //"Message" : "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\",\"eventTime\":\"2021-07-05T07:26:50.032Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:AROAJCAYALGS4YPQSBBSM:i-0ba771459554877ae\"},\"requestParameters\":{\"sourceIPAddress\":\"54.86.111.129\"},\"responseElements\":{\"x-amz-request-id\":\"W17QTTT3CWMVZNBH\",\"x-amz-id-2\":\"jFjFTv45K5wDEN3I7YNIzoKBWrkzfqMgOynoiNrb50RoJN57evCkrNe2HfRtVzTWXJWEfxqk4lOofuvhEDPhcN4q9WVy5NQu\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"sxm-isa-publish\",\"bucket\":{\"name\":\"sxm-isa-publish-telenav\",\"ownerIdentity\":{\"principalId\":\"A1UOLTCDS0N6CT\"},\"arn\":\"arn:aws:s3:::sxm-isa-publish-telenav\"},\"object\":{\"key\":\"publish/WeatherRadar/radar.tif\",\"size\":170245883,\"eTag\":\"b5a4ec5f08344de46a0862030aefdb9a\",\"sequencer\":\"0060E2B4363A5BBFA9\"}}}]}",
  def getDate(line: String): Date = {

    val line1 = "{"+line+"}"

    val jsonObject = new JsonObject(line1)
    val messageString = jsonObject.getString("Message")
    val messObj = new JsonObject(messageString)
    val timeString = messObj.getJsonArray("Records").getJsonObject(0).getString("eventTime")
    val timeString1 = timeString.replace("T", " ").replace("Z", "")
    val date = dateFormat.parse(timeString1)
    date

  }
}
