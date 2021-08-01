package com.dxx.weather

import java.text.SimpleDateFormat
import java.util.Date

import com.dxx.weather.WeatherToESMain.getTimestamp
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object WeatherToMysqlMain {

  case class Notification(awsRegion: String, eventTime: String, eventName: String, directory: String, objectName: String)

  def main(args: Array[String]): Unit = {
    val sc: SparkSession = SparkSession.builder()
      .appName(WeatherToESMain.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val lineDS: Dataset[String] = sc.read.textFile("/Users/xxdeng/Documents/Work/WeatherNotification/csv/secondbatch/xaa.csv")
    import sc.implicits._


    val notificationDS: Dataset[Notification] = lineDS
      .map(decode(_))
      .filter(_ != null)


    val df: DataFrame = notificationDS.toDF()
    df.printSchema()

    df
      .write
      .format("jdbc")
      .mode(SaveMode.Append)
      //      .option("url", "jdbc:mysql://localhost:3306/weathernotification")
      //      .option("dbtable", "briefdata")
      //      .option("driver", "com.mysql.jdbc.Driver")
      //      .option("user", "root")
      //      .option("password", "root")
      .option("url", "jdbc:mysql://10.189.200.144:3306/umstool")
      .option("dbtable", "briefdata")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "umstool")
      .option("password", "rdyh45td")
      .save()
  }

  // format:
  // "us-east-1","2021-05-05T07:38:00.110Z","ObjectCreated:Put","publish/HourlyForecast/c73fnmry1apcccwn2k7aqzq1t.json.gz"
  def decode(line: String): Notification = {

    try {

      val array: Array[String] = line.replace("\"", "").split(",")
      val awsRegion: String = array(0)
      val eventTime0: String = array(1)

      val eventName: String = array(2)
      val objectKey: String = array(3)

      val index0: Int = objectKey.lastIndexOf('/')
      val directory: String = objectKey.substring(0, index0)
      val objectName: String = objectKey.substring(index0 + 1)

      val eventTimestamp: Long = getTimestamp(eventTime0)

      val formatedTime: String = getTimeString(eventTimestamp)

      Notification(awsRegion, formatedTime, eventName, directory, objectName)

    } catch {
      case e: Exception => {
        println(e)
        null
      }
    }


  }

  def getTimeString(timestamp: Long) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val timeString: String = dateFormat.format(new Date((timestamp)))
    timeString
  }

}
