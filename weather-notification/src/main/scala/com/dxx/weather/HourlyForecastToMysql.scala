package com.dxx.weather

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object HourlyForecastToMysql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(HourlyForecastToMysql.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

//    val file = "/Users/xxdeng/Documents/Work/hourlyforecast/HourlyForecast.log.2021-08-10.backup.gz";
//
//    //2021-08-11 00:00:00.094 [HourlyForecast-4] [INFO ] [HourlyForecastLog] - {"temp":26,"validDateUtc":"2021-08-11T08:00:00.000Z","validDateLocal":"2021-08-11T03:00:00","skyDescription":"Mostly Clear","iconCode":"48","probabilityOfPrecip":6,"probabilityOfSnow":0,"relativeHumidity":90,"windSpeed":4,"windDirectionDegrees":211,"windDirectionCardinal":"SSW","cloudCoverage":21,"uvIndex":0,"uvDescription":"Low","weatherLocation":"eriupfgcoufwhijbv8hkro729","type":"HourlyForecast","_status":"active","_id":"bf12euh1rz2coz1q9a2uvfqet","lastUpdated":"2021-08-11T06:49:50.528Z"}
//
//    val txtDS: Dataset[String] = spark.read.textFile(file)
//
//    val hourlyBodyDS: Dataset[String] = txtDS
//      .filter(!StringUtils.isEmpty(_))
//      .map(_.split(" - ")(1))
//
//    hourlyBodyDS.take(2).foreach(println)

    val tmpDir = "/Users/xxdeng/Documents/Work/hourlyforecast/tmpoutput/";
//
//    hourlyBodyDS
//      .write
//      .format("text")
//      .mode(SaveMode.Overwrite)
//      .save(tmpDir)

    val df: DataFrame = spark.read.json(tmpDir)
    df.printSchema()

    //    root
    //    |-- _id: string (nullable = true)
    //    |-- _status: string (nullable = true)
    //    |-- cloudCoverage: long (nullable = true)
    //    |-- iconCode: string (nullable = true)
    //    |-- lastUpdated: string (nullable = true)
    //    |-- probabilityOfPrecip: long (nullable = true)
    //    |-- probabilityOfSnow: double (nullable = true)
    //    |-- relativeHumidity: long (nullable = true)
    //    |-- skyDescription: string (nullable = true)
    //    |-- temp: long (nullable = true)
    //    |-- type: string (nullable = true)
    //    |-- uvDescription: string (nullable = true)
    //    |-- uvIndex: long (nullable = true)
    //    |-- validDateLocal: string (nullable = true)
    //    |-- validDateUtc: string (nullable = true)
    //    |-- weatherLocation: string (nullable = true)
    //    |-- windDirectionCardinal: string (nullable = true)
    //    |-- windDirectionDegrees: long (nullable = true)
    //    |-- windSpeed: double (nullable = true)

    val targetDF = df.select(
      "_id",
      "_status",
      "iconCode",
      "lastUpdated",
      "skyDescription",
      "temp",
      "type",
      "validDateLocal",
      "validDateUtc",
      "weatherLocation"
    )

    targetDF.printSchema()

    targetDF.write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("url", "jdbc:mysql://10.189.200.144:3306/umstool?rewriteBatchedStatements=true")
      .option("dbtable", "sxm_hourlyforecast_08")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "umstool")
      .option("password", "rdyh45td")
      .option("batchsize", 1000)
      .option("isolationLevel", "NONE")
      .save()


  }

}
