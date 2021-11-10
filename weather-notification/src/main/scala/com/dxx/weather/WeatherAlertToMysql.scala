package com.dxx.weather

import json.JsonObject
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object WeatherAlertToMysql {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(WeatherAlertToMysql.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._


    val tmpDir = "/Users/xxdeng/Documents/Work/weatherAlert/SirisProcessLog/WeatherAlert.log.backup";

    val df: Dataset[SirisWeatherAlert] = spark.read.textFile(tmpDir)
      .filter(StringUtils.isNotBlank(_))
      .map(weatherAlertConvert)

    df.printSchema()


    df.write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("url", "jdbc:mysql://10.189.200.144:3306/umstool?rewriteBatchedStatements=true")
      .option("dbtable", "sxm_weatheralert")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "umstool")
      .option("password", "rdyh45td")
      .option("batchsize", 100)
      .option("isolationLevel", "NONE")
      .save()


  }

  case class SirisWeatherAlert(
                                name: String,
                                activeAt: String,
                                expireAt: String,
                                phenomenonText: String,
                                reportText: String,
                                significanceText: String,
                                color: String,
                                instructions: String,
                                typee: String,
                                _id: String,
                                _status: String,
                                lastUpdated: String
                              )

  def weatherAlertConvert(json: String): SirisWeatherAlert = { //        {
    //            "name": "EXCESSIVE HEAT WARNING IN EFFECT FROM 10 AM PDT /10 AM MST/ SUNDAY TO 8 PM PDT /8 PM MST/ MONDAY",
    //             "geo": {
    //                    "coordinates": [
    //                    [
    //                        [-114.43619537399996, 34.07971191400003],
    //                        [-114.57909393299997, 34.080013275000056],
    //                        [-114.49079894999994, 34.33901214600007],
    //                        [-114.22929382299998, 34.18721389800004],
    //                        [-114.43619537399996, 34.07971191400003]
    //                    ]
    //                ],
    //                    "type": "Polygon"
    //                },
    //            "activeAt": "2021-09-12T17:00:00.000Z",
    //                "expireAt": "2021-09-14T03:00:00.000Z",
    //                "icaoSite": "KVEF",
    //                "phenomenon": "EH",
    //                "phenomenonText": "Excessive Heat",
    //                "reportText": "* WHAT...Dangerously hot conditions with temperatures 110 to 117 n  expected.nn* WHERE...In Arizona, Lake Havasu and Fort Mohave and Lake Meadn  National Recreation Area. In California, San Bernardino County-n  Upper Colorado River Valley. In Nevada, Lake Mead National n  Recreation Area.nn* WHEN...From 10 AM PDT /10 AM MST/ Sunday to 8 PM PDT /8 PM n  MST/ Monday.nn* IMPACTS...Extreme heat will significantly increase the n  potential for heat related illnesses, particularly for those n  working or participating in outdoor activities.",
    //                "significance": "W",
    //                "significanceText": "Warning",
    //                "color": "#C71585",
    //                "instructions": "Drink plenty of fluids, stay in an air-conditioned room, stay outnof the sun, and check up on relatives and neighbors. Youngnchildren and pets should never be left unattended in vehiclesnunder any circumstances.nnTake extra precautions if you work or spend time outside. Whennpossible reschedule strenuous activities to early morning ornevening. Know the signs and symptoms of heat exhaustion and heatnstroke. Wear lightweight and loose fitting clothing whennpossible. To reduce risk during outdoor work, the OccupationalnSafety and Health Administration recommends scheduling frequentnrest breaks in shaded or air conditioned environments. Anyonenovercome by heat should be moved to a cool and shaded location.nHeat stroke is an emergency! Call 9 1 1.",
    //                "type": "WeatherAlert",
    //                "_id": "7jsho7ml704oixwh7n5eoccu9",
    //                "_status": "active",
    //                "lastUpdated": "2021-09-13T16:56:12.335Z"
    //        }

    val array: Array[String] = json.split("\\[WeatherAlertLog\\] - ")
    val alertJs = array(1)

    val sirisJson = new JsonObject(alertJs)
    val name = sirisJson.getString("name")
    val _status = sirisJson.getString("_status")
    val _id = sirisJson.getString("_id")

    val phenomenonText = sirisJson.getString("phenomenonText")
    val typee = sirisJson.getString("type")
    val activeAt = sirisJson.getString("activeAt")
    val expireAt = sirisJson.getString("expireAt")
    val lastUpdated = sirisJson.getString("lastUpdated")

    val instructions = sirisJson.getString("instructions")
    val significanceText = sirisJson.getString("significanceText")
    val color = sirisJson.getString("color")
    val reportText = sirisJson.getString("reportText")

    SirisWeatherAlert(name = name, activeAt = activeAt, expireAt = expireAt, phenomenonText = phenomenonText, reportText = reportText, significanceText = significanceText, color = color, instructions = instructions, typee = typee, _id = _id, _status = _status, lastUpdated = lastUpdated)
  }
}
