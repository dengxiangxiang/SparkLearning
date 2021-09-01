package userlog

import common.LogProcessor
import common.entity.ProcessResult
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import utils.{Result, UserUtils}

object UserLogBriefTotal {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("please specify 2 param to the app, inputDir and output dir!")
      System.exit(0)
    }

    val inputDir = args(0)
    val outputDir = args(1)

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName(UserLogBriefTotal.getClass.getName)
      .getOrCreate()

    import spark.implicits._

    val originLines: Dataset[String] = spark.read.textFile(inputDir)

    val notEmptyLines = originLines.filter(line => line != null && line.length != 0)

    val resultDS: Dataset[Result] = notEmptyLines.map {
      case line => {
        try {
          var processResult: ProcessResult = null;
          if (line.contains("\"message\":\"[")) {
            // in this case, its kafka log from filebeats
            processResult = LogProcessor.processKafkaMsg(line)
          } else {
            // in this case, its origin log from log files
            processResult = LogProcessor.processUserServiceLog(line)
          }

          Result(
            processResult.getLogTime,
            processResult.getUserId,
            processResult.getApplicationId,
            processResult.getCredentialType,
            processResult.getCredentialKey,
            processResult.getDeviceId,
            processResult.getRequestName,
            processResult.getStatusCode)
        } catch {
          case e: Exception => null
        }
      }
    }.filter(_ != null)


    val resultDF: DataFrame = resultDS.toDF().na.fill("null")
    resultDF.cache()

    //    root
    //    |-- time: long (nullable = true)
    //    |-- userId: string (nullable = true)
    //    |-- applicationId: string (nullable = true)
    //    |-- credentialType: string (nullable = true)
    //    |-- credentialKey: string (nullable = true)
    //    |-- deviceId: string (nullable = true)
    //    |-- requestName: string (nullable = true)
    //    |-- statusCode: string (nullable = true)

    resultDF.createOrReplaceTempView("resultView")


    //========================= for dailyBriefData ======================================

    val countDF: DataFrame = spark.sql(
      "select " +
        "userId,applicationId,credentialType,credentialKey,deviceId, count(1) as count " +
        "from resultView " +
        "group by userId,applicationId,credentialType,credentialKey,deviceId " +
        "order by count desc")


    val res1: DataFrame = countDF.select(concat_ws(",",
      col("applicationId"),
      col("userId"),
      col("credentialType"),
      col("credentialKey"),
      col("deviceId"),
      col("count")))

    res1.coalesce(1).write.format("text").mode(SaveMode.Overwrite).save(outputDir + "/" + "userClick.txt")

    val appIdCountDF: DataFrame = spark.sql(
      "select " +
        "applicationId, count(distinct deviceId) as deviceCount " +
        "from resultView " +
        "group by applicationId " +
        "order by deviceCount desc"
    )

    val res2DF: DataFrame = appIdCountDF.select(concat_ws(",",
      col("applicationId"),
      col("deviceCount")))


    res2DF.coalesce(1).write.format("text").mode(SaveMode.Overwrite).save(outputDir + "/" + "appIdCount.txt")

    Thread.sleep(1000 * 60 * 10)


  }

}
