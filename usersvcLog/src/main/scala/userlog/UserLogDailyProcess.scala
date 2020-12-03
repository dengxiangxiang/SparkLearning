package userlog


import common.LogProcessor
import common.entity.ProcessResult
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import utils.{Result, UserUtils}

object UserLogDailyProcess {
  def main(args: Array[String]): Unit = {

    if (args.length != 5) {
      println("Error: Invalid number of arguments")
      println("main method should have 5 params: <inputDir> <outputDir> <date> <isLocalFile> <isLocalRunning>")
      println("<date> example: 2020-09-01, <isLocalFile>: true or false, <isLocalRunning> true or false")
      System.exit(0)
    }

    val inputDir = args(0)
    val outputDir = args(1)
    val dateList = args(2).split(",")
    val isLocalFile = args(3).toBoolean
    val isLocalRunning = args(4).toBoolean


    // build spark session
    val builder = SparkSession
      .builder()
      .appName(this.getClass.getName)

    if (isLocalRunning) {
      builder.master("local[*]")
    }

    val spark = builder.getOrCreate()

    dateList.foreach{
      case date:String=>dailyProcess(spark,inputDir,outputDir,date,isLocalFile)
    }

  }

  def dailyProcess(spark:SparkSession,
                   inputDir:String,
                   outputDir:String,
                   date:String,
                   isLocalFile:Boolean):Unit={
    // get log files

    import spark.implicits._

    var inputFiles: Array[String] = null
    if (isLocalFile) {
      inputFiles = UserUtils.getLocalFiles(inputDir, date)
    } else {
      inputFiles = UserUtils.getS3Files(inputDir, date)
    }

    val originLines: Dataset[String] = spark.read.textFile(inputFiles: _*)
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
    resultDS.cache()


    val resultDF: DataFrame = resultDS.toDF().na.fill("null")
    resultDF.printSchema()

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

    //format: get 202009-01 from 2020-09-01
    val dateString: String = date.replaceFirst("-", "")


    //========================= for dailyBriefData ======================================

    val countDF: DataFrame = spark.sql(
      "select " +
        "userId,applicationId,credentialType,credentialKey,deviceId, count(1) as count " +
        "from resultView " +
        "group by userId,applicationId,credentialType,credentialKey,deviceId " +
        "order by count desc")


    val res1: DataFrame = countDF.withColumn("date", lit(dateString)).select(concat_ws(",",
      col("date"),
      col("applicationId"),
      col("userId"),
      col("credentialType"),
      col("credentialKey"),
      col("deviceId"),
      col("count")))
    res1.printSchema()

    res1.coalesce(1).write.format("text").mode(SaveMode.Overwrite).save(outputDir + "/" + UserUtils.getDailyBriefDataOutputDir(date))

    //========================= for dailyRegisterData ========================================

    val registerCountDF: DataFrame = spark.sql(
      "select " +
        "userId,applicationId,credentialType,credentialKey,deviceId,statusCode,count(1) as count " +
        "from resultView " +
        "where requestName like '%/register%' " +
        "group by userId,applicationId,credentialType,credentialKey,deviceId,statusCode " +
        "order by count desc")


    val resRegister: DataFrame = registerCountDF.withColumn("date", lit(dateString)).select(concat_ws(",",
      col("date"),
      col("applicationId"),
      col("userId"),
      col("credentialType"),
      col("credentialKey"),
      col("deviceId"),
      col("statusCode"),
      col("count")))
    resRegister.printSchema()

    resRegister.coalesce(1).write.format("text").mode(SaveMode.Overwrite).save(outputDir + "/" + UserUtils.getDailyRegisterDataOutputDir(date))
  }


}

