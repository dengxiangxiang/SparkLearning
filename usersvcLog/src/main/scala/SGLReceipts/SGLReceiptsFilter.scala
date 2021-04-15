package SGLReceipts

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import utils.UserUtils

object SGLReceiptsFilter {
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

    var inputFiles: Array[String] = null
    if (isLocalFile) {
      inputFiles = UserUtils.getLocalFiles(inputDir, date)
    } else {
      inputFiles = UserUtils.getS3Files(inputDir, date)
    }

    val originLines: Dataset[String] = spark.read.textFile(inputFiles: _*)
    val notEmptyLines = originLines.filter(line => line != null && line.length != 0)

    val sglSaveReceiptRDD: Dataset[String] = notEmptyLines.filter {
      line => {

        val isSaveReceipt = line.contains("/v6/receipt") && line.contains("POST")
        val isSGLOneYear = line.contains("1yearpaid") || line.contains("APPLE_APPSTORE") || line.contains("GOOGLE_PLAY")
        isSaveReceipt && isSGLOneYear
      }
    }
    sglSaveReceiptRDD
      .coalesce(1)
      .write
      .format("text")
      .mode(SaveMode.Overwrite)
      .save(outputDir+"/"+date)

  }

}
