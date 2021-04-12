package SaveReceiptRequest

import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import utils.UserUtils.getDaysByYearMonth

object SaveReceiptRequestETLMonthly {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Error: Invalid number of arguments")
      println("main method should have 3 params: <inputDir> <outputDir> <isLocalRunning>")
      println("<isLocalRunning> true or false")
      System.exit(0)
    }

    val inputDir = args(0)
    val outputDir = args(1)
    val isLocalRunning = args(2).toBoolean


    // build spark session
    val builder = SparkSession
      .builder()
      .appName(this.getClass.getName)

    if (isLocalRunning) {
      builder.master("local[*]")
    }

    val spark = builder.getOrCreate()

    monthlyMergeSaveReceiptRequest(spark, inputDir, outputDir)

  }

  def monthlyMergeSaveReceiptRequest(spark: SparkSession, inputDir: String, outputDir: String): Unit = {
    import spark.implicits._


    val lineDS: Dataset[String] = spark.read.textFile(inputDir)
    val receiptRequestInfoDS: Dataset[ReceiptRequestInfo] = lineDS.map {
      case line: String => {
        //        case class ReceiptRequestInfo(requestTime:Long,
        //////                                      requestTimeString:String,
        //////                                      applicationId: String,
        //////                                      secureToken: String,
        //////                                      deviceUid: String,
        //////                                      osName: String,
        //////                                      paymentProcessor: String,
        //////                                      offerExpiryUtcTimeStamp: Long,
        //////                                      purchaseTimeStamp: Long,
        //////                                      sku: String,
        //////                                      offerCode: String
        //////                                     )
        val arr: Array[String] = line.split(",")
        if (arr.length == 11) {
          ReceiptRequestInfo(
            arr(0).toLong,
            arr(1),
            arr(2),
            arr(3),
            arr(4),
            arr(5),
            arr(6),
            arr(7).toLong,
            arr(8).toLong,
            arr(9),
            arr(10)
          )
        } else {
          println("Format Warn: " + line)
          null
        }
      }
    }.filter(_ != null)


    val sortedDS: Dataset[ReceiptRequestInfo] = receiptRequestInfoDS.orderBy(col("requestTime"))

    val oneColumnDF: DataFrame = sortedDS.select(concat_ws(",",
      col("requestTime"),
      col("requestTimeString"),
      col("applicationId"),
      col("secureToken"),
      col("deviceUid"),
      col("osName"),
      col("paymentProcessor"),
      col("offerExpiryUtcTimeStamp"),
      col("purchaseTimeStamp"),
      col("sku"),
      col("offerCode")))

    oneColumnDF.coalesce(1)
      .write
      .format("text")
      .mode(SaveMode.Overwrite)
      .save(outputDir)

  }
}
