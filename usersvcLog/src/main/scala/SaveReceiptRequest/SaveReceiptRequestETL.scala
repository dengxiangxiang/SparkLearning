package SaveReceiptRequest

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import utils.UserUtils
import com.jayway.jsonpath.{Configuration, DocumentContext, JsonPath, Option, ParseContext}
import org.apache.spark.sql.functions.{col, concat_ws}

object SaveReceiptRequestETL {

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

      val receiptRequestInfoDF: DataFrame = notEmptyLines.map(parse(_)).filter(_ != null).na.fill("not found")


//    requestTime:Long,
//    requestTimeString:String,
//    applicationId: String,
//    secureToken: String,
//    deviceUid: String,
//    osName: String,
//    paymentProcessor: String,
//    offerExpiryUtcTimeStamp: Long,
//    purchaseTimeStamp: Long,
//    sku: String,
//    offerCode: String

    val oneColumnDF: DataFrame = receiptRequestInfoDF.select(concat_ws(",",
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


    oneColumnDF
      .coalesce(1)
      .write
      .format("text")
      .mode(SaveMode.Overwrite)
      .save(outputDir+"/"+date)

  }

  def parse(log:String) :ReceiptRequestInfo={
    try{
      val index: Int = log.indexOf('{')
      val accessLog: String = log.substring(index)

      val conf: Configuration = Configuration.defaultConfiguration.addOptions(Option.SUPPRESS_EXCEPTIONS)
      val parseContext: ParseContext = JsonPath.using(conf)
      val context: DocumentContext = parseContext.parse(accessLog)

      val uri: String = context.read[String]("uri")
      val method: String = context.read[String]("method")

      if(!uri.contains("/v6/receipt") || !method.equals("POST")){
        return null;
      }


      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      dateFormat.setTimeZone(TimeZone.getTimeZone("GMT+0:00"))

      val requestTime =   context.read[Number]("request-time").longValue()
      val requestTimeString: String = dateFormat.format(new Date(requestTime))

      val applicationId: String = context.read[String]("request.application_id")
      val secureToken: String = context.read[String]("request.secure_token")
      val deviceUid: String = context.read[String]("request.device_info.device_uid")
      val osName: String = context.read[String]("request.device_info.os_name")

      val paymentProcessor: String = context.read[String]("request.receipt.payment_processor")
      val offerExpiryUtcTimeStamp = context.read[Number]("request.receipt.offer_expiry_utc_timestamp").longValue()
      val purchaseTimeStamp = context.read[Number]("request.receipt.purchase_utc_timestamp").longValue()
      val sku: String = context.read[String]("request.receipt.sku")
      val offerCode: String = context.read[String]("request.receipt.offer_code")

      ReceiptRequestInfo(requestTime,requestTimeString,applicationId,secureToken,deviceUid,osName,paymentProcessor,offerExpiryUtcTimeStamp,purchaseTimeStamp,sku,offerCode)
    }catch {
      case e: Exception => {
        e.printStackTrace()
        println(log)
        null
      }
    }


  }



}
