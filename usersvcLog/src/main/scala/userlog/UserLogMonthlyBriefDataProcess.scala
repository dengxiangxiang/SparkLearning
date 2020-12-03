package userlog

import org.apache.spark.sql.functions.{col, concat_ws, lit, udf}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import utils.{BriefDataResult, ProductUtils, RegisterDataResult, UserUtils}

object UserLogMonthlyBriefDataProcess {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Error: Invalid number of arguments")
      println("main method should have 4 params: <inputDir> <outputDir> <yearMonth> <isLocalRunning>")
      println("<yearMonth> example: 202009, <isLocalRunning> true or false")
      System.exit(0)
    }

    val inputDir = args(0)
    val outputDir = args(1)
    val yearMonth = args(2)
    val isLocalRunning = args(3).toBoolean


    // build spark session
    val builder = SparkSession
      .builder()
      .appName(this.getClass.getName)

    if (isLocalRunning) {
      builder.master("local[*]")
    }

    val spark = builder.getOrCreate()
    import spark.implicits._

    val products: Array[utils.Product] = ProductUtils.loadProducts
    val productDF: DataFrame = products.toSeq.toDF("applicationId", "productName", "productGroup")
    productDF.createOrReplaceTempView("productView")

    monthlyAggregateBriefData(spark, inputDir, outputDir, yearMonth)

  }

  def monthlyAggregateBriefData(spark: SparkSession, inputDir: String, outputDir: String, yearMonth: String): Unit = {
    import spark.implicits._
    //    ============================== aggregate all dailyBriefData  of a month========================================
    // 1 get daily files
    var inputFiles: Array[String] = UserUtils.getAggregatedDailyBriefDatas(inputDir, yearMonth)

    // 2 convert to DF and create tempView
    val lineDS: Dataset[String] = spark.read.textFile(inputFiles: _*)
    val briefDataResultDS: Dataset[BriefDataResult] = lineDS.map {
      case line: String => {
        //201910-06,5VHmYfwJhPSg9JflfbuJrQYTDFh1,5FK5XD62G1YD62E1SZILMK43M,SCOUT_PTN,+16304498856,58F225C2-A7EA-495F-BEB0-A6FAFCACB7DA,28
        val arr: Array[String] = line.split(",")
        if (arr.length == 7) {
          BriefDataResult(arr(1), arr(2), arr(3), arr(4), arr(5), arr(6).toInt)
        } else {
          println("Format Warn: " + line)
          null
        }
      }
    }.filter(_ != null)
    val briefDataResultDF: DataFrame = briefDataResultDS.toDF()
    briefDataResultDF.printSchema()
    briefDataResultDF.createOrReplaceTempView("monthlyDailyBriefDataView")
    //    root
    //    |-- applicationId: string (nullable = true)
    //    |-- userId: string (nullable = true)
    //    |-- credentialType: string (nullable = true)
    //    |-- credentialKey: string (nullable = true)
    //    |-- deviceId: string (nullable = true)
    //    |-- count: Int (nullable = true)


    // 3 output monthly text file
    val monthlyAggregatedDF: DataFrame = spark.sql(
      "select " +
        "applicationId,userId,credentialType,credentialKey,deviceId,sum(count) as total " +
        "from monthlyDailyBriefDataView " +
        "group by applicationId,userId,credentialType,credentialKey,deviceId " +
        "order by total desc")
    val res1: DataFrame = monthlyAggregatedDF
      .withColumn("yearMonth", lit(yearMonth))
      .select(concat_ws(",",
        col("yearMonth"),
        col("applicationId"),
        col("userId"),
        col("credentialType"),// for security , we do not save credentialKey
        col("deviceId"),
        col("total")))


    res1.coalesce(1)
      .write
      .format("text")
      .mode(SaveMode.Overwrite)
      .save(outputDir + "/" + UserUtils.getMonthlySumDailyBriefDataDir(yearMonth))

    // 4 join with Product info message

    val appIdDeviceCountDF: DataFrame = spark.sql(
      "select " +
        "applicationId,count(distinct deviceId) as deviceCount " +
        "from monthlyDailyBriefDataView " +
        "group by applicationId " +
        "order by deviceCount desc")
    appIdDeviceCountDF.createOrReplaceTempView("appIdDeviceCountView")

    val res2DF: DataFrame = spark.sql(
      "select " +
        "p.applicationId as application_id," +
        "p.productName as product_name," +
        "p.productGroup as product_group," +
        "a.deviceCount as device_count " +
        "from appIdDeviceCountView as a left join productView as p " +
        "on p.applicationId=a.applicationId " +
        "order by device_count desc"
    ).withColumn("report_month", lit(yearMonth)).na.fill("not found")

    // 5 output json file
    res2DF
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(outputDir + "/" + UserUtils.getMonthlyBriefDataJsonResultDir(yearMonth))
  }


}
