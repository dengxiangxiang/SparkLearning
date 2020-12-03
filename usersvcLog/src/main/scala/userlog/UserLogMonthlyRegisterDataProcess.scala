package userlog

import org.apache.spark.sql.functions.{col, concat_ws, lit}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import utils.{ProductUtils, RegisterDataResult, UserUtils}

object UserLogMonthlyRegisterDataProcess {
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

    monthlyAggregateRegisterData(spark, inputDir, outputDir, yearMonth)

  }


  def monthlyAggregateRegisterData(spark: SparkSession, inputDir: String, outputDir: String, yearMonth: String): Unit = {
    //    ============================== aggregate all dailyRegisterData  of a month========================================
    import spark.implicits._
    // 1 get daily files
    var inputFiles: Array[String] = UserUtils.getAggregatedDailyRegisterDatas(inputDir, yearMonth)

    // 2 convert to DF and create tempView
    val lineDS: Dataset[String] = spark.read.textFile(inputFiles: _*)
    val registerDataResultDS: Dataset[RegisterDataResult] = lineDS.map {
      case line: String => {
        //201910-06,c1761436-efe6-456c-95ac-4048d70ce829,8WULFV2MBRX2UQYMHGR5ZK88I,ANONYMOUS,70033924-BF13-4C38-BE55-9A429F92D618,45991156-D076-4E94-B9DA-0A621CF92B74,13200,1
        val arr: Array[String] = line.split(",")
        if (arr.length == 8) {
          RegisterDataResult(arr(1), arr(2), arr(3), arr(4), arr(5), arr(6).toInt, arr(7).toInt)
        } else {
          println("Format Warn: " + line)
          null
        }
      }
    }.filter(_ != null)
    val registerDataResultDF: DataFrame = registerDataResultDS.toDF()
    registerDataResultDF.printSchema()
    registerDataResultDF.createOrReplaceTempView("monthlyRegisterDataView")
    //    root
    //    |-- applicationId: string (nullable = true)
    //    |-- userId: string (nullable = true)
    //    |-- credentialType: string (nullable = true)
    //    |-- credentialKey: string (nullable = true)
    //    |-- deviceId: string (nullable = true)
    //    |-- statusCode:Int (nullable = true)
    //    |-- count: Int (nullable = true)


    // 3 monthly aggregate and output text file
    val monthlyAggregatedDF: DataFrame = spark.sql(
      "select " +
        "applicationId,userId,credentialType,credentialKey,deviceId,statusCode,sum(count) as total " +
        "from monthlyRegisterDataView " +
        "group by applicationId,userId,credentialType,credentialKey,deviceId,statusCode " +
        "order by total desc")
    val res1: DataFrame = monthlyAggregatedDF
      .withColumn("yearMonth", lit(yearMonth))
      .select(concat_ws(",",
        col("yearMonth"),
        col("applicationId"),
        col("userId"),
        col("credentialType"), // for security,we do not save credentialKey
        col("deviceId"),
        col("statusCode"),
        col("total")))


    res1.coalesce(1)
      .write
      .format("text")
      .mode(SaveMode.Overwrite)
      .save(outputDir + "/" + UserUtils.getMonthlySumDailyRegisterDataDir(yearMonth))

    // 4 join with Product info message

    val appIdDeviceCountDF: DataFrame = spark.sql(
      "select " +
        "applicationId,credentialType,statusCode,count(distinct credentialKey) as credentialKeyCount " +
        "from monthlyRegisterDataView " +
        "group by applicationId,credentialType,statusCode " +
        "order by credentialKeyCount desc")
    appIdDeviceCountDF.createOrReplaceTempView("credentialKeyCountView")

    val res2DF: DataFrame = spark.sql(
      "select " +
        "p.applicationId as application_id," +
        "p.productName as product_name," +
        "p.productGroup as product_group," +
        "a.credentialType as credential_type," +
        "a.statusCode as status_code," +
        "a.credentialKeyCount as credential_key_count " +
        "from credentialKeyCountView as a left join productView as p " +
        "on p.applicationId=a.applicationId " +
        "order by credential_key_count desc"
    ).withColumn("report_month", lit(yearMonth)).na.fill("not found")

    // 5 output json file
    res2DF
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(outputDir + "/" + UserUtils.getMonthlyRegisterDataJsonResultDir(yearMonth))
  }

}
