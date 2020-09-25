package userlog


import common.LogProcessor
import common.entity.ProcessResult
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import userlog.utils.{Result, UserUtils}


object UserLogAnalysis {
  def main(args: Array[String]): Unit = {

    println("args:")
    args.foreach(println)

    assert(args.length == 4)

    val inputDir = args(0)
    val outputDir = args(1)
    val date = args(2)

    var isLocalMode = false
    if (args.length == 4) {
      isLocalMode = args(3).toBoolean
    }

    val builder = SparkSession
      .builder()
      .appName(this.getClass.getName)

    if (isLocalMode) {
      println("master: local[*]")
      builder.master("local[*]")
    }

    val spark = builder.getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val inputFiles: Array[String] = UserUtils.userServers.map(
      inputDir + "//" + _ + ".app.log." + date + ".gz"
    )

    val originDS: Dataset[String] = spark.read.textFile(inputFiles: _*).filter(line => line != null && line.length != 0)

    val resultDS: Dataset[Result] = originDS.map {
      case line => {
        try {
          val processResult: ProcessResult = LogProcessor.processUserServiceLog(line)
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
    }.filter(_!=null)

    resultDS.toDF().createOrReplaceTempView("resultView")

    val countDF: DataFrame = spark.sql("select count(1) as count from resultView")

    countDF.printSchema()

    countDF.cache()

    countDF.collect().foreach(println)

    countDF.coalesce(1)
      .write
      .format("json")
      .mode(SaveMode.Append)
      .save(outputDir+"/resultOfCount")



  }


}

