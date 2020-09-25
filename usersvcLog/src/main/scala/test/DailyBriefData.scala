package test

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DailyBriefData {
  def main(args: Array[String]): Unit = {


    val builder: SparkSession.Builder = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")

    val sparkSession: SparkSession = builder.getOrCreate()

    import sparkSession.implicits._

    val df: DataFrame = sparkSession.read
      .textFile("/Users/xxdeng/dailyBriefData-30-tmp-createdAt-20200831002500-aggregate")
      .map(line => line.split(',')).map(p => new Log(p(0), p(1), p(2), p(3), p(4), p(5), p(6).toInt)).toDF()
     df.createOrReplaceTempView("tempView")

    df.printSchema()

    val apiKeyDF: DataFrame = sparkSession.sql("select apiKey, sum(count) as total from tempView group By apiKey order By total desc")

    val resultFrame: DataFrame = apiKeyDF.select(concat_ws(",",col("apiKey"),col("total")))
    resultFrame.printSchema()


    resultFrame.coalesce(1).write.mode(SaveMode.Overwrite).format("text").save("/Users/xxdeng/dailyBriefData-30-tmp-createdAt-20200831002500-aggregate.result")
  }

  case class Log(date:String,apiKey:String,userId :String,creType:String,creKey:String,deviceId:String,count:Int);
}
