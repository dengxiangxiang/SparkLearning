
import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
object ObjectMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("UsersvcLog")
      .master("local[*]")
      .getOrCreate()

    def getJsonString(log:String) :String ={
      val index = log.indexOf("{")
      log.substring(index)
    }

    def getDate(timestamp:Long) : String ={
      val format: SimpleDateFormat= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss")
      return format.format( new Date(timestamp))
    }

    def getJsonStringUdf = udf(getJsonString(_:String):String)
    def getDateUdf:UserDefinedFunction = udf(getDate(_:Long):String)

    val logDF = spark.read.text("/data/usersvc-log/test.log")

    val logJsonStringDF = logDF
      .filter("value <> ''")
      .select(getJsonStringUdf(col("value")) as "jsonString")
      .filter(col("jsonString").contains("1594105213618")||col("jsonString").contains("1594105215963"))

    var resultDF= logJsonStringDF
      .select(
      get_json_object(col("jsonString"), "$.request-time") as "request_time",
      col("jsonString"))
      .select(
        col("request_time"),
        getDateUdf(col("request_time")) as "date",
        col("jsonString")
      ).orderBy("request_time")
        .select(concat_ws("************",col("request_time"),col("date"),col("jsonString")))

    resultDF.printSchema
    resultDF.printSchema()
    resultDF.show
    resultDF.coalesce(1).write.mode(SaveMode.Overwrite).format("text").save("data/usersvc-log/result")

    println()



  }


}
