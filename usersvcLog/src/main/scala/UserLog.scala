import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, concat_ws, get_json_object, udf}

object UserLog {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("UsersvcLog")
      .master("spark://localhost:7077")
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

    val logDF = spark.read.text("/data/usersvc-log/app.log/")

    val logJsonStringDF = logDF
      .filter("value <> ''")
      .select(getJsonStringUdf(col("value")) as "jsonString")
      .filter(col("jsonString").contains("94b547e2-8937-48af-bcbf-8a007f5150ac") ||col("jsonString").contains("9TPWAC83PTMJTULB0RVRU73GP") || col("jsonString").contains("6Q2GU5ZZ9FPYY6Z3FEOVIHVIF") ||col("jsonString").contains("CZKJPH3TZLSZN7RP9N5ZOJD32") ||col("jsonString").contains("9K6Z0UDK9OR88HYHAIH32QUYO") || col("jsonString").contains("HOI2NRS69BJOP9TFM619TNXC") || col("jsonString").contains("DG7VXVQOY0MDBOS4KC77QK49U") || col("jsonString").contains("2FUXNX3GWECIXJIVG7R7YU0WY") || col("jsonString").contains("9Y8X2MVO1QC2Z29U3MF8AR173")|| col("jsonString").contains("AGJEU205BC3YU5UOH5M1NMFUS") || col("jsonString").contains("EDC17UKXB3MPFN9DT7OUYS9U1") || col("jsonString").contains("420T3LIJ51TH5BMTTT68959AT") || col("jsonString").contains("3YCVFCNERGKTRX2A3ZSBFR2TT"))

    var resultDF= logJsonStringDF
      .select(
        get_json_object(col("jsonString"), "$.request-time") as "request_time",
        col("jsonString"))
      .select(
        col("request_time"),
        getDateUdf(col("request_time")) as "date",
        get_json_object(col("jsonString"),"$.uri") as "uri",
        col("jsonString")
      ).orderBy("request_time")
        .select(concat_ws("************",col("request_time"),col("date"),col("jsonString")))


    resultDF.coalesce(1).write.mode(SaveMode.Overwrite).format("text").save("data/usersvc-log/app.log/result")



  }

}
