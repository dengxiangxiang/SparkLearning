package com.dxx

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery


object SocketSource {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("WorldCount")
      .getOrCreate()

    import  spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host","192.168.137.90")
      .option("port","9999")
      .load

    val worldCount: DataFrame = lines.as[String].flatMap(_.split(" ")).groupBy("value").count()

    val result: StreamingQuery = worldCount.writeStream
      .format("console")
      .outputMode("update")
      .start

    result.awaitTermination()
  }

}
