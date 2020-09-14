package com.dxx

import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaSource {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("KafkaSource")
      .getOrCreate()

    import  spark.implicits._

    val kafkaDF: DataFrame = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load
    kafkaDF.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate",false)
      .start
      .awaitTermination()
  }

}
