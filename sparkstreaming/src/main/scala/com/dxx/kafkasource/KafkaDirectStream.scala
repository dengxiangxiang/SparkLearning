package com.dxx.kafkasource

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDirectStream {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val streamingContext = new StreamingContext(sparkConf,Seconds(5))

    val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext,
      Map("bootstrap.servers"->"localhost:9092",
      "group.id"->"my-kafka-streaming-group-01",
      "auto.offset.reset"->"smallest"),
      Set("test"))
    val wordCountStream: DStream[(String, Int)] = kafkaStream.flatMap(_._2.split(" ")).map((_,1)).reduceByKey(_+_)

    wordCountStream.print()

    streamingContext.start()

    streamingContext.awaitTermination()



  }

}
