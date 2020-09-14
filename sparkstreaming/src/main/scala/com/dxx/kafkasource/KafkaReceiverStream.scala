package com.dxx.kafkasource

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaReceiverStream {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val streamingContext = new StreamingContext(sparkConf,Seconds(5))

    val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](streamingContext,
      Map("zookeeper.connect"->"localhost:2181",
        "bootstrap.servers"->"localhost:9092",
        "group.id"->"my-kafka-streaming-group-01",
        "auto.offset.reset"->"smallest"),
      Map("test"->1),
      StorageLevel.MEMORY_ONLY)
    val wordCountStream: DStream[(String, Int)] = kafkaStream.flatMap(_._2.split(" ")).map((_,1)).reduceByKey(_+_)

    wordCountStream.print()

    streamingContext.start()

    streamingContext.awaitTermination()



  }
}
