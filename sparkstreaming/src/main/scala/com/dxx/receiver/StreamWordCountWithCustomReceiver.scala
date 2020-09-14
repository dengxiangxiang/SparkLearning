package com.dxx.receiver

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamWordCountWithCustomReceiver {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val streamingContext = new StreamingContext(sparkConf,Seconds(5))

    val receiver: CustomReceiver = new CustomReceiver("localhost",9999)
    val inputStream: ReceiverInputDStream[String] = streamingContext.receiverStream(receiver)

    val wordCountStream: DStream[(String, Int)] = inputStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    wordCountStream.print()

    streamingContext.start()

    streamingContext.awaitTermination()
  }

}
