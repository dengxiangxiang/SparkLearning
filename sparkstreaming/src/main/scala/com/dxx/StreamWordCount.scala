package com.dxx

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

// start a tcp server by:
// nc -lk  9999
//

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sparkContext = new SparkContext(conf)

    val streamingContext = new StreamingContext(sparkContext,Seconds(8))
    val lineStreams: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost",9999)

    val wordCountStreams: DStream[(String, Int)] = lineStreams
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    wordCountStreams.print()

    streamingContext.start()

    streamingContext.awaitTermination()

  }
}
