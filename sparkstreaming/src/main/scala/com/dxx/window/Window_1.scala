package com.dxx.window

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object Window_1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sparkContext = new SparkContext(conf)

    val streamingContext = new StreamingContext(sparkContext, Seconds(3))
    streamingContext.sparkContext.setCheckpointDir("checkpointdir")
    val lineStreams: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)

    val wordCountStreams: DStream[(String, Int)] = lineStreams
      .flatMap(_.split(" "))
      .map((_, 1))

    val windowStream: DStream[(String, Int)] = wordCountStreams.window(Seconds(9),Seconds(9))
    val wordCountsStream: DStream[(String, Int)] = windowStream.reduceByKey(_+_)

    val countStream1: DStream[(String, Int)] = wordCountStreams.reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y,
      Seconds(9),
      Seconds(6)
    )

    val countStream2 = wordCountStreams.reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y,
      (x: Int, y: Int) => x - y,
      Seconds(9),
      Seconds(6)
    )

    val countStream3: DStream[(String, Int)] = wordCountStreams.reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y,
      (x: Int, y: Int) => x - y,
      Seconds(9),
      Seconds(6),
      new HashPartitioner(sparkContext.defaultParallelism),
      (kv: (String, Int)) => kv._2 > 0
    )


    countStream1.print()
    countStream2.print()
    countStream3.print()

    streamingContext.start()

    streamingContext.awaitTermination()
  }

}
