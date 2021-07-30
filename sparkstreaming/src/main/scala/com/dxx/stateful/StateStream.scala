package com.dxx.stateful

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

//updateStateByKey
object StateStream {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sparkContext = new SparkContext(conf)

    val streamingContext = new StreamingContext(sparkContext, Seconds(8))
    streamingContext.sparkContext.setCheckpointDir("checkpointdir")
    val lineStreams: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)

    val wordCountStreams: DStream[(String, Int)] = lineStreams
      .flatMap(_.split(" "))
      .map((_, 1))


    val stateDS: DStream[(String, Int)] = wordCountStreams.updateStateByKey {
      (seq: Seq[Int], s) => {
        val sum: Int = s.getOrElse(0) + seq.foldLeft(0)(_ + _)
        Option(sum)
      }
    }


    val stateDS2: DStream[(String, Int)] = wordCountStreams.updateStateByKey(updateFunc)
    stateDS2.print()

    streamingContext.start()

    streamingContext.awaitTermination()

  }

  def updateFunc(seq: Seq[Int], s: Option[Int]): Option[Int] = {
    val res: Int = s.getOrElse(0) + seq.reduce(_ + _)
    Option(res)
  }

}
