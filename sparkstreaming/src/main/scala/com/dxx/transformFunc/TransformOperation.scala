package com.dxx.transformFunc

import org.apache.commons.lang.StringUtils
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TransformOperation {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sparkContext = new SparkContext(conf)

    val streamingContext = new StreamingContext(sparkContext,Seconds(8))
    val lineStreams: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost",9999)

    val wordString: DStream[String] = lineStreams
      .flatMap(_.split(" "))
      .transform(rdd => {
        rdd.filter(word => StringUtils.isNotBlank(word) && word.startsWith("AA"))
      })
    wordString.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
