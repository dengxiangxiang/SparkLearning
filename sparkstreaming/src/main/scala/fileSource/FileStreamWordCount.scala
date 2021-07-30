package fileSource

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileStreamWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(8))

    val lines: DStream[String] = ssc.textFileStream("/Users/xxdeng/Documents/study/sparkParent/sparkstreaming/src/main/resources/fileSource/fileSourceFinal")
    val wordCountStream = lines
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    wordCountStream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
