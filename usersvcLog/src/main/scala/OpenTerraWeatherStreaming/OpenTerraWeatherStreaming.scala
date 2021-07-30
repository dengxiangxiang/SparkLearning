package OpenTerraWeatherStreaming

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object OpenTerraWeatherStreaming {
  def main(args: Array[String]): Unit = {

    if(args.length!=2){
      throw new Exception("Should set parameter1: S3 path and parameter 2: monitor time interval in second to start...")
    }

    val path = args(0)
    val interval = args(1).toInt

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val sparkContext: SparkContext = new SparkContext(conf)

    val streamingContext = new StreamingContext(sparkContext,Seconds(interval))

    val ds: DStream[String] = streamingContext.textFileStream(path)

    ds.foreachRDD(rdd=>{
      rdd.foreach(println)
    })


    streamingContext.start()
    streamingContext.awaitTermination();
  }

}
