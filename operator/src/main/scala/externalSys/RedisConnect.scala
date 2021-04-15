package externalSys

import com.redislabs.provider.redis._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RedisConnect {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("redis-conn")
      .setMaster("local")
      // initial redis host - can be any node in cluster mode
      .set("spark.redis.host", "localhost")
      // initial redis port
      .set("spark.redis.port", "6379")
      // optional redis AUTH password
//      .set("spark.redis.auth", "passwd")

    val sc = new SparkContext(sparkConf)

    // 读取
    val redisRDD: RDD[String] = sc.fromRedisList("list",2)
    redisRDD.foreach(println)

    //写入
    sc.toRedisLIST(redisRDD,"list")


    //再次读
    val redisRDD2: RDD[String] = sc.fromRedisList("list",2)
    redisRDD2.foreach(println)

  }

}
