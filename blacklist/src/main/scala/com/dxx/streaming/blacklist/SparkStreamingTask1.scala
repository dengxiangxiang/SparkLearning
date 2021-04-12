package com.dxx.streaming.blacklist

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import com.dxx.streaming.utils.JDBCUtil
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer


object SparkStreamingTask1 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkStreamingTask")

    val context: SparkContext = org.apache.spark.SparkContext.getOrCreate(sparkConf)

    val streamingContext = new StreamingContext(context, Seconds(5))

    val kafkaPara: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "com.dxx",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> (new StringDeserializer().getClass.getName),
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> new StringDeserializer().getClass.getName
    )

    val kafkaStream: InputDStream[(String, String)] = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](
        streamingContext,
        kafkaPara,
        Set("topicOfAds"))

    val adClickData: DStream[AdClickData] = kafkaStream.map(
      kv => {
        val strs: Array[String] = kv._2.split(",")
        AdClickData(strs(0).toLong, strs(1), strs(2), strs(3), strs(4))
      }
    )


    val ds: DStream[((String, String, String), Int)] = adClickData.transform(
      (rdd: RDD[AdClickData]) => {
        // TODO 周期性获取黑名单数据
        val blackList: ListBuffer[String] = getBlackList

        // TODO 判断点击用户是否在黑名单
        val filterRDD: RDD[AdClickData] = rdd.filter(x =>
          !blackList.contains(x.userId)
        )


        // TODO 如果用户不在黑名单中，则进行数量统计（该统计周期内的统计）
        filterRDD.map {
          data => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val date: String = sdf.format(new Date(data.timestamp.toLong))
            val userId = data.userId
            val adId = data.adId

            ((date, userId, adId), 1)
          }
        }.reduceByKey(_ + _)
      }
    )

    // 使用rdd.foreachPartition 这个方法提高性能，以分区为单位进行数据处理，避免为每个数据创建连接。
//    ds.foreachRDD{
//      rdd=>{
//        rdd.foreachPartition{
//          iterator =>{
//            val connection = JDBCUtil.getConnection()
//            iterator.foreach{
//              // use connection
//            }
//            connection.close()
//          }
//        }
//      }
//    }


    // 这个实现为每一个数据创建连接，性能有问题。
    ds.foreachRDD {
      rdd => {
        rdd.foreach {
          case ((day, user, ad), count) => {
            if (count >= 30) {
              // TODO 如果统计数量超过点击阈值，则拉入黑名单
              insertBlackList(user)
            } else {
              // TODO 如果没有超过阈值，则将当天的广告点击数量进行更新
              val count0: Int = queryCountOfUserAd(day, user, ad)
              val count1 = count + count0
              if (count0 < 0) {
                // 如果不存在则插入
                insertUserAdCount(day, user, ad, count)
              } else {
                updateUserAdCount(day, user, ad, count1)
              }

              if (count1 > 30) {
                // TODO 判断更新后的点击数量是否超过阈值，如果超过，则拉入黑名单
                insertBlackList(user)

              }
            }
          }
        }
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()


  }

  case class AdClickData(timestamp: Long, area: String, city: String, userId: String, adId: String)

  def getBlackList: ListBuffer[String] = {
    val list: ListBuffer[String] = new ListBuffer[String]();
    val connection: Connection = JDBCUtil.getConnection()
    val statement: PreparedStatement = connection.prepareStatement("select user_id from black_list")
    val resultSet: ResultSet = statement.executeQuery()

    while (resultSet.next()) {
      list.append(resultSet.getString("user_id"))
    }

    resultSet.close()
    statement.close()
    connection.close()

    list;
  }

  def insertBlackList(userId: String) = {
    val connection: Connection = JDBCUtil.getConnection()
    val statement: PreparedStatement = connection.prepareStatement(
      """
        |insert into black_list (user_id) values (?)
        |on DUPLICATE KEY
        |UPDATE user_id = ?
        |""".stripMargin)
    statement.setString(1, userId)
    statement.setString(2, userId)
    statement.executeUpdate()
    statement.close()
    connection.close()
  }

  def insertUserAdCount(dt: String, userId: String, adId: String, count: Int) = {
    val connection: Connection = JDBCUtil.getConnection()
    val statement: PreparedStatement = connection.prepareStatement(
      " insert into user_ad_count (dt,user_id,ad_id,count) values (?,?,?,?)")
    statement.setString(1, dt)
    statement.setString(2, userId)
    statement.setString(3, adId)
    statement.setInt(4, count)
    statement.executeUpdate()
    statement.close()
    connection.close()
  }

  def updateUserAdCount(dt: String, userId: String, adId: String, count: Int) = {
    val connection: Connection = JDBCUtil.getConnection()
    val statement: PreparedStatement = connection.prepareStatement(
      " update user_ad_count set count = ? where dt = ? and user_id = ? and ad_id = ?")
    statement.setInt(1, count)
    statement.setString(2, dt)
    statement.setString(3, userId)
    statement.setString(4, adId)

    statement.executeUpdate()
    statement.close()
    connection.close()
  }

  def queryCountOfUserAd(dt: String, userId: String, adId: String): Int = {
    var res: Int = -1;
    val connection: Connection = JDBCUtil.getConnection()
    val statement: PreparedStatement = connection.prepareStatement(
      " select count from user_ad_count where dt = ? and user_id = ? and ad_id = ?")
    statement.setString(1, dt)
    statement.setString(2, userId)
    statement.setString(3, adId)

    val resultSet: ResultSet = statement.executeQuery()

    if (resultSet.next()) {
      res = resultSet.getInt(1)
    }

    statement.close()
    connection.close()

    res
  }


}
