package com.dxx.streaming.blacklist

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object MockData {
  def main(args: Array[String]): Unit = {
    // 生成模拟数据
    // 格式：timestamp,area,city,userId,adId

    //app => Kafka => sparkStreaming => Analysis

    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String,String](prop)

    while (true){
      mockData().foreach({
        data=>{
          val record = new ProducerRecord[String,String]("topicOfAds",data)
          producer.send(record)

          println(data)
        }
      })

      Thread.sleep(2000)
    }



  }

  def mockData(): ListBuffer[String] = {
    var list = ListBuffer[String]()
    val areaList = ListBuffer[String]("华北", "华东", "华南")
    val cityList = ListBuffer[String]("北京", "上海", "深圳")


    for (i <- 1 to 20) {
      val timestamp = System.currentTimeMillis()
      val area = areaList(new Random().nextInt(3))
      val city = cityList(new Random().nextInt(3))
      val userId = new Random().nextInt(6)+1
      val adId = new Random().nextInt(6)+1

      list.append(s"${timestamp},${area},${city},${userId},${adId}")
    }

    list
  }
}
