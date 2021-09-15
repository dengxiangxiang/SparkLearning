package com.dxx.radarmap

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerJobStart, StatsReportListener}
import org.apache.spark.sql.SparkSession

import java.io.ByteArrayOutputStream
import java.util
import scala.collection.JavaConverters

object RadarMapMain {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[20]")
      .appName("RadarMap")
      .getOrCreate()

    val context = spark.sparkContext
    context.addSparkListener(new StatsReportListener() {

      var startTime: Long = 0

      override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
        super.onApplicationStart(applicationStart)
        val appName = applicationStart.appName
        println(appName + "========================start")
        startTime = System.currentTimeMillis()
      }

      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        super.onJobStart(jobStart)
        startTime = jobStart.time
      }

      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        super.onApplicationEnd(applicationEnd)

        val appName = applicationEnd.toString + "================= stooooooped"
        println(appName)

        val timeCostInSec = (System.currentTimeMillis() - startTime) / 1000.0

        println("time cost in seconds: " + timeCostInSec.toString)
      }
    })

    val list: Seq[ZXY] = generateZXYList
    println("size:" + list.size)

    val zxyRDD: RDD[ZXY] = spark.sparkContext.parallelize(generateZXYList, 60)


    zxyRDD.foreachPartition {
      itr => {
        val context = JedisUtil.getTileContext
        val jedis = JedisUtil.getJedis
        itr.foreach {
          zxy => {
            val outputStream = new ByteArrayOutputStream()
            context.outputPng(zxy.z, zxy.x, zxy.y, outputStream)
            val byteArray: Array[Byte] = outputStream.toByteArray
            val subKey = String.format("%s/%s/%s", zxy.z.toString, zxy.x.toString, zxy.y.toString)
            val hkey = "radarmap"
            jedis.hset(hkey.getBytes, subKey.getBytes, byteArray)

          }

        }

      }
    }


  }

  def generateZXYList(): Seq[ZXY] = {

    val list = new util.ArrayList[ZXY]()

    val t0 = System.currentTimeMillis()

    for (z <- 11 to 11) {
      val d = Math.pow(2, z).toInt
      for (x <- Range(0, Math.pow(2, z).toInt)) {
        for (y <- Range(0, Math.pow(2, z).toInt)) {
          if (TileContext.checkValidZXY(z, x, y)){
            list.add(ZXY(z,x,y))
          }

        }
      }
    }

    val seq: Seq[ZXY] = JavaConverters.asScalaIteratorConverter(list.iterator()).asScala.toSeq

    val secs = (System.currentTimeMillis() - t0) / 1000.0
    println("generateZXYList cost: " + secs)

    seq;
  }


  case class ZXY(z: Int, x: Int, y: Int);
}
