package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RddMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf ()
    conf.setMaster("local[*]")
    conf.setAppName("RDD test")

    val sc = new SparkContext(conf)
    val listRdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

     
    val anotherRDD = listRdd.map(_*2)
    anotherRDD.foreach(println)

    val aaaRdd = listRdd.mapPartitions(_.map(_*2))
    aaaRdd.collect().foreach(println)

    val indexRdd = listRdd.mapPartitionsWithIndex((index,ital)=>ital.map((_,"分区：+"+index)))
    indexRdd.collect().foreach(println)

    val arrRdd: RDD[Int] = sc.makeRDD(Array(3,2,1,5))
    arrRdd.sortBy(x=>x).collect().foreach(println)

  }
}
