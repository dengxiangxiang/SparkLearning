package SharedVariables

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

object Broadcast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("broadcast").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val bc: Broadcast[List[Int]] = sc.broadcast(List(100, 2, 3))
    val myAccumulator: LongAccumulator = sc.longAccumulator("myAccumulator")

    println(bc.value)

    val rdd: RDD[Int] = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8), 3)

    rdd.mapPartitions(ite => {
      println(ite)
      ite

    }).foreach(myAccumulator.add(_))

    println(myAccumulator.value)

    Thread.sleep(100 * 1000)
  }
}
