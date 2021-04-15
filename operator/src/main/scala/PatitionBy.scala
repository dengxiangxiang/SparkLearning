import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object PatitionBy {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("PatitionBy").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val listRdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c"), (4, "d")), 1)

    listRdd.coalesce(1).collect().foreach(print)

    val partitionedRdd: RDD[(Int, String)] = listRdd.partitionBy(new MyPatitioner(2))

    partitionedRdd.saveAsTextFile("data/output")
  }
  class MyPatitioner(patitionNum: Int) extends Partitioner {
    override def numPartitions: Int = {
      patitionNum
    }

    override def getPartition(key: Any): Int = {
      1
    }
  }
}


