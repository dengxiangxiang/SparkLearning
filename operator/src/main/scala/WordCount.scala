import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val master = args(0)
    val input = args(1)
    val output = args(2)



    val sparkConf: SparkConf = new SparkConf()
      .setMaster(master)
      .setAppName("WordCount")

    val sc : SparkContext = new SparkContext(sparkConf);

    val lines: RDD[String] = sc.textFile(input)

    println(lines.partitions.length)
    
    val kvs: RDD[(String, Int)] = lines.repartition(4).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    kvs.coalesce(1).saveAsTextFile(output)

    System.in.read()

  }

}
