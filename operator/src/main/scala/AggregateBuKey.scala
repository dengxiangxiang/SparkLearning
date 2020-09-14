import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AggregateBuKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReduceByKey")

    val sc = new SparkContext(conf);

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3),("a",2),("c",2),("c",5)),2)

    val res: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_,_+_)

    res.collect().foreach(println)
  }

}
