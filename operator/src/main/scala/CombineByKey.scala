import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CombineByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReduceByKey")

    val sc = new SparkContext(conf);

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3),("a",2),("c",2),("c",5)))

    val kvn: RDD[(String, (Int, Int))] = rdd.combineByKey(
      x=>(x,1),
      (c:(Int,Int), v)=>(c._1+v,c._2+1),
      (c:(Int,Int), v:(Int,Int))=>(c._1+v._1,c._2+v._2))

    kvn.collect().foreach(println)

    val res: RDD[(String, Double)] = kvn.mapValues(x => x._1.toDouble/x._2)

    res.collect().foreach(println)
  }
}
