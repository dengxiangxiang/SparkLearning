import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Example {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Example")

    val sc = new SparkContext(conf);

    val rdd: RDD[(String, String)] = sc.makeRDD(
      List(("p1", "a1"),("p1", "a1"),("p1", "a1"), ("p1", "a2"), ("p1", "a3"), ("p1", "a3"),
        ("p2", "a2"), ("p2", "a4"), ("p2", "a5"), ("p2", "a3"),
        ("p3", "a1"), ("p3", "a2"), ("p3", "a3"), ("p3", "a3")
        , ("p3", "a1"), ("p3", "a4"),("p3", "a3"))
      , 2)

    val rdd2: RDD[((String, String), Int)] = rdd.map((_,1))

    val rdd3: RDD[((String, String), Int)] = rdd2.reduceByKey(_+_)

    val rdd4: RDD[(String, (String, Int))] = rdd3.map(r=>(r._1._1,(r._1._2,r._2)))

    rdd4.collect().foreach(println)

    val rdd5: RDD[(String, Iterable[(String, Int)])] = rdd4.groupByKey()

    val rdd6: RDD[(String, List[(String, Int)])] = rdd5.mapValues(i=>i.toList.sortWith((x,y)=>x._2>y._2))

    rdd6.collect().foreach(println)
//    rdd4.sor
  }

}
