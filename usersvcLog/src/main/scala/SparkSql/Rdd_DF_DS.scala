package SparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Rdd_DF_DS {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val builder = new SparkSession.Builder()

    val spark: SparkSession = builder
      .appName("a")
      .master("local")
      .getOrCreate()

    import spark.implicits._


    val rdd: RDD[Person] = spark.sparkContext.makeRDD(Seq(Person("a", 10), Person("b", 11), Person("c", 12)))


    //    rdd=>ds
    val ds: Dataset[Person] = rdd.toDS()

    //    rdd=>df
    val df1: DataFrame = rdd.toDF()
    val df2: DataFrame = rdd.toDF("col_name", "col_age", "col_sl")

    df1.printSchema()
    df2.printSchema()

    //    ds=>rdd
    val rdd2: RDD[Person] = ds.rdd

    //    df=>rdd
    val rdd3: RDD[Row] = df1.rdd


    //    ds=>df
    val df3: DataFrame = ds.toDF()
    df3.printSchema()

    //    df=>ds
    val ds1: Dataset[Person] = df1.map(row => Person(row.getAs[String](1), row.getAs[Int](2)))

    println(ds1.collect())

  }

}
