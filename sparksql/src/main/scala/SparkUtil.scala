import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtil {

  def getSparkEnv(master: String = "local", appName: String = "demo"): SparkSession = {
    val spark: SparkSession = SparkSession.builder()
      .master(master)
      .appName(appName)
      .getOrCreate()
    spark
  }

}
