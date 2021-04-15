package externalSys

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.{JdbcRDD, RDD}

object MysqlConnect {

  case class SensorData(id: String, temperature: Double)

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("mysql-connector")
    val sc = new SparkContext(sparkConf)


    val getConnection = () => {
      Class.forName("com.mysql.jdbc.Driver")
      val connection: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?" +
        "useUnicode=true&characterEncoding=utf-8", "root", "root")
      connection
    }


    // 读取数据

    val jdbcRDD:JdbcRDD[SensorData] = new JdbcRDD(
      sc,
      getConnection,
      "select * from sensor_data where id>=? and id<=?",
      -1,
      100,
      2,
      resultSet => SensorData(resultSet.getString(1), resultSet.getDouble(2))
    )
    println(jdbcRDD.toDebugString)
    println(jdbcRDD.collect().toBuffer)


    // 写入数据
    val rdd: RDD[SensorData] = sc.parallelize(Seq(SensorData("Sensor_9",9),SensorData("Sensor_10",10),SensorData("Sensor_11",11)),2)

    rdd.foreachPartition(iterator=>{
      val connection: Connection = getConnection()
      val statement: PreparedStatement = connection.prepareStatement("insert into sensor_data values(?,?);")
      iterator.foreach(sensorData=>{
        statement.setString(1,sensorData.id)
        statement.setDouble(2,sensorData.temperature)
        statement.executeUpdate()
      })
    })

  }
}
