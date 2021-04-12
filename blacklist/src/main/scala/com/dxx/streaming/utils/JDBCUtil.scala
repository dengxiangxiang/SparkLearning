package com.dxx.streaming.utils

import java.sql.Connection
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource


object JDBCUtil {

  def init(): DataSource = {

    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", "jdbc:mysql://localhost:3306/spark_streaming")
    properties.setProperty("username", "root")
    properties.setProperty("password", "123456")
    properties.setProperty("maxActive", "50")

    DruidDataSourceFactory.createDataSource(properties)
  }

  var dataSource: DataSource = init()

  def getConnection():Connection ={
    dataSource.getConnection
  }
}
