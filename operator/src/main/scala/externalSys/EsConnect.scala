package externalSys

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark._

object EsConnect {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getCanonicalName)

    conf.set(ConfigurationOptions.ES_NODES, "127.0.0.1")
    conf.set(ConfigurationOptions.ES_PORT, "9200")
    conf.set(ConfigurationOptions.ES_NODES_WAN_ONLY, "true")
    conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "true")
    conf.set(ConfigurationOptions.ES_NODES_DISCOVERY, "false")
//    conf.set(ConfigurationOptions.ES_WRITE_OPERATION,ConfigurationOptions.ES_OPERATION_UPDATE)
    //    conf.set(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, esUser)
    //    conf.set(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, esPwd)
    conf.set("es.write.rest.error.handlers", "ignoreConflict")
    conf.set("es.write.rest.error.handler.ignoreConflict", "externalSys.handler.CustomESErrorHandler")

    val sc = new SparkContext(conf)


    sc.esRDD("user").foreach(each => {
      each._2.keys.foreach(println)
    })
    sc.esJsonRDD("user").foreach(each => {
      println(each._2)
    })

    val esRDD: RDD[(String, collection.Map[String, AnyRef])] = sc.esRDD("user")

    esRDD.saveToEs("user/book")



    sc.stop()
  }


}
