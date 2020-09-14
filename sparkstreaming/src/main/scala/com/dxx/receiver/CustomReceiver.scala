package com.dxx.receiver

import java.io.{BufferedInputStream, BufferedReader, InputStream, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.slf4j.{Logger, LoggerFactory}


class CustomReceiver(server: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  val LOG : Logger = LoggerFactory.getLogger(this.getClass.getName)

  var socket:Socket = _
  override def onStart(): Unit = {

    LOG.info(s"Start receiver connecting to $server : $port")
      socket = new Socket(server,port)

    new Thread("Receiver Thread"){

      override def run(): Unit = {receive()}

    }.start()

  }


  override def onStop(): Unit = {
    if(socket!=null){
      socket.close()
    }

  }

  def receive(): Unit = {
    val inputStream: InputStream = socket.getInputStream
    val bufferedReader = new BufferedReader(new InputStreamReader(inputStream))

    var line  :String = null;
    while((line=bufferedReader.readLine())!=null){
      super.store(line)
    }
  }

}
