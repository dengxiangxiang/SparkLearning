package com.dxx.weather

import java.io.{BufferedReader, BufferedWriter, FileReader, FileWriter}
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import scala.collection.mutable.ListBuffer

object SyncCurrentWeatherToLocalFile {

  class DataUnit(var starttime: String, var endtime: String, var fileList: ListBuffer[String]) {

    def addFile(fileName: String): Unit = {
      if (fileList == null) {
        fileList = ListBuffer[String]()
      }
      fileList += fileName
    }

    def settime(time: String): Unit = {
      this.starttime = this.endtime
      this.endtime = time
    }

    def writeToFile(targetFile: String): Unit = {
      val fileWriter = new FileWriter(targetFile, true)
      val bufferedWriter = new BufferedWriter(fileWriter)

      if (fileList != null && fileList.size > 0) {
        fileList.foreach(element => {
          val line: String = String.format("%s,%s,%s", starttime, endtime, element)
          bufferedWriter.write(line)
          bufferedWriter.newLine()
        })

        bufferedWriter.flush()
        fileList.clear()
      }


    }
  }

  val srcformat = new SimpleDateFormat("E MMM d HH:mm:ss Z yyyy")
  val targetFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")


  def main(args: Array[String]): Unit = {
    targetFormat.setTimeZone(TimeZone.getTimeZone("GMT-700"))
    val file: String = "/Users/xxdeng/Documents/Work/WeatherNotification/synccurrentweather.log"
    val targetFile: String = "/Users/xxdeng/Documents/Work/WeatherNotification/synccurrentweather.log.formatted"
    val fileReader = new FileReader(file)
    val br = new BufferedReader(fileReader)

    var unRecoredCount = 0

    var dataUnit: DataUnit = new DataUnit(null, null, null)
    var line: String = br.readLine()

    while (line != null) {
      try {

        if (line.contains("PDT 2021")) {
          val time: String = convertTimeString(line)
          dataUnit.settime(time)
        } else if (line.contains("---end")) {
          dataUnit.writeToFile(targetFile)
        } else if (line.contains("CurrentWeather")) {
          val index: Int = line.lastIndexOf("/")
          val name: String = line.substring(index + 1)
          dataUnit.addFile(name)
        }

      } catch {
        case e: Exception => {
          println(e.getMessage)
          println(line)
          unRecoredCount = unRecoredCount + 1
          println("unRecoredCount:" + unRecoredCount)
        }
      }

      line = br.readLine()

    }
  }

  def convertTimeString(time: String): String = {
    val date: Date = srcformat.parse(time)
    val str: String = targetFormat.format(date)
    str

  }

}


