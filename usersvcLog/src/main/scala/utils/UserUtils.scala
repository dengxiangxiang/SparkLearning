package utils

import java.io.File
import java.util.{Calendar, TimeZone}

import scala.collection.immutable

object UserUtils {
  val DAILYBRIEFDATA = "DailyBriefData"
  val DAILYREGISTERDATA = "DailyRegisterData"

  def getLocalFiles(inputDir: String, date: String): Array[String] = {

    val userServers: Array[String] = Array(
      "ec1-usersvc-01",
      "ec1-usersvc-02",
      "ec1-usersvc-03",
      "ec1-usersvc-04",
      "ec2-usersvc-01",
      "ec2-usersvc-02",
      "ec2-usersvc-03"
    )
    val fileFormat = "%s.app.log.%s.gz";
    val files: Array[String] = userServers
      .map(inputDir + "/" + fileFormat.format(_, date))
      .filter {
        case path: String => {
          if (path.startsWith("s3:")) {
            if (S3Utils.isS3PathExist(path)) {
              println(path + " included!")
              true
            } else {
              println("Warn: " + path + " should exist!")
              false
            }
          } else {
            val file = new File(path)
            if (!file.exists() || !file.isFile()) {
              println("Warn: " + path + " should be file and exist!")
              false
            } else {
              true
            }
          }
        }
      }

    files
  }

  /*
  for ec1:
  s3://noc-archive-virginia/ec1-usersvc-01/2020/09/home/tnuser/logs/UserService/app.log.2020-09-01.gz

  for ec2:
  s3://noc-archive-oregon/ec2-usersvc-01/2020/09/home/tnuser/logs/UserService/app.log.2020-09-01.gz
   */
  def getS3Files(inputDir: String, date: String): Array[String] = {
    val year = date.substring(0, 4)
    val month = date.substring(5, 7)

    val ec1Format = "s3://noc-archive-virginia/%s/%s/%s/home/tnuser/logs/UserService/app.log.%s.gz"
    val ec2Format = "s3://noc-archive-oregon/%s/%s/%s/home/tnuser/logs/UserService/app.log.%s.gz"

    Array(
      ec1Format.format("ec1-usersvc-01", year, month, date),
      ec1Format.format("ec1-usersvc-02", year, month, date),
      ec1Format.format("ec1-usersvc-03", year, month, date),
      ec1Format.format("ec1-usersvc-04", year, month, date),

      ec2Format.format("ec2-usersvc-01", year, month, date),
      ec2Format.format("ec2-usersvc-02", year, month, date),
      ec2Format.format("ec2-usersvc-03", year, month, date)
    )

  }

  def checkExist(path: String): Boolean = {
    var exist = false;
    if (path.startsWith("s3:")) {
      if (S3Utils.isS3PathExist(path)) {
        exist = true
      }
    } else {
      val file = new File(path)
      if (file.exists) {
        exist = true
      }
    }

    if (exist) {
      println(path + " exists!")
    } else {
      println("Warn: " + path + " does not exist!")
    }

    exist
  }

  def getAggregatedDailyBriefDatas(inputDir: String, yearMonth: String): Array[String] = {
    val subDirFormat = "%s/%s/%s/%s"
    val year = yearMonth.substring(0, 4).toInt
    val month = yearMonth.substring(4, 6).toInt
    getDaysByYearMonth(year, month)
      .map(subDirFormat.format(inputDir, yearMonth, _, this.DAILYBRIEFDATA))
      .filter {
        case path: String => {
          checkExist(path)
        }
      }
  }

  def getAggregatedDailyRegisterDatas(inputDir: String, yearMonth: String): Array[String] = {
    val subDirFormat = "%s/%s/%s/%s"
    val year = yearMonth.substring(0, 4).toInt
    val month = yearMonth.substring(4, 6).toInt
    getDaysByYearMonth(year, month)
      .map(subDirFormat.format(inputDir, yearMonth, _, this.DAILYREGISTERDATA))
      .filter {
        case path: String => {
          checkExist(path)
        }
      }
  }

  def getDailyBriefDataOutputDir(date: String): String = {
    val yearMonth = getYearMonthString(date)
    yearMonth + "/" + date + "/" + this.DAILYBRIEFDATA
  }

  def getDailyRegisterDataOutputDir(date: String): String = {
    val yearMonth = getYearMonthString(date)
    yearMonth + "/" + date + "/" + this.DAILYREGISTERDATA
  }

  def getMonthlySumDailyBriefDataDir(yearMonth: String): String = {
    val dir = yearMonth + "/" + "MonthlySum" + "/" + this.DAILYBRIEFDATA
    dir
  }

  def getMonthlySumDailyRegisterDataDir(yearMonth: String): String = {
    val dir = yearMonth + "/" + "MonthlySum" + "/" + this.DAILYREGISTERDATA
    dir
  }

  def getMonthlyBriefDataJsonResultDir(yearMonth: String): String = {
    val dir = yearMonth + "/" + "JsonResult" + "/" + this.DAILYBRIEFDATA
    dir
  }

  def getMonthlyRegisterDataJsonResultDir(yearMonth: String): String = {
    val dir = yearMonth + "/" + "JsonResult" + "/" + this.DAILYREGISTERDATA
    dir
  }

  def getTotalDayOfMonth(year: Int, month: Int): Int = {

    val calendar: Calendar = Calendar.getInstance()
    calendar.setTimeZone(TimeZone.getTimeZone("GMT+0000"))
    calendar.set(Calendar.YEAR, year)
    calendar.set(Calendar.MONTH, month - 1)
    calendar.set(Calendar.DAY_OF_MONTH, 15)

    calendar.getActualMaximum(Calendar.DAY_OF_MONTH)
  }


  def getDaysByYearMonth(year: Int, month: Int): Array[String]

  = {
    val dateFormat = "%04d-%02d-%02d"
    val maxDay: Int = getTotalDayOfMonth(year, month)
    val strings: immutable.IndexedSeq[String] = (1 to maxDay).map(dateFormat.format(year, month, _))
    val array = Array(strings: _*)
    array
  }

  private def getYearMonthString(date: String): String

  = {
    val year = date.substring(0, 4)
    val month = date.substring(5, 7)
    year + month
  }


}
