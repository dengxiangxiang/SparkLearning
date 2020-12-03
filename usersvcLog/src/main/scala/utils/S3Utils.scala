package utils

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.transfer.TransferManager

import scala.collection.mutable.ListBuffer

object S3Utils {

  def uploadFileToS3(s3bucket: String, s3Prefix: String, localDirectory: String): Boolean = {
    val tx = new TransferManager(new DefaultAWSCredentialsProviderChain());
    val file = new File(localDirectory);
    try{
      val myUpload = tx.upload(s3bucket, s3Prefix, file);
      while (myUpload.isDone() == false) {
        try {
          Thread.sleep(100);
        } catch {
          case ie: InterruptedException => ie.printStackTrace()
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
        return false
    }
    tx.shutdownNow();
    return true

  }

  def uploadDirectoryToS3(s3bucket: String, s3Prefix: String, localDirectory: String): Boolean = {
    val tx = new TransferManager(new DefaultAWSCredentialsProviderChain());
    val file = new File(localDirectory);
    try{
      val myUpload = tx.uploadDirectory(s3bucket, s3Prefix, file, true);
      while (myUpload.isDone() == false) {
        try {
          Thread.sleep(100);
        } catch {
          case ie: InterruptedException => ie.printStackTrace()
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
        return false
    }
    tx.shutdownNow();
    return true

  }

  def getLastXdaysPath(numberOfDays: Integer, basePath: String): Seq[String] = {

    val yearformat = new SimpleDateFormat("yyyy")
    val monthformat = new SimpleDateFormat("MM")
    val dayformat = new SimpleDateFormat("dd")
    var s3PathLastXDays = new ListBuffer[String]()
    for( a <- 1 to numberOfDays) {
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE, (-1 * a))
      val log_year = yearformat.format(cal.getTime())
      val log_month = monthformat.format(cal.getTime())
      val log_day = dayformat.format(cal.getTime())
      val path = basePath.concat("/").concat(s"log_year=$log_year").concat("/").concat(s"log_month=$log_month").concat("/").concat(s"log_day=$log_day")
      if(S3Utils.isPathExist(path)){
        s3PathLastXDays += path
      }
      else{
        println(path+" does not exist and is ignore.")
      }

    }

    return s3PathLastXDays.toSeq

  }

  // today
  def getLastXdaysPath(numberOfDays: Integer, basePath: String,runDt:String ,isLocal:Boolean, containsRunDay:Boolean): Seq[String] = {
    val sdf1=new SimpleDateFormat("yyyyMMdd")
    val dateRunDt=sdf1.parse(runDt)
    val cal = Calendar.getInstance()
    cal.setTime(dateRunDt)
    val yearformat = new SimpleDateFormat("yyyy")
    val monthformat = new SimpleDateFormat("MM")
    val dayformat = new SimpleDateFormat("dd")
    var s3PathLastXDays = new ListBuffer[String]()
    for( a <- 0 to (numberOfDays-1)) {
      if(a>0) {
        cal.add(Calendar.DATE, -1)
      }else{
        if(containsRunDay){
          cal.add(Calendar.DATE, 0)
        }else{
          cal.add(Calendar.DATE, -1)
        }
      }
      val log_year = yearformat.format(cal.getTime())
      val log_month = monthformat.format(cal.getTime())
      val log_day = dayformat.format(cal.getTime())
      val path = basePath.concat("/").concat(s"log_year=$log_year").concat("/").concat(s"log_month=$log_month").concat("/").concat(s"log_day=$log_day")
      if(!isLocal){
        if(S3Utils.isPathExist(path)){
          s3PathLastXDays += path
        }
        else{
          println(path+" does not exist and is ignore.")
        }
      }else{
        s3PathLastXDays += path
      }
    }

    return s3PathLastXDays.toSeq

  }

  def isPathExist(path: String): Boolean ={
    if(path.startsWith("s3")){
      return isS3PathExist(path)
    }
    else if(path.startsWith("file")){
      return true
    }
    else {
      return false
    }
  }


  def isS3PathExist(s3Path: String): Boolean = {
    var isExisted = false
    if (null == s3Path || 0 == s3Path.length) isExisted = false
    try {
      val s3 = new AmazonS3Client
      val s3PrefixLength = "s3://".length
      val firstDelimiter = s3Path.indexOf("/", s3PrefixLength)
      if (firstDelimiter > 0) {
        val bucketName = s3Path.substring(s3PrefixLength, firstDelimiter)
        val objectKey = s3Path.substring(firstDelimiter + 1)
        val request = new ListObjectsRequest
        request.setBucketName(bucketName)
        request.setPrefix(objectKey)
        request.setMaxKeys(1)
        val list = s3.listObjects(request)
        if (list.getObjectSummaries.size == 1) isExisted = true
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    isExisted
  }

  def getEsTimestamp(runDt: String): String = {
    var ret = ""
    val TIME_ZONE="UTC"
    try {
      val dateIdFormat = new SimpleDateFormat("yyyyMMdd")
      dateIdFormat.setTimeZone(TimeZone.getTimeZone(TIME_ZONE))
      val cal = Calendar.getInstance
      cal.setTime(dateIdFormat.parse(runDt))
      val dirFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
      dirFormat.setTimeZone(TimeZone.getTimeZone(TIME_ZONE))
      ret = dirFormat.format(cal.getTime)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    ret
  }

  def composeOutputS3Location(outputS3Path: String,runDt:String): String = {

    val cal = Calendar.getInstance()
    val dateIdFormat = new SimpleDateFormat("yyyyMMdd")
    cal.setTime(dateIdFormat.parse(runDt))
    val yearformat = new SimpleDateFormat("yyyy")
    val monthformat = new SimpleDateFormat("MM")
    val dayformat = new SimpleDateFormat("dd")

    val log_year = yearformat.format(cal.getTime())
    val log_month = monthformat.format(cal.getTime())
    val log_day = dayformat.format(cal.getTime())

    val outputS3Location: String = outputS3Path.concat("/").concat(s"log_year=$log_year").concat("/").concat(s"log_month=$log_month").concat("/").concat(s"log_day=$log_day")

    return outputS3Location

  }

}
