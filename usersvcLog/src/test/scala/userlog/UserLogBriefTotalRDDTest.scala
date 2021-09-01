package userlog

import org.scalatest.FunSuite

class UserLogBriefTotalRDDTest extends FunSuite{

  test(""){
    val array: Array[String] = Array("/Users/xxdeng/Desktop/usersvc-log-2020-ec1-01", "/Users/xxdeng/Desktop/usersvc-log-2020-ec1-01-output")
    UserLogBriefTotalRDD.main(array)
  }

}
