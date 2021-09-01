package userlog

import org.scalatest.FunSuite

class UserLogBriefTotalTest extends FunSuite{

  test("UserLogBriefTotalTest1"){

    val array: Array[String] = Array("/Users/xxdeng/Desktop/usersvc-log-2020-ec1-01", "/Users/xxdeng/Desktop/usersvc-log-2020-ec1-01-output")
    UserLogBriefTotal.main(array)
  }
}
