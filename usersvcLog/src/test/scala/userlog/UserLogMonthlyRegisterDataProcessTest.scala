package userlog

import org.scalatest.FunSuite

class UserLogMonthlyRegisterDataProcessTest extends FunSuite{
  test("UserLogMonthlyProcessTest"){
    val args = Array(
      "/Users/xxdeng/Documents/study/sparkParent/usersvcLog/src/test/resources/output",
      "/Users/xxdeng/Documents/study/sparkParent/usersvcLog/src/test/resources/output",
      "201910",
      "true")

    UserLogMonthlyRegisterDataProcess.main(args)
  }

}
