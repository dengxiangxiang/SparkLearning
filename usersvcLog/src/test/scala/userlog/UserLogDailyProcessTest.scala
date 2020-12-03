package userlog

import org.scalatest.FunSuite

class UserLogDailyProcessTest extends FunSuite {
  test("UserLogDailyProcessTest1") {
    val args = Array(
      "/Users/xxdeng/Documents/study/sparkParent/usersvcLog/src/test/resources/input",
      "/Users/xxdeng/Documents/study/sparkParent/usersvcLog/src/test/resources/output",
      "2019-10-06",
      "true",
      "true")
    UserLogDailyProcess.main(args)
  }

  test("UserLogDailyProcessTest2") {
    val args = Array(
      "/Users/xxdeng/Documents/study/sparkParent/usersvcLog/src/test/resources/input",
      "/Users/xxdeng/Documents/study/sparkParent/usersvcLog/src/test/resources/output",
      "2019-10-17",
      "true",
      "true")
    UserLogDailyProcess.main(args)
  }

  test("UserLogDailyProcessTest3") {
    val args = Array(
      "/Users/xxdeng/Documents/study/sparkParent/usersvcLog/src/test/resources/input",
      "/Users/xxdeng/Documents/study/sparkParent/usersvcLog/src/test/resources/output",
      "2019-10-06,2019-10-17",
      "true",
      "true")
    UserLogDailyProcess.main(args)
  }

}
