package userlog

import SGLReceipts.SGLReceiptsFilter
import org.scalatest.FunSuite

class SGLReceiptsTest extends FunSuite {

  test("SGLReceiptsTest1") {
    val args = Array(
      "/Users/xxdeng/Desktop/usersvc-log-2020",
      "/Users/xxdeng/Documents/study/sparkParent/usersvcLog/src/test/resources/forSGLReceiptsTest/output",
      "2020-09-01",
      "true",
      "true")
    SGLReceiptsFilter.main(args)
  }

}
