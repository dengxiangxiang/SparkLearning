package SaveReceiptRequest

import org.scalatest.FunSuite

class SaveReceiptRequestETLMonthlyTest extends FunSuite {
  test("SaveReceiptRequestETLMonthlyTest1") {
    val args = Array(
      "/Users/xxdeng/Documents/study/sparkParent/usersvcLog/src/test/resources/SaveReceiptRequestETLTest/output/202009/*/part-*",
      "/Users/xxdeng/Documents/study/sparkParent/usersvcLog/src/test/resources/SaveReceiptRequestETLTest/output/monthlySum/202009",
      "true")
    SaveReceiptRequestETLMonthly.main(args)
  }
}
