package utils

import org.scalatest.FunSuite

class UserUtilsTest extends FunSuite {
  test("getTotalDayOfMonth-1") {
    var days: Int = UserUtils.getTotalDayOfMonth(2019, 2)
    assert(days == 28)

    days = UserUtils.getTotalDayOfMonth(2020, 2)
    assert(days == 29)

    days = UserUtils.getTotalDayOfMonth(2020, 9)
    assert(days == 30)

    days = UserUtils.getTotalDayOfMonth(2020, 10)
    assert(days == 31)
  }
}
