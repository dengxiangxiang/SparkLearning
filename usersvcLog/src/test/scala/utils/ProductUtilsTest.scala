package utils

import org.scalatest.FunSuite

class ProductUtilsTest extends FunSuite {
  test("ProductUtilsTest") {

    val products: Array[Product] = ProductUtils.loadProducts
    assert(products.length == 82)

  }


}
