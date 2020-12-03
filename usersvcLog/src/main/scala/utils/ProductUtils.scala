package utils

import java.io.InputStream

import scala.io.Source

object ProductUtils {
  def loadProducts: Array[Product] = {

    val inputStream: InputStream = this.getClass.getClassLoader.getResourceAsStream("applicationId_product")
    val products: Iterator[Product] = Source
      .fromInputStream(inputStream, "utf-8")
      .getLines()
      .map {
        case line: String => {
          val arr: Array[String] = line.split(",")
          Product(arr(1), arr(2), arr(3))
        }
      }

    products.toArray
  }
}

case class Product(
                    applicationId: String,
                    productName: String,
                    productGroup: String
                  )
