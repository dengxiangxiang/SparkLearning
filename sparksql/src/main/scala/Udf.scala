import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}

object Udf {

  case class Employee(name: String, salary: Long)

  case class Average(var count: Long, var sum: Long)


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkEnv()

    val employees: Seq[Employee] = Seq(
      Employee("a", 20),
      Employee("b", 40),
      Employee("c", 60),
      Employee("d", 80),
      Employee("e", 100)
    )
    val employeeRdd: RDD[Employee] = spark.sparkContext.makeRDD(employees)

    import spark.implicits._
    val employeeDS: Dataset[Employee] = employeeRdd.toDS()

        val employeeDF: DataFrame = employeeRdd.toDF
        employeeDF.createOrReplaceTempView("employee")
    //
    //    val myUdfFun = (s: String) => StringUtils.upperCase(s)

    //    spark.udf.register("myUdfFun", myUdfFun)
    //    spark.udf.register("UdfClass", new UdfClass)
    //
    //    val df1: DataFrame = spark.sql("select myUdfFun(name),salary from employee")
    //    df1.show()
    //
    //    val df2: DataFrame = spark.sql("select UdfClass(name),salary from employee")
    //    df2.show()

    spark.udf.register("UdfTypeUnSafeAgg", UdfTypeUnSafeAgg)
    val df3: DataFrame = spark.sql("select count(1),UdfTypeUnSafeAgg(salary) from employee")
    df3.show()

    val aggCol: TypedColumn[Employee, Double] = MyAverage.toColumn.name("MyAverage")
     val df4: Dataset[Double] = employeeDS.select(aggCol)
    df4.show()

    spark.stop()


  }

  class UdfClass extends Function1[String, String] with Serializable {
    override def apply(v1: String): String = {
      StringUtils.upperCase(v1)
    }
  }


  object UdfTypeSafeAgg extends Aggregator[Employee, Average, Double] {
    override def zero: Average = Average(0L, 0L)

    override def reduce(b: Average, a: Employee): Average = {
      val count: Long = b.count + 1
      val sum: Long = b.sum + a.salary
      Average(count, sum)
    }

    override def merge(b1: Average, b2: Average): Average = {
      val count: Long = b1.count + b2.count
      val sum: Long = b1.sum + b2.sum
      Average(count, sum)
    }

    override def finish(reduction: Average): Double = {
      if (reduction.count == 0) {
        0
      } else {
        reduction.sum / reduction.count
      }
    }

    override def bufferEncoder: Encoder[Average] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  object UdfTypeUnSafeAgg extends UserDefinedAggregateFunction {
    // Data types of input arguments of this aggregate function
    def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

    // Data types of values in the aggregation buffer
    def bufferSchema: StructType = {
      StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
    }

    // The data type of the returned value
    def dataType: DataType = DoubleType

    // Whether this function always returns the same output on the identical input
    def deterministic: Boolean = true

    // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
    // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
    // the opportunity to update its values. Note that arrays and maps inside the buffer are still
    // immutable.
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    // Updates the given aggregation buffer `buffer` with new input data from `input`
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
      }
    }

    // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    // Calculates the final result
    def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
  }

  object MyAverage extends Aggregator[Employee, Average, Double] {
    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    def zero: Average = Average(0L, 0L)

    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    def reduce(buffer: Average, employee: Employee): Average = {
      buffer.sum += employee.salary
      buffer.count += 1
      buffer
    }

    // Merge two intermediate values
    def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }

    // Transform the output of the reduction
    def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

    // Specifies the Encoder for the intermediate value type
    def bufferEncoder: Encoder[Average] = Encoders.product

    // Specifies the Encoder for the final output value type
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }


}
