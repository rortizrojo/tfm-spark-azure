package samples

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.junit.Test
import org.scalatest.FunSuite

@Test
class CleanerTest extends FunSuite with DataFrameSuiteBase {

  @Test
  def testCleaner(): Unit ={
    test("test") {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      val input1 = sc.parallelize(List(1, 2, 3)).toDF
      assertDataFrameEquals(input1, input1) // equal

      val input2 = sc.parallelize(List(4, 5, 6)).toDF

      val input3 = sc.parallelize(List(4, 5, 6)).toDF

      intercept[org.scalatest.exceptions.TestFailedException] {
        assertDataFrameEquals(input1, input2) // not equal
      }


      assertDataFrameEquals(input2, input3) // not equal

    }
  }


}
