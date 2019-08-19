package tfm.DataPreparation

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class FilteringTest extends FunSuite with DataFrameSuiteBase{

  test("testDeleteDuplicates") {

  }

  test("testReduceDuplicatesToOne") {
    import spark.implicits._

    val input = sc.parallelize(Seq(("hola"), ("hola"))).toDF()
    val reslut = new Filtering().reduceDuplicatesToOne(input, "value")
    val expected = sc.parallelize(Seq(("hola"))).toDF()

    assertDataFrameEquals(expected,reslut )
  }

  test("testDeleteNoDataRows") {

  }

}
