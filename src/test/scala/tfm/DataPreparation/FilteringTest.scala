package tfm.DataPreparation

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class FilteringTest extends FunSuite with DataFrameSuiteBase{

  test("testDeleteDuplicates") {
    import spark.implicits._

    val input = sc.parallelize(Seq(("hola"), ("hola"), ("adiós"), ("Adiós"))).toDF()
    val reslut = new Filtering().deleteDuplicates(input, "value").orderBy("value")
    val expected = sc.parallelize(Seq(("adiós"), ("Adiós"))).toDF().orderBy("value")

    assertDataFrameEquals(expected,reslut )
  }

  test("testReduceDuplicatesToOne") {
    import spark.implicits._

    val input = sc.parallelize(Seq(("hola"), ("hola"), ("adiós"), ("Adiós"))).toDF()
    val reslut = new Filtering().reduceDuplicatesToOne(input, "value").orderBy("value")
    val expected = sc.parallelize(Seq(("hola"),("adiós"), ("Adiós"))).toDF().orderBy("value")

    assertDataFrameEquals(expected,reslut )
  }

  test("testDeleteNoDataRows") {

  }

}
