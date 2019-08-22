package tfm.DataPreparation

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CleaningTest extends FunSuite with DataFrameSuiteBase {

  //      intercept[org.scalatest.exceptions.TestFailedException] {
  //        assertDataFrameEquals(input1, input2) // not equal
  //      }
  test("testApostropheCleaning") {
    import spark.implicits._

    val input = spark.sparkContext.parallelize(
      Seq(
        ("I'd like to ride a bike")
      )
    ).toDF()
    val result = new Cleaning().apostropheCleaning(input, "value")
    val expected = sc.parallelize(Seq(
      ("hola", "1"),
      ("Hola amigos", "2"))).toDF().orderBy("_1")

    assertDataFrameEquals(expected,result )

  }

  test("testExpressionsCleaning") {

  }

  test("testUrlCleaning") {

  }

  test("testHtmlCleaning") {

  }

  test("testAttachedWordsCleaning") {

  }

  test("testStandarizingCleaning") {

  }

  test("testInitialFinalApostropheCleaning") {

  }

  test("testGrammarCleaning") {

  }


  test("testCharactersCleaning") {

  }

  test("testDecodingCleaning") {

  }

  test("testSlangCleaning") {

  }

  test("testContentCleaning") {

  }

}
