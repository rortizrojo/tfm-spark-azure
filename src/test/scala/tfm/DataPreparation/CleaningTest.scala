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

    val listInput = Seq(
      ("I'd like to ride a bike"),
      ("She'd liked to ride a bike"),
      ("They can't ride a bike"),
      ("you're riding a bike"),
      ("He'll ride a bike"),
      ("That bike is Pedro's"),
      ("It's the bike I don't want to ride"),
      ("This is Pedro's bike")
    )

    val listExpected = Seq(
      ("I would like to ride a bike"),
      ("She had liked to ride a bike"),
      ("They cannot ride a bike"),
      ("you are riding a bike"),
      ("He will ride a bike"),
      ("That bike is Pedro"),
      ("It is the bike I do not want to ride"),
      ("This is Pedro bike")
    )


    val input = sc.parallelize(listInput).toDF().orderBy("value")
    val result = new Cleaning().apostropheCleaning(input, "value")
    val expected = sc.parallelize(listExpected).toDF().orderBy("value")
    assertDataFrameEquals(expected,result)

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
