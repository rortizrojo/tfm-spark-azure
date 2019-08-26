package tfm.DataPreparation

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CleaningTest extends FunSuite with DataFrameSuiteBase {

  //      intercept[org.scalatest.exceptions.TestFailedException] {
  //        assertDataFrameEquals(input1, input2) // not equal
  //      }
  test("testApostropheCleaning") {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()


    val listInput = List(

      Row("Paco","Garcia","24", "I'd like to ride a bike"),
      Row("Juan","Garcia","26", "She'd liked to ride a bike"),
      Row("Juan","Garcia","26", "They can't ride a bike"),
      Row("Juan","Garcia","26", "you're riding a bike"),
      Row("Juan","Garcia","26", "He'll ride a bike"),
      Row("Juan","Garcia","26", "That bike is Pedro's"),
      Row("Juan","Garcia","26", "It's the bike I don't want to ride"),
      Row("Lola","Martin","29", "This is Pedro's bike")
    )


    val rddInput = sc.parallelize(listInput)
    val schema = StructType(
      List(
        StructField("nombre", StringType),
        StructField("apellido", StringType),
        StructField("edad", StringType),
        StructField("values", StringType)
      )
    )
    val listExpected = List(
      Row("Paco","Garcia","24", "I would like to ride a bike"),
      Row("Juan","Garcia","26", "She had liked to ride a bike"),
      Row("Juan","Garcia","26", "They cannot ride a bike"),
      Row("Juan","Garcia","26", "you are riding a bike"),
      Row("Juan","Garcia","26", "He will ride a bike"),
      Row("Juan","Garcia","26", "That bike is Pedro"),
      Row("Juan","Garcia","26", "It is the bike I do not want to ride"),
      Row("Lola","Martin","29", "This is Pedro bike")
    )
    val rddExpected = sc.parallelize(listExpected)
    val dfInput : DataFrame = spark.createDataFrame(rddInput,schema).toDF().orderBy("values")
    val dfExpected : DataFrame = spark.createDataFrame(rddExpected,schema).toDF().orderBy("values")

    val result = new Cleaning().apostropheCleaning(dfInput, "values")

    assertDataFrameEquals(dfExpected,result)

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
