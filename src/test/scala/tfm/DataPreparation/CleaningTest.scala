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


  test("testAttachedWordsCleaning") {
    import spark.implicits._

    val input1 = sc.parallelize(Seq( ("hola"), ("GoodBye my friend"))).toDF()
    val input2 = sc.parallelize(Seq( ("hola"), ("GoodBye my FRIEND"))).toDF()
    val result1 = new Cleaning().attachedWordsCleaning(input1, "value")
    val result2 = new Cleaning().attachedWordsCleaning(input2, "value")
    val expected1 = sc.parallelize(Seq( ("hola"), ("Good Bye my friend"))).toDF()
    val expected2 = sc.parallelize(Seq( ("hola"), ("Good Bye my FRIEND"))).toDF()

    assertDataFrameEquals(expected1,result1)
    assertDataFrameEquals(expected2,result2)
  }

  test("testCharactersCleaning") {
    import spark.implicits._
    val charList = List('ç', 'ñ')

    val input = sc.parallelize(Seq(("çhola"), ("byñe"))).toDF()
    val result = new Cleaning().charactersCleaning(input, "value",charList ).orderBy("value")
    val expected = sc.parallelize(Seq(("hola"), ("bye"))).toDF().orderBy("value")

    assertDataFrameEquals(expected,result )

  }

  test("testExpressionsCleaning") {

  }

  test("testUrlCleaning") {

  }

  test("testHtmlCleaning") {

  }


  test("testStandarizingCleaning") {

  }

  test("testInitialFinalApostropheCleaning") {

  }

  test("testGrammarCleaning") {

  }





  test("testDecodingCleaning") {

  }

  test("testSlangCleaning") {

  }

  test("testContentCleaning") {

  }

}
