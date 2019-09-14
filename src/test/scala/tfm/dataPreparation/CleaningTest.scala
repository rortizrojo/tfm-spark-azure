package tfm.dataPreparation

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

    val result = new Cleaning().apostropheCleaning("values")(dfInput)

    assertDataFrameEquals(dfExpected,result)

  }


  test("testAttachedWordsCleaning") {
    import spark.implicits._

    val input1 = sc.parallelize(Seq("hola", "GoodBye my friend")).toDF()
    val input2 = sc.parallelize(Seq( "hola", "GoodBye my FRIEND")).toDF()
    val result1 = new Cleaning().attachedWordsCleaning("value")(input1)
    val result2 = new Cleaning().attachedWordsCleaning("value")(input2)
    val expected1 = sc.parallelize(Seq( "hola", "Good Bye my friend")).toDF()
    val expected2 = sc.parallelize(Seq( "hola", "Good Bye my FRIEND")).toDF()

    assertDataFrameEquals(expected1,result1)
    assertDataFrameEquals(expected2,result2)
  }

  test("testCharactersCleaning") {
    import spark.implicits._
    val charList = List('ç', 'ñ')

    val input = sc.parallelize(Seq("çhola", "byñe")).toDF()
    val result = new Cleaning().charactersCleaning("value",charList)(input).orderBy("value")
    val expected = sc.parallelize(Seq("hola", "bye")).toDF().orderBy("value")

    assertDataFrameEquals(expected,result )

  }

  test("testContentCleaning") {
    import spark.implicits._
    val wordList = List("good","bad")

    val input = sc.parallelize(Seq("holabad", "good bye")).toDF()
    val result = new Cleaning().contentCleaning("value", wordList)(input).orderBy("value")
    val expected = sc.parallelize(Seq("holabad", " bye")).toDF().orderBy("value")

    assertDataFrameEquals(expected,result )

  }

  test("testDecodingCleaning") {
    import spark.implicits._

    val input = sc.parallelize(Seq("hola 編", "bye")).toDF()
    val result = new Cleaning().decodingCleaning("value")(input).orderBy("value")
    val expected = sc.parallelize(Seq("hola ", "bye")).toDF().orderBy("value")

    assertDataFrameEquals(expected,result )
  }

  test("testExpressionsCleaning") {
    import spark.implicits._
    val expressionList = List("good bye","bad", "friend")


    val input = sc.parallelize(Seq("hola", "good bye my friend")).toDF()
    val result = new Cleaning().expressionsCleaning("value", expressionList)(input).orderBy("value")
    val expected = sc.parallelize(Seq("hola", " my ")).toDF().orderBy("value")

    assertDataFrameEquals(expected,result )
  }

  test("testGrammarCleaning") {

  }

  test("testHtmlCleaning") {
    import spark.implicits._
    val aditionalTokens = List("&bnsp;")

    val input = sc.parallelize(Seq("hola", "<html>&bnsp;hola</html>")).toDF()
    val result = new Cleaning().htmlCleaning("value", aditionalTokens)(input).orderBy("value")
    val expected = sc.parallelize(Seq("hola", "hola")).toDF().orderBy("value")

    assertDataFrameEquals(expected,result )
  }

  test("testInitialFinalApostropheCleaning") {
    import spark.implicits._

    val input = sc.parallelize(Seq("'hola'", "'ho'l'a'")).toDF()
    val result = new Cleaning().initialFinalApostropheCleaning("value")(input).orderBy("value")
    val expected = sc.parallelize(Seq("hola", "ho'l'a")).toDF().orderBy("value")

    assertDataFrameEquals(expected,result )
  }

  test("testSlangCleaning") {
    import spark.implicits._

    val input = sc.parallelize(Seq("hola caracola", "AFAIK")).toDF()
    val result = new Cleaning().slangCleaning("value")(input).orderBy("value")
    val expected = sc.parallelize(Seq("hola caracola", "As Far As I Know")).toDF().orderBy("value")

    assertDataFrameEquals(expected,result )
  }

  test("testStandarizingCleaning") {

  }

  test("testUrlCleaning") {
    import spark.implicits._

    val input1 = sc.parallelize(Seq("hola", "http://aaa.com https://bbb.com:8080 www.google.es hola")).toDF()
    val input2 = sc.parallelize(Seq("hola", "www.gplan.co.uk https://subdominio.bbb.com:8080 clogau.co.uk hola")).toDF()
    val input3 = sc.parallelize(Seq("hola", "http://aaa.com https://subdominio.bbb.com:8080 hola")).toDF()
    val result1 = new Cleaning().urlCleaning("value", "OFF")(input1).orderBy("value")
    val result2 = new Cleaning().urlCleaning("value", "ON")(input2).orderBy("value")
    val result3 = new Cleaning().urlCleaning("value", "CAPITALIZED")(input3).orderBy("value")
    val expected1 = sc.parallelize(Seq("hola", "   hola")).toDF().orderBy("value")
    val expected2 = sc.parallelize(Seq("hola", "gplan bbb clogau hola")).toDF().orderBy("value")
    val expected3 = sc.parallelize(Seq("hola", "Aaa Bbb hola")).toDF().orderBy("value")

    assertDataFrameEquals(expected1,result1 )
    assertDataFrameEquals(expected2,result2 )
    assertDataFrameEquals(expected3,result3 )
  }












}
