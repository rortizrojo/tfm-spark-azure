package tfm

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.junit.runner.RunWith
import org.junit.{Assert, Test}
import org.scalacheck.Prop.False
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatestplus.junit.JUnitRunner
import tfm.DataPreparation.Cleaning

@RunWith(classOf[JUnitRunner])
class CleaningTest extends FunSuite with DataFrameSuiteBase with BeforeAndAfter {

  //      intercept[org.scalatest.exceptions.TestFailedException] {
  //        assertDataFrameEquals(input1, input2) // not equal
  //      }

  test("Pasar a minúsculas una columna de un DataFrame") {
    import spark.implicits._

    val input = sc.parallelize(Seq(("HOLA", "PePe"),("juan","Mama"), ("JuaN","peppito"))).toDF()

    val reslut1 = new Cleaning().toLower(input, "_1")
    val reslut2 = new Cleaning().toLower(input, "_2")

    val expected1 = sc.parallelize(Seq(("hola", "PePe"),("juan","Mama"),("juan","peppito"))).toDF()
    val expected2 = sc.parallelize(Seq(("HOLA", "pepe"),("juan","mama"),("JuaN","peppito"))).toDF()

    assertDataFrameEquals(reslut1, expected1)
    assertDataFrameEquals(reslut2, expected2)

  }


  test("Eliminar palabras con menos de n letras") {
    import spark.implicits._

    val input = sc.parallelize(Seq(" a aa aaaaaa", "a aa aaa aaaa")).toDF()

    val reslut1 = new Cleaning().nLenWordCleaning(input, "value", 4)
    val reslut2 = new Cleaning().nLenWordCleaning(input, "value", 2)

    val expected1 = sc.parallelize(Seq("aaaaaa", "")).toDF()
    val expected2 = sc.parallelize(Seq("aaaaaa", "aaa aaaa")).toDF()

    assertDataFrameEquals(expected1,reslut1)
    assertDataFrameEquals(expected2,reslut2)

  }





}
