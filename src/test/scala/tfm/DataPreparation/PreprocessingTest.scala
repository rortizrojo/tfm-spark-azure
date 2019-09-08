package tfm.DataPreparation

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PreprocessingTest extends FunSuite with DataFrameSuiteBase {

  test("Pasar a minúsculas una columna de un DataFrame") {
    import spark.implicits._

    val input = sc.parallelize(Seq(("HOLA", "PePe"),("juan","Mama"), ("JuaN","peppito"))).toDF()

    val reslut1 = new Preprocessing().toLower("_1")(input)
    val reslut2 = new Preprocessing().toLower("_2")(input)

    val expected1 = sc.parallelize(Seq(("hola", "PePe"),("juan","Mama"),("juan","peppito"))).toDF()
    val expected2 = sc.parallelize(Seq(("HOLA", "pepe"),("juan","mama"),("JuaN","peppito"))).toDF()

    assertDataFrameEquals(expected1,reslut1 )
    assertDataFrameEquals(expected2,reslut2 )

  }

  test("Eliminar palabras con menos de n letras") {
    import spark.implicits._

    val input = sc.parallelize(Seq(" a aa aaaaaa", "a aa aaa aaaa")).toDF()

    val reslut1 = new Preprocessing().nLenWordCleaning("value", 4)(input)
    val reslut2 = new Preprocessing().nLenWordCleaning("value", 2)(input)

    val expected1 = sc.parallelize(Seq("aaaaaa", "")).toDF()
    val expected2 = sc.parallelize(Seq("aaaaaa", "aaa aaaa")).toDF()

    assertDataFrameEquals(expected1,reslut1)
    assertDataFrameEquals(expected2,reslut2)

  }

  test("Eliminación de filas cuyo valor en la columna indicada es null"){
    import spark.implicits._
    val input = sc.parallelize(Seq((null, "PePe"),("juan",null), ("JuaN","peppito"))).toDF()

    val reslut1 = new Preprocessing().nullCleaning(Seq("_1"))(input)
    val reslut2 = new Preprocessing().nullCleaning(Seq("_2"))(input)

    val expected1 = sc.parallelize(Seq(("juan",null), ("JuaN","peppito"))).toDF()
    val expected2 = sc.parallelize(Seq((null, "PePe"),("JuaN","peppito"))).toDF()

    assertDataFrameEquals(expected1,reslut1)
    assertDataFrameEquals(expected2,reslut2)
  }


  test("Elimina el inicio de una cadena que coincida con una expresión regular") {
    import spark.implicits._

    val regExpr = "[^A-Za-z0-9]+"
    val input = sc.parallelize(Seq((" - hola", "    -hola1  -  "), ("   112334r","peppito"))).toDF()

    val reslut1 = new Preprocessing().startsWithCleaner("_1", regExpr)(input)
    val reslut2 = new Preprocessing().startsWithCleaner("_2", regExpr)(input)

    val expected1 = sc.parallelize(Seq(("hola", "    -hola1  -  "), ("112334r","peppito"))).toDF()
    val expected2 = sc.parallelize(Seq((" - hola", "hola1  -  "), ("   112334r","peppito"))).toDF()

    assertDataFrameEquals(expected1,reslut1)
    assertDataFrameEquals(expected2,reslut2)

  }

  test("Reemplaza caracteres especiales por espacios") {
    import spark.implicits._
    val input = sc.parallelize(Seq(("hola.% ho\\\\la ho\"la"), ("    -hola1  -  "))).toDF()
    val reslut = new Preprocessing().specialCharCleaning( "value")(input)
    val expected = sc.parallelize(Seq(("hola   ho  la ho la"), ("     hola1     "))).toDF()

    assertDataFrameEquals(expected,reslut)
  }

  test("Reemplaza el texto que coincida con una expresión regular por otra cadena dada") {
    import spark.implicits._
    val input = sc.parallelize(Seq(("hola"), ("12341234 www.google.es hola123"))).toDF()
    val reslut = new Preprocessing().replacerCleaning( "value", "[0-9]+", "xxx")(input)
    val expected = sc.parallelize(Seq(("hola"), ("xxx www.google.es holaxxx"))).toDF()

    assertDataFrameEquals(expected,reslut)
  }

  test("Elimina palabras que coincidan con una expresión regular dada") {
    import spark.implicits._
    val input = sc.parallelize(Seq(("hola"), ("12341234 www.google.es hola123"))).toDF()
    val reslut = new Preprocessing().regularExprCleaning( "value", "[0-9]+$")(input)
    val expected = sc.parallelize(Seq(("hola"), (" www.google.es "))).toDF()

    assertDataFrameEquals(expected,reslut)
  }

}
