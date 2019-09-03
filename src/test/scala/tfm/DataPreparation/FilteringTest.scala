package tfm.DataPreparation

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class FilteringTest extends FunSuite with DataFrameSuiteBase{

  test("Elimina filas de un dataframe que contiene duplicados en la columna indicada no dejando ninguna de las filas con duplicados") {
    import spark.implicits._

    val input = sc.parallelize(Seq(("hola"), ("hola"), ("adiós"), ("Adiós"))).toDF()
    val result = new Filtering().deleteDuplicates("value")(input).orderBy("value")
    val expected = sc.parallelize(Seq(("adiós"), ("Adiós"))).toDF().orderBy("value")

    assertDataFrameEquals(expected,result )
  }

  test("Elimina filas de un dataframe que contiene duplicados en la columna indicada dejando una sola fila") {
    import spark.implicits._

    val input = sc.parallelize(Seq(("hola"), ("hola"), ("adiós"), ("Adiós"))).toDF()
    val result = new Filtering().reduceDuplicatesToOne("value")(input).orderBy("value")
    val expected = sc.parallelize(Seq(("hola"),("adiós"), ("Adiós"))).toDF().orderBy("value")

    assertDataFrameEquals(expected,result )
  }

  test("Elimina filas de un dataframe que no tiene datos en la columna indicada. Por ejemplo, si tiene solamente espacios, tabulaciones o saltos de línea") {
    import spark.implicits._

    val input = spark.sparkContext.parallelize(
      Seq(
        ("hola", "1"),
        ("Hola amigos", "2"),
        (" ", "3"),
        ("", "4"),
        ("\t", "5"),
        ("\n", "6")
      )
    ).toDF()
    val result = new Filtering().deleteNoDataRows( "_1")(input).orderBy("_1")
    val expected = sc.parallelize(Seq(
      ("hola", "1"),
      ("Hola amigos", "2"))).toDF().orderBy("_1")

    assertDataFrameEquals(expected,result )
  }

}
