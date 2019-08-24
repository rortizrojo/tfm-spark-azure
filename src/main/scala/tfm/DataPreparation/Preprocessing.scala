package tfm.DataPreparation

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lower, regexp_replace, udf}

class Preprocessing {

  def preprocess(df: DataFrame):DataFrame={
    val logger = Logger.getLogger(this.getClass.getName)

    val column = "Queries"
    val regExp = "[0-9]+"

    val dfReparted = df.repartition(8)
    logger.warn("Numero de elementos antes de limpieza: " + dfReparted.count())
    val dfNullCleaned = nullCleaning(dfReparted, column)
    logger.warn("Numero de elementos despues de limpieza: " + dfNullCleaned.count())
    val dfLowered = toLower(dfNullCleaned, column)
    val dfNlenghtCleaned = nLenWordCleaning(dfLowered, column, 3)
    val dfStartCleaned = startsWithCleaner(dfNlenghtCleaned, column, regExp)
    val dfReplacerCleaning = replacerCleaning(dfStartCleaned,column, regExp, "")
    val dfRegExpression = regularExprCleaning(dfReplacerCleaning, column, regExp)
    val dfSpecialCharCleaning = specialCharCleaning(dfRegExpression, column)

    dfSpecialCharCleaning
  }

  /**
    * Elimina filas de un dataframe que contienen nulos en la columna indicada.
    *
    * @param df DataFrame con los datos de entrada.
    * @param column Columna en la que hay que realizar el reemplazo.
    * @return El DataFrame con la columna indicada modificada o añadida si no existe.
    */
  def nullCleaning(df: DataFrame, column: String):DataFrame={
    df.filter(col(column).isNotNull)
  }

  /**
    * Pasa el texto de una columna de un dataframe a minúsculas
    *
    * @param df DataFrame con los datos de entrada.
    * @param column Columna en la que hay que realizar el reemplazo.
    * @return El DataFrame con la columna indicada modificada o añadida si no existe.
    */
  def toLower(df: DataFrame, column: String): DataFrame = {
    df.withColumn(column, lower(col(column)).alias(column))
  }

  /**
    * Elimina palabras de la columna indicada que sean menores o iguales que la longitud indicada
    *
    * @param df DataFrame con los datos de entrada.
    * @param column Columna en la que hay que realizar el reemplazo.
    * @param wordLengthToDelete Longitud máxima de una palabra
    * @return El DataFrame con la columna indicada modificada o añadida si no existe.
    */
  def nLenWordCleaning(df:DataFrame, column:String, wordLengthToDelete: Int): DataFrame ={

    val nlengthCleanUDF = udf { s: String =>
      //print("Raw:" + s)
      val cleaned = s.split(" ").filter(x => x.length > wordLengthToDelete).mkString(" ")
      //println("--> " + cleaned)
      cleaned
    }
    df.withColumn(column, nlengthCleanUDF(col(column)))
  }

  /**
    * Reemplaza cada carácter espcial de una columna de un DataFrame por un espacio
    * @param df DataFrame con los datos de entrada.
    * @param column Columna en la que hay que realizar el reemplazo.
    * @return El DataFrame con la columna indicada modificada o añadida si no existe.
    */
  def specialCharCleaning(df: DataFrame, column: String): DataFrame = {
    val special = "(!|\"|#|\\$|%|&|'|\\(|\\)|\\*|\\+|,|-|\\.|\\/|:|;|<|=|>|\\?|@|\\[|\\\\|\\]|\\^|_|`|\\{|\\||\\}|~)"
    df.withColumn(column, regexp_replace(col(column),special, " "))
  }


  /**
    * Elimina palabras de la columna indicada de un DataFrame en las que se encuentre coincidencia con una expresión regular dada
    *
    * @param df DataFrame con los datos de entrada.
    * @param column Columna en la que hay que realizar el reemplazo.
    * @param regExpr Expresión regular para el reemplazo.
    * @return El DataFrame con la columna indicada modificada o añadida si no existe.
    */
  def regularExprCleaning(df: DataFrame,column: String, regExpr: String): DataFrame = {
    import scala.util.matching.Regex

    val keyValPattern:Regex = regExpr.r

    val regularExprCleaningUDF = udf { s: String =>
      s.split(" ").map(x =>keyValPattern.findFirstIn(x) match{
        case Some(_) => ""
        case None => x
      }).mkString(" ")
    }
    df.withColumn(column, regularExprCleaningUDF(col(column)))
  }


  /**
    * Reemplaza el texto de la columna indicada de un DataFrame que coincida con una expresión regular por otra cadena dada
    *
    * @param df DataFrame con los datos de entrada.
    * @param column Columna en la que hay que realizar el reemplazo.
    * @param regExpr Expresión regular para el reemplazo.
    * @param replacement Cadena que reemplaza el texto que coincida con la expresión regular.
    * @return El DataFrame con la columna indicada modificada o añadida si no existe.
    */
  def replacerCleaning(df: DataFrame, column: String,regExpr: String, replacement: String): DataFrame = {
    df.withColumn(column,regexp_replace(col(column),regExpr, replacement ))
  }

  /**
    * Elimina de la columna indicada de un DataFrame el inicio de una cadena que coincida con la expresión regular dada
    *
    * @param df DataFrame con los datos de entrada.
    * @param column Columna en la que hay que realizar la limpieza.
    * @param regExpr Expresión regular para la eliminación del inicio de la cadena.
    * @return El DataFrame con la columna indicada modificada o añadida si no existe.
    */
  def startsWithCleaner(df: DataFrame, column: String, regExpr: String): DataFrame = {
    df.withColumn(column,regexp_replace(col(column),"^" + regExpr, "" ))
  }
}
