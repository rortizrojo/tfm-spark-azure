package tfm.DataPreparation

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lower, regexp_replace, udf}

/**
  * Clase que realiza el preproceso de los datos de un dataframe
  */
class Preprocessing {

  def preprocess(df: DataFrame):DataFrame={

    val logger = Logger.getLogger(this.getClass.getName)
    logger.warn("Preprocesado")

    val column = "Queries"
      val regExp = "[0-9]+"

    val dfFinal = df
      .transform(nullCleaning(Seq(column)))
      .transform(toLower(column))
      .transform(nLenWordCleaning(column,3))
      .transform(startsWithCleaner(column, regExp))
      .transform(replacerCleaning(column, regExp, ""))
      //Reemplazo de puntos por comas en los números decimales para evitar errores en Power BI
      .transform(replacerCleaning("Max_cpc", "\\.", ","))
      .transform(replacerCleaning("User_latitude", "\\.", ","))
      .transform(replacerCleaning("User_longitude", "\\.", ","))
      .transform(replacerCleaning("Avg_cpc", "\\.", ","))
      .transform(replacerCleaning("Avg_position", "\\.", ","))
      .transform(replacerCleaning("Cost_keyword", "\\.", ","))
      .transform(replacerCleaning("Cost", "\\.", ","))
      .transform(replacerCleaning("Net_revenue", "\\.", ","))
      .transform(replacerCleaning("Profit_unitario", "\\.", ","))
      .transform(replacerCleaning("ROI", "\\.", ","))
      .transform(regularExprCleaning(column, regExp))
      .transform(specialCharCleaning(column))

    dfFinal
  }

  /**
    * Elimina filas de un dataframe que contienen nulos en las columnas indicada.
    *
    * @param df DataFrame con los datos de entrada.
    * @param columns Columnas en la que hay que realizar el reemplazo.
    * @return El DataFrame con la columna indicada modificada o añadida si no existe.
    */
  def nullCleaning(columns: Seq[String])(df: DataFrame):DataFrame={
   // df.filter(col(column).isNotNull) //Son equivalentes
     df.na.drop(columns)
  }

  /**
    * Pasa el texto de una columna de un dataframe a minúsculas
    *
    * @param df DataFrame con los datos de entrada.
    * @param column Columna en la que hay que realizar el reemplazo.
    * @return El DataFrame con la columna indicada modificada o añadida si no existe.
    */
  def toLower(column: String)(df: DataFrame): DataFrame = {
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
  def nLenWordCleaning(column:String, wordLengthToDelete: Int)(df:DataFrame): DataFrame ={

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
  def specialCharCleaning(column: String)(df:DataFrame): DataFrame = {
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
  def regularExprCleaning(column: String, regExpr: String)(df:DataFrame): DataFrame = {
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
  def replacerCleaning(column: String,regExpr: String, replacement: String)(df:DataFrame): DataFrame = {
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
  def startsWithCleaner(column: String, regExpr: String)(df:DataFrame): DataFrame = {
    df.withColumn(column,regexp_replace(col(column),"^" + regExpr, "" ))
  }
}
