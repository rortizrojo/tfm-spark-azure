package tfm.DataPreparation

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count}

/**
  * Clase que realiza sobre un dataframe el filtrado de filas no deseadas
  */
class Filtering {

  def filter(df:DataFrame, column : String): DataFrame ={
    val logger = Logger.getLogger(this.getClass.getName)
    logger.warn("Filtrado")

    //Elegir uno de los dos siguientes algoritmos, no los dos
    df
      .transform(reduceDuplicatesToOne(column))
      .transform(deleteNoDataRows(column))
      //.transform(deleteDuplicates(column))

  }

  /**
    * Elimina filas de un dataframe que contiene duplicados en la columna indicada dejando una sola fila
    *
    * @param df DataFrame con los datos de entrada.
    * @param column Columna en la que hay que realizar el proceso.
    * @return El DataFrame con las filas duplicadas eliminadas excepto una
    */
  def reduceDuplicatesToOne(column: String)(df:DataFrame): DataFrame = {
    df.dropDuplicates(column)
  }

  /**
    * Elimina filas de un dataframe que contiene duplicados en la columna indicada no dejando ninguna de las filas con duplicados
    *
    * @param df DataFrame con los datos de entrada.
    * @param column Columna en la que hay que realizar el proceso.
    * @return El DataFrame con las filas duplicadas eliminadas
    */
  def deleteDuplicates(column: String)(df:DataFrame): DataFrame = {
    df
      .withColumn("cnt", count("*").over(Window.partitionBy(col(column))))
      .where(col("cnt")===1).drop(col("cnt"))
  }

  /**
    * Elimina filas de un dataframe que no tiene datos en la columna indicada. Por ejemplo, si tiene solamente espacios, tabulaciones o saltos de línea
    *
    * @param df DataFrame con los datos de entrada.
    * @param column Columna en la que hay que realizar el proceso.
    * @return El DataFrame con las filas sin datos eliminadas
    */
  def deleteNoDataRows(column: String)(df:DataFrame): DataFrame = {
    df.filter(!col(column).rlike("^(\\s|\t|\n)*$"))
  }

}
