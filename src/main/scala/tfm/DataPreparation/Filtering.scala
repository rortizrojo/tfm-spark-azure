package tfm.DataPreparation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count}

class Filtering {

  def filter(df:DataFrame): DataFrame ={

    val column = "Queries"

    //Elegir uno de los dos siguientes algoritmos, no los dos
    val dfReduced = reduceDuplicatesToOne(df, column)
    //val dfDeletedDuplicates = deleteDuplicates(df, column)
    val dfDeletedNullRows = deleteNoDataRows(dfReduced, column)
    dfDeletedNullRows
  }

  /**
    * Elimina filas de un dataframe que contiene duplicados en la columna indicada dejando una sola fila
    *
    * @param df DataFrame con los datos de entrada.
    * @param column Columna en la que hay que realizar el proceso.
    * @return El DataFrame con las filas duplicadas eliminadas excepto una
    */
  def reduceDuplicatesToOne(df:DataFrame, column: String): DataFrame = {
    df.dropDuplicates(column)
  }

  /**
    * Elimina filas de un dataframe que contiene duplicados en la columna indicada no dejando ninguna de las filas con duplicados
    *
    * @param df DataFrame con los datos de entrada.
    * @param column Columna en la que hay que realizar el proceso.
    * @return El DataFrame con las filas duplicadas eliminadas
    */
  def deleteDuplicates(df:DataFrame, column: String): DataFrame = {
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
  def deleteNoDataRows(df:DataFrame, column: String): DataFrame = {
    df.filter(!col(column).rlike("^(\\s|\t|\n)*$"))
  }

}
