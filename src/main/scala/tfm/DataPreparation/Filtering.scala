package tfm.DataPreparation

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col,count}

class Filtering {


  def filter(df:DataFrame): DataFrame ={

    val logger = Logger.getLogger(this.getClass.getName)
    val column = "Queries"

    //Elegir uno de los dos siguientes algoritmos, no los dos
    val dfReduced = reduceDuplicatesToOne(df, column)
    //val dfDeletedDuplicates = deleteDuplicates(df, column)
    val dfDeletedNullRows = deleteNoDataRows(df, column)
    dfDeletedNullRows
  }

  /**
    *
    * @param df
    * @param column
    * @return
    */
  def reduceDuplicatesToOne(df:DataFrame, column: String): DataFrame = {
    df.dropDuplicates(column)
  }

  /**
    *
    * @param df
    * @param column
    * @return
    */
  def deleteDuplicates(df:DataFrame, column: String): DataFrame = {


    df
      .withColumn("cnt", count("*").over(Window.partitionBy(col(column))))
      .where(col("cnt")===1).drop(col("cnt"))

  }

  /**
    *
    * @param df
    * @param column
    * @return
    */
  def deleteNoDataRows(df:DataFrame, column: String): DataFrame = {
    ???
  }

}
