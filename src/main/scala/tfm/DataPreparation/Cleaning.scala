package tfm.DataPreparation

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._
import shapeless.ops.tuple.Length
import tfm.config.Config._


class Cleaning {
  def clean(df: DataFrame):DataFrame={
    val dfLowered = toLower(df, "Queries")
    val dfNlenghtCleaned = nLenWordCleaning(dfLowered, "Queries", 3)


    dfNlenghtCleaned
  }

  def toLower(df: DataFrame, columnName: String): DataFrame = {
    val dfReparted = df.repartition(8)
    dfReparted.withColumn(columnName, lower(col(columnName)).alias(columnName))
  }

  def nLenWordCleaning(df:DataFrame, columnName:String, wordLengthToDelete: Int): DataFrame ={

    val nlengthCleanUDF = udf { s: String =>
      print("String:" + s)
      s.split(" ").filter(x => x.length > wordLengthToDelete).mkString(" ")
    }

    val dfReparted = df.repartition(8)
    dfReparted.withColumn(columnName, nlengthCleanUDF(col(columnName)))
  }

}
