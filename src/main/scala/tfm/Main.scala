package tfm

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import tfm.config.Config
import org.apache.spark.sql.functions.col
import tfm.DataPreparation.{Cleaning, Filtering, Preprocessing}


object Main {
  def main(args : Array[String]) {

    val spark = Config.spark
    spark.sparkContext.setLogLevel("WARN")

    val pathFile = "input/muestraFicheroPequenya.csv"
    val df = spark.read.option("header", true).option("delimiter","\t").csv(pathFile)

    val dfCleaned = new Cleaning().clean(df)
    val dfCleanedFiltered = new Filtering().filter(dfCleaned)
    val dfCleanedFilteredPreprocessed = new Preprocessing().preprocess(dfCleanedFiltered)

    dfCleanedFilteredPreprocessed
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save("mydata.csv")
  }
}
