package tfm

import org.apache.log4j.Logger
import tfm.DataPreparation.{Cleaning, Filtering, Preprocessing}
import tfm.config.Config


object Main {
  def main(args : Array[String]) {

    val logger = Logger.getLogger(this.getClass.getName)
    logger.warn("Inicio de proceso de limpieza")
    val spark = Config.spark
   // spark.sparkContext.setLogLevel("WARN")

    val pathFile = "input/muestraSubido.csv"
    val df = spark.read.option("header", true).option("delimiter","\t").csv(pathFile)

    logger.warn("Preprocesado")
    val dfPreprocessed = new Preprocessing().preprocess(df)
    logger.warn("Filtrado")
    val dfPreprocessedFiltered = new Filtering().filter(dfPreprocessed)
    logger.warn("Limpieza")
    val dfPreprocessedFilteredCleaned = new Cleaning().clean(dfPreprocessedFiltered)

    dfPreprocessedFilteredCleaned
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save("mydata.csv")
    logger.warn("Finalizado proceso de limpieza")
  }
}
