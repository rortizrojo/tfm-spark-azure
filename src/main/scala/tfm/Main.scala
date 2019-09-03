package tfm

import org.apache.log4j.Logger
import org.joda.time.format.DateTimeFormat
import tfm.DataPreparation.{Cleaning, Filtering, Preprocessing}
import tfm.config.Config


object Main {
  def main(args : Array[String]) {
    import org.joda.time.DateTime

    val date: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now())

    val logger = Logger.getLogger(this.getClass.getName)
    logger.warn("Inicio de proceso de limpieza")
    val spark = Config.spark
   // spark.sparkContext.setLogLevel("WARN")

    val pathFile = "input/muestraSubido.csv"
    val dfInput = spark.read.option("header", true).option("delimiter","\t").csv(pathFile)

    logger.warn("Preprocesado")
    val dfPreprocessed = new Preprocessing().preprocess(dfInput)
    logger.warn("Filtrado")
    val dfPreprocessedFiltered = new Filtering().filter(dfPreprocessed)
    logger.warn("Limpieza")
    val dfPreprocessedFilteredCleaned = new Cleaning().clean(dfPreprocessedFiltered)

    logger.warn(s"Number of partitions: ${dfPreprocessedFilteredCleaned.rdd.getNumPartitions}")

    dfPreprocessedFilteredCleaned
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .save(s"output/${date}-output.csv")
    logger.warn("Finalizado proceso de limpieza")
  }
}
