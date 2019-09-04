package tfm

import org.apache.log4j.Logger
import org.joda.time.Period
import org.joda.time.format.DateTimeFormat
import tfm.DataPreparation.{Cleaning, Filtering, Preprocessing}
import org.apache.spark.sql.functions.col
import tfm.config.Config


object Main {
  def main(args : Array[String]) {
    import org.joda.time.DateTime

    val date: String = DateTimeFormat.forPattern("yyyy-MM-dd-HHmmSS").print(DateTime.now())
    val timeStart =DateTime.now()

    val logger = Logger.getLogger(this.getClass.getName)
    logger.warn("Inicio de proceso de limpieza")
    val spark = Config.spark
   // spark.sparkContext.setLogLevel("WARN")

    val pathFile = "input/muestraSubido.csv"
    val dfInput = spark.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", true)
      .option("header", true)
      .option("delimiter","\t")
      .csv(pathFile)

    dfInput.printSchema()

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
      .option("delimiter", "\t")
      .option("header", "true")
      .save(s"output/${date}-output.csv")
    logger.warn("Finalizado proceso de limpieza")

    val timeEnd =DateTime.now()

    val p = new Period(timeStart, timeEnd )
    logger.warn("Total time elapsed: %02d:%02d:%02d.%03d".format(p.getHours, p.getMinutes, p.getSeconds, p.getMillis))

  }
}
