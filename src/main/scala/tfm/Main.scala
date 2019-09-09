package tfm

import org.apache.log4j.Logger
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import org.joda.time.Period
import org.joda.time.format.DateTimeFormat
import tfm.DataPreparation.{Cleaning, Filtering, Preprocessing}
import org.apache.spark.sql.functions.col
import tfm.config.Config
import tfm.ML.CountClassifier


object Main {
  def main(args : Array[String]) {
    import org.joda.time.DateTime

    val date: String = DateTimeFormat.forPattern("yyyy-MM-dd-HHmmSS").print(DateTime.now())
    val timeStart =DateTime.now()
    val logger = Logger.getLogger(this.getClass.getName)
    val spark = Config.spark
    spark.sparkContext.setLogLevel("WARN")


    val dfInput = getInputData(args(0), "\t")

    /** Cleaning, Filtering, Preprocessing **/
    logger.warn("Inicio de proceso de limpieza")
    val dfPreprocessed = new Preprocessing().preprocess(dfInput)
    val dfPreprocessedFiltered = new Filtering().filter(dfPreprocessed)
    val dfPreprocessedFilteredCleaned = new Cleaning().clean(dfPreprocessedFiltered)

    /** Machine Learning **/
    val model = trainModel(dfPreprocessedFilteredCleaned, "Keyword_match_type", "Queries")
    val dfOutput = model.transform(dfPreprocessedFilteredCleaned)

    logger.warn(s"Number of partitions: ${dfOutput.rdd.getNumPartitions}")

    dfOutput
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("delimiter", "\t")
      .option("header", "true")
      .save(s"output/${date}-output.csv")
    logger.warn("Finalizado proceso Spark")

    val timeEnd =DateTime.now()
    val p = new Period(timeStart, timeEnd )
    logger.warn("Total time elapsed: %02d:%02d:%02d.%03d".format(p.getHours, p.getMinutes, p.getSeconds, p.getMillis))

  }

  /**
    * Función que realiza el entrenaminento de un modelo con la clase CountClassifier y la prueba del modelo
    *
    * @param dfData Dataframe para realizar el entrenamiento y el test
    * @param columnToTrain Columna de clasificación o etiquetada
    * @param columnToClassificate
    * @return
    */
  def trainModel(dfData: DataFrame, columnToTrain: String, columnToClassificate: String): PipelineModel ={
    val Array(trainDf, testDf) = dfData.randomSplit(Array(0.8, 0.2))
    val model = CountClassifier.train(trainDf,columnToTrain,  columnToClassificate)
    CountClassifier.test(model, testDf)
    model
  }

  /**
    * Devuelve un dataframe a partir de un fichero csv
    * @param pathFile Ruta HDFS (Azure Data Lake Storage Gen2) al fichero
    * @return DataFrame con los datos que contiene el csv
    */
  def getInputData(pathFile: String, sep: String): DataFrame ={

    val dfInput = tfm.config.Config.spark.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", true)
      .option("header", true)
      .option("delimiter",sep)
      .csv(pathFile)

    dfInput.printSchema()
    dfInput
  }
}
