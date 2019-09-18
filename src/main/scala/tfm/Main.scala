package tfm

import org.apache.log4j.Logger
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import org.joda.time.Period
import org.joda.time.format.DateTimeFormat
import tfm.dataPreparation.{Cleaning, Filtering, Preprocessing}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import tfm.config.Config
import tfm.ml.CountClassifier
import tfm.ml.SimilartyCalculation


object Main {
  def main(args : Array[String]) {
    import org.joda.time.DateTime

    val date: String = DateTimeFormat.forPattern("yyyy-MM-dd-HHmmSS").print(DateTime.now())
    val timeStart =DateTime.now()
    val logger = Logger.getLogger(this.getClass.getName)
    val spark = Config.spark
    spark.sparkContext.setLogLevel("WARN")

    val columnQuery = args(2)//"Query"
    val columnKeyword = args(3)//"Keyword"
    val categoria = args(4)//"Categoría"

    val dfInput = getInputData(args(0), args(1))
        //.select("ID", categoria, columnQuery, columnKeyword)
        //.sample(0.3)


    logger.warn("Filas leídas: " + dfInput.count())
    /** Cleaning, Filtering, Preprocessing **/
    logger.warn("Inicio de proceso de limpieza")
    val dfPreprocessed = new Preprocessing().preprocess(dfInput,columnQuery, columnKeyword)
    val dfPreprocessedFiltered = new Filtering().filter(dfPreprocessed, columnQuery)
    val dfPreprocessedFilteredCleaned = new Cleaning().clean(dfPreprocessedFiltered, columnQuery)

    /** Machine Learning - Clasificación  **/
    val dfClassified = trainModel(dfPreprocessedFilteredCleaned, categoria, columnQuery)

    /** Machine Learning - Similitud entre columnas  **/
    val dfOutput = new SimilartyCalculation().calculateSimilarity(dfClassified, dfClassified,  columnQuery, columnKeyword )

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
    * Función que clasifica textos entrenando un modelo con la clase CountClassifier y la prueba del modelo
    *
    * @param dfData Dataframe para realizar el entrenamiento y el test
    * @param columnCategory Columna de clasificación o categoría
    * @param columnFeatures Columna de features o características
    * @return El dataframe de entrada con la columna prediction, y label. Que son la columna de predicción de categoría
    *         y la columa real de la categoría transformada a número
    */
  def trainModel(dfData: DataFrame, columnCategory: String, columnFeatures: String): DataFrame ={
    val Array(trainDf, testDf) = dfData.randomSplit(Array(0.8, 0.2))
    val model = CountClassifier.train(trainDf,columnCategory,  columnFeatures)
    CountClassifier.test(model, testDf)
    model.transform(dfData).drop("tokens", "features", "rawPrediction", "probability")
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
      .withColumn("ID", monotonically_increasing_id)

    dfInput.printSchema()
    dfInput
  }


}
