package tfm.ML

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, Tokenizer}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.{DateTime, Period}

/**
  * Clase que entrena y prueba un modelo para clasificar en un dataframe el texto de una columna usando la clase CountVectorizer
  * de Spark ML
  */
object CountClassifier {

  /**
    * Función que entrena un modelo a partir de la columna etiquetada "labeledColumn" y la columna sobre la que se ha
    * clasificado "columnToClassificate" usando la clase de Spark ML CountVectorizer, que convierte una colección de
    * documentos de texto a vectores de conteo de tokens
    * Mas info: https://spark.apache.org/docs/latest/ml-features.html#countvectorizer
    *
    * @param dfTrain Dataframe con los datos para el entrenamiento
    * @param labeledColumn Columna que contiene la clasificación y se usará para el entrenamiento
    * @param columnToClassificate Columna que contiene los textos sobre los que se hace la clasificación
    * @return Modelo entrenado
    */
  def train(dfTrain: DataFrame, labeledColumn:String, columnToClassificate:String): PipelineModel = {
    val logger = Logger.getLogger(this.getClass.getName)
    val stages = Array(
      new StringIndexer().setInputCol(labeledColumn).setOutputCol("label"), //Primero se convierte la columna de string a double.
      new Tokenizer().setInputCol(columnToClassificate).setOutputCol("tokens"), //Se extraen los tokens de la columna de clasificación
      new CountVectorizer().setInputCol("tokens").setOutputCol("features"), //Se aplica la vectorización de conteo de tokens
      new LogisticRegression().setMaxIter(30).setRegParam(0.001) //Finalmente se aplica una regresión logística con 30 iteraciones y un valor de regularización de 0.001
    )

    val timeStart = DateTime.now()
    //Se aplican los pasos definidos anteriormente, es decir el entrenamiento del modelo
    val model =  new Pipeline().setStages(stages).fit(dfTrain)
    val timeEnd = DateTime.now()
    val p = new Period(timeStart, timeEnd)
    logger.warn("Total time elapsed training model with CountClassifier Algorithm: %02d:%02d:%02d.%03d".format(p.getHours, p.getMinutes, p.getSeconds, p.getMillis))

    model
  }

  /**
    * Función que prueba el acierto de un modelo escribiendo el resultado en el log.
    *
    * @param model Modelo a probar
    * @param testDf Dataframe con los datos para el entrenamiento
    */
  def test(model: PipelineModel, testDf: DataFrame): Unit = {
    val logger = Logger.getLogger(this.getClass.getName)
    // Method1  Test en máquina local tarda 4.5 segundos
    var totalCorrect = 0.0
    val result = model.transform(testDf)
      .select("prediction", "label")
      .collect()
    result.foreach { case Row(prediction, label) => if (prediction == label) totalCorrect += 1 }
    val accuracy = totalCorrect / result.length
    logger.warn(s"Accuracy of model: $accuracy")

    //    // Method2. Test en máquina local tarda 9.5 segundos
    //    val timeStart2 =DateTime.now()
    //    val predictionLabelsRDD =  model.transform(test).select("prediction", "label").rdd.map(r => (r.getDouble(0), r.getDouble(1)))
    //    val multiModelMetrics = new MulticlassMetrics(predictionLabelsRDD)
    //    println(s"Accuracy: ${multiModelMetrics.accuracy}")
    //    val timeEnd2 =DateTime.now()
    //
    //    val p2 = new Period(timeStart2, timeEnd2 )
    //    println("Total time elapsed: %02d:%02d:%02d.%03d".format(p2.getHours, p2.getMinutes, p2.getSeconds, p2.getMillis))
  }
}
