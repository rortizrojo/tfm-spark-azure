package tfm.ML

import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, Tokenizer}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.{DateTime, Period}

object CountClassifier {

  def train(dfTrain: DataFrame): PipelineModel = {

    val stages = Array(
      new StringIndexer().setInputCol("Keyword_match_type").setOutputCol("label"),
      new Tokenizer().setInputCol("Queries").setOutputCol("tokens"),
      new CountVectorizer().setInputCol("tokens").setOutputCol("features"),
      new LogisticRegression().setMaxIter(30).setRegParam(0.001)
    )

    val timeStart = DateTime.now()
    val model =  new Pipeline().setStages(stages).fit(dfTrain)
    val timeEnd = DateTime.now()
    val p = new Period(timeStart, timeEnd)
    println("Total time elapsed training model with CountClassifier Algorithm: %02d:%02d:%02d.%03d".format(p.getHours, p.getMinutes, p.getSeconds, p.getMillis))

    model
  }

  def test(model: PipelineModel, testDf: DataFrame): Unit = {

    // Method1  Test en máquina local tarda 4.5 segundos
    var totalCorrect = 0.0
    val result = model.transform(testDf)
      .select("prediction", "label")
      .collect()
    result.foreach { case Row(prediction, label) => if (prediction == label) totalCorrect += 1 }
    val accuracy = totalCorrect / result.length
    println(s"Accuracy: $accuracy")

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
