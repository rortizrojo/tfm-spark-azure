package tfm.ML

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, udf}

object SimilarityTest {

  def main(args: Array[String]): Unit = {

    otherTest

  }
def test1: Unit ={
  val udfToArray = udf{ text: String =>
    text.split(" ")
  }
  import tfm.config.Config.spark
  import org.apache.spark.ml.feature.StopWordsRemover
  val pathFile = "input/muestraSubido.csv"
  val dfInput = spark.read
    .format("com.databricks.spark.csv")
    .option("inferSchema", true)
    .option("header", true)
    .option("delimiter","\t")
    .csv(pathFile)
    .na.drop(Seq("Queries"))
    .withColumn("QueriesArray", udfToArray(col("Queries")))

  // Remove stop words
  val stopWordsRemover = new StopWordsRemover().setInputCol("QueriesArray").setOutputCol("nostopwords")
  val documentDF = stopWordsRemover.transform(dfInput)

  documentDF.take(2).foreach(println)


  import org.apache.spark.ml.feature.Word2Vec

  // Learn a mapping from words to Vectors
  val word2Vec = new Word2Vec().
    setInputCol("QueriesArray").
    setOutputCol("result").
    setVectorSize(200).
    setMinCount(10)
  val model = word2Vec.fit(documentDF)


  // Find synonyms for a single word
  model.findSynonyms("free baby goats", 10)
    .collect
    .foreach(println)
}


  def otherTest: Unit ={

    import tfm.config.Config.spark
    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("result").take(3).foreach(println)
  }

}
