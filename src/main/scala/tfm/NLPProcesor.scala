package tfm

import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.{PerceptronModel, SentenceDetector, Tokenizer}
import com.johnsnowlabs.util.Benchmark
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

class NLPProcesor(columnData:String) extends Serializable {

  import config.Config.spark.implicits._

  val documentAssembler = new DocumentAssembler()
    .setInputCol(columnData)
    .setOutputCol("document")

  val sentenceDetector = new SentenceDetector()
    .setInputCols(Array("document"))
    .setOutputCol("sentence")


  val tokenizer = new Tokenizer()
    .setInputCols(Array("sentence"))
    .setOutputCol("token")
    .setInfixPatterns(Array("(\\w+)('{1}\\w+)"))


  val pos_path = "resources/pos_anc_en_2.0.2_2.4_1556659930154"
  val posTagger = PerceptronModel.load(pos_path)
    .setInputCols("document", "token")
    .setOutputCol("pos")


  val pipeline = new Pipeline()
    .setStages(Array(
      documentAssembler,
      sentenceDetector,
      tokenizer,
      posTagger
    ))


  val model = Benchmark.time("Time to train Perceptron Model") {
    pipeline.fit(Seq.empty[String].toDF(columnData))
  }


  def transform(data:DataFrame): DataFrame ={
    import org.apache.spark.sql.catalyst.expressions._
    model.transform(data).drop("document", "sentence").map(
      x => x match {
      case Row(value: String, tokenArray: mutable.WrappedArray[Row], posArray: mutable.WrappedArray[Row]) =>
        {
          val tokens = tokenArray.map(token => token.getString(3)).toArray
          val pos_tags = posArray.map(pos => pos.getString(3)).toArray
          proces(tokens, pos_tags)
        }
      case _ => "Un matched"
    }).toDF()
  }

  /**
    * Método que recibe dos listas correspondientes a los tokens y a los tags respectivos y devuelve la senetencia sin apóstrofes
    * @param tokens
    * @param pos_tags
    * @return
    */
  def proces(tokens: Array[String], pos_tags: Array[String]): String ={
    val zipped = tokens zip pos_tags
    val list = zipped.toList
    val listOverlapped = list.init zip list.tail
    val first = listOverlapped(0)._1
    val second = listOverlapped(0)._2
  //  manage_ambiguos_apostrophes(first, second) for x,y in first,second
    ""
  }

  def manage_ambiguos_apostrophes(first: (String, String), second:(String, String)) : String ={
    if (first._1 == "'s") {
      if (second._2 == "VBN")
        return "has"
      else if (first._2 != "POS")
        return "is"
    }
    else if (first._1 == "'d") {
      if (second._2 == "VBN")
        return "had"
      else
        return "would"
    }
    else if (second._1.contains("'") &&  second._1 != "'s" &&  second._1 != "'d")
      return first._1 + second._1
    else if (!first._1.contains("'"))
      return first._1

    return ""
  }
}
