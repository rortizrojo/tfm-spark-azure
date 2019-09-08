package tfm

import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.{PerceptronModel, SentenceDetector, Tokenizer}
import com.johnsnowlabs.util.Benchmark
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}

import scala.collection.mutable

case class Test(original: String, replace: String)

class NLPProcessor(columnData:String) extends Serializable {

  @transient lazy val logger = Logger.getLogger(this.getClass.getName)

  import config.Config.spark.implicits._


   val documentAssembler = new DocumentAssembler()
    .setInputCol(columnData)
    .setOutputCol("document")

  val sentenceDetector = new SentenceDetector()
    .setInputCols(Array("document"))
    .setOutputCol("sentence")


  @transient lazy val tokenizer = new Tokenizer()
    .setInputCols(Array("sentence"))
    .setOutputCol("token")
    .setInfixPatterns(Array("(\\w+)('{1}\\w+)"))


   val pos_path = "resources/pos_anc_en_2.0.2_2.4_1556659930154"
   val posTagger = PerceptronModel.load(pos_path)
    .setInputCols("document", "token")
    .setOutputCol("pos")


  @transient lazy val pipeline = new Pipeline()
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
    val path = "resources/apostrophe_dict.csv"
    val apostropheDict : Array[Test] = config.Config.spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(path)
      .as[Test]
      .collect()

    val udfNoAmbiguos = udf{value:String =>
      val tokenList = value.split(" ")
      val processedTokens= tokenList.map(x => {
        val filtered  = apostropheDict.filter(elem => elem.original == x)
        if (filtered.length > 0)
          filtered(0).replace
        else
          x
      })
      val outputString = processedTokens.mkString(" ")
      //println("No ambiguos : cadena original:" + value  + " --> " + outputString)
      outputString
    }

    val udfAmbiguos = udf((value:String , tokenArray: mutable.WrappedArray[Row],posArray: mutable.WrappedArray[Row]) =>
    {
      val tokens = tokenArray.map(token => token.getString(3)).toArray
      val pos_tags = posArray.map(pos => pos.getString(3)).toArray
      val procesedTokens = process(tokens, pos_tags)
      val outputList = procesedTokens.filter(x => !x.isEmpty).map(x => x match {
        case Some(_) => x.get
        case None => ""
      })
      val outputString = outputList.mkString(" ")
      //println("Ambiguos : cadena original:" + value  + " --> " + outputString)
      outputString
    })

    logger.info("Proceso de eliminación de apóstrofes ambiguos")
    val ambiguousApostropheRemoved= model
      .transform(data)
      .drop("document", "sentence")
      .withColumn(columnData,udfAmbiguos(col(columnData), col("token"), col("pos")) )
      .drop("token", "pos")

    logger.info("Proceso de eliminación de apóstrofes no ambiguos")
    ambiguousApostropheRemoved.withColumn(columnData,udfNoAmbiguos(col(columnData))).toDF()

  }


  /**
    * Método que recibe dos listas correspondientes a los tokens y a los tags respectivos y devuelve la senetencia sin apóstrofes
    * @param tokens
    * @param pos_tags
    * @return
    */
  def process(tokens: Array[String], pos_tags: Array[String]): List[Option[String]] ={
    val zipped = tokens zip pos_tags
    val list = zipped.toList
    val listOverlapped = list.init zip list.tail
    val finalList = listOverlapped.map(x => manage_ambiguos_apostrophes(x._1, x._2))

    val optionTest =Option(zipped.last._1)
    val newList: List[Option[String]] =  if (zipped.length > 0 && zipped.last._2 != "POS")
      finalList ++ Some(optionTest)
    else
      finalList

    newList
  }

  def manage_ambiguos_apostrophes(first: (String, String), second:(String, String)) : Option[String] ={
    if (first._1 == "'s") {
      if (second._2 == "VBD")
        return Some("has")
      else if (first._2 != "POS")
        return Some("is")
    }
    else if (first._1 == "'d") {
      if (second._2 == "VBD")
        return Some("had")
      else
        return Some("would")
    }
    else if (second._1.contains("'") &&  second._1 != "'s" &&  second._1 != "'d")
      return Some(first._1 + second._1)
    else if (!first._1.contains("'"))
      return Some(first._1)

    return None
  }
}

