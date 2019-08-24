//package tfm
//
//import com.johnsnowlabs.nlp.DocumentAssembler
//import com.johnsnowlabs.nlp.annotator.{PerceptronApproach, SentenceDetector, Tokenizer}
//import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
//import com.johnsnowlabs.util.{Benchmark, PipelineModels}
//import org.apache.spark.ml.Pipeline
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.col
//
//import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
//import com.johnsnowlabs.nlp.annotators.{Normalizer, Stemmer, Tokenizer}
//import com.johnsnowlabs.nlp.annotator._
//import com.johnsnowlabs.nlp.base._
//import com.johnsnowlabs.util.{Benchmark, PipelineModels, Version}
//import org.apache.spark.ml.feature.NGram
//
//import org.apache.spark.ml.Pipeline
//import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
//import com.johnsnowlabs.nlp.annotators.{Normalizer, Stemmer, Tokenizer}
//import com.johnsnowlabs.nlp.annotator._
//import com.johnsnowlabs.nlp.base._
//import com.johnsnowlabs.util.{Benchmark, PipelineModels, Version}
//import org.apache.spark.ml.feature.NGram
//import org.apache.spark.ml.Pipeline
//
//object TestNLP3 extends App{
//
//  test1
//
//
//  def test1: Unit = {
//
//
//
//    val spark = SparkSession
//      .builder()
//      .master("local[*]")
//      .enableHiveSupport()
//      .getOrCreate()
//
//    import spark.implicits._
//
//    //val pipeline = PretrainedPipeline("entity_recognizer_dl", "en")
//
//    val data = Seq(
//      "I'd like to ride a bike"
//    ).toDF("text")
//
//
//    val documentAssembler = new DocumentAssembler()
//      .setInputCol("text")
//      .setOutputCol("document")
//
//    val sentenceDetector = new SentenceDetector()
//      .setInputCols(Array("document"))
//      .setOutputCol("sentence")
//
//
//    val tokenizer = new Tokenizer()
//       .setInputCols(Array("sentence"))
//      .setOutputCol("token")
//      .setInfixPatterns(Array("(\\w+)('{1}\\w+)"))
//
//
//    val pos_path = "resources/pos_anc_en_2.0.2_2.4_1556659930154"
//    val posTagger = PerceptronModel.load(pos_path)
//      .setInputCols("document", "token")
//      .setOutputCol("pos")
//
//
//    val pipeline = new Pipeline()
//      .setStages(Array(
//        documentAssembler,
//        sentenceDetector,
//        tokenizer,
//        posTagger
//      ))
//
//
//    val model = Benchmark.time("Time to train Perceptron Model") {
//       pipeline.fit(Seq.empty[String].toDF("text"))
//    }
//
//    model.transform(data).select("token", "pos").show(false)
//
//
//
//  }
//}
