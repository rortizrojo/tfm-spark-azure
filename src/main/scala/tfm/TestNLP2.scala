//package tfm
//
//import com.johnsnowlabs.nlp.annotators.common.Annotated.PosTaggedSentence
//import com.johnsnowlabs.nlp.annotators.parser.dep.Tagger
//import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
//import com.johnsnowlabs.nlp.{DocumentAssembler, LightPipeline, SparkNLP}
//import com.johnsnowlabs.nlp.annotators.{LemmatizerModel, Tokenizer}
//import com.johnsnowlabs.nlp.annotators.pos.perceptron.{PerceptronApproach, PerceptronModel}
//import com.johnsnowlabs.util.Benchmark
//import org.apache.spark.ml.{Pipeline, PipelineModel}
//import org.apache.spark.sql.SparkSession
//
//object TestNLP2 extends App {
//
//
//  test2
//
//
//  def test1: Unit ={
//    SparkNLP.version()
//    val spark = SparkSession
//      .builder()
//      .master("local[*]")
//      .enableHiveSupport()
//      .getOrCreate()
//    val testData = spark.createDataFrame(Seq(
//      (1, "I'd like to ride a bike")
//    )).toDF("id", "text")
//
//    val path = "resources/explain_document_ml_en_2.1.0_2.4_1563203154682/"
//    val advancedPipeline = PipelineModel.load(path)
//    // To use the loaded Pipeline for prediction
//    val annotation = advancedPipeline.transform(testData)
//
//    annotation.show(false)
//   // val annotations = PretrainedPipeline("explain_document_ml").annotate("We are very happy about SparkNLP")
//    //
//    //  val pos_tags = annotations("pos")
//
//    //  println(pos_tags)
//
//    //new LightPipeline(somePipelineModel).annotate(someStringOrArray))
//
//    //Pos model
//
//
//
//
//    val english_pos = PerceptronModel.load("resources/pos_anc_en_2.0.2_2.4_1556659930154/")
//      .setInputCols("text", "token")
//      .setOutputCol("pos")
//
//    english_pos.transform(testData).show(false)
//  }
//
//
//
//
////
////  val posTagger = new PerceptronApproach()
////    .setInputCols(Array("sentence", "token"))
////    .setOutputCol("pos")
////    .setNIterations(2)
////    .fit(testData)
//
//
//
//  def test2: Unit = {
//    import com.johnsnowlabs.nlp.LightPipeline
//    val spark = SparkSession
//      .builder()
//      .master("local[*]")
//      .enableHiveSupport()
//      .getOrCreate()
//
////    val path = "resources/pos_anc_en_2.0.2_2.4_1556659930154/"
////    val advancedPipeline = PerceptronModel.load(path)
////
////
////    val tokenizer = new Tokenizer()
////      .setInputCols("sentence")
////      .setOutputCol("token")
////      .setContextChars(Array("(", ")", "?", "!"))
////      .setSplitChars(Array("-"))
////      .addException("New York")
////      .addException("e-mail")
////
////     val annotations = PretrainedPipeline("explain_document_ml").annotate("We are very happy about SparkNLP")
////    val lemma = LemmatizerModel.load("resources/lemma_antbnc_en_2.0.2_2.4_1556480454569/")
////      .setInputCols("document", "token")
////      .setOutputCol("lemmas")
////
////    lemma.annotate("We are very happy about SparkNLP")
////
////
////    val annotado = advancedPipeline.tag(tokenizer)
////    annotado
////
//
//
//    val testData = spark.createDataFrame(Seq(
//      (1, "I'd like to ride a bike"))).toDF("id", "text")
//
//    val document = new DocumentAssembler()
//      .setInputCol("text")
//      .setOutputCol("document")
//
//    val token = new Tokenizer()
//      .setInputCols("document")
//      .setOutputCol("token")
//     .addInfixPattern("(\\w+)([^\\s\\p{L}]{1})+(\\w+)")
//     .addInfixPattern("(\\w+'{1})(\\w+)") // (l',air) instead of (l, 'air) in GSD
//      .addInfixPattern("(\\p{L}+)(n't\\b)")
//      .addInfixPattern("((?:\\p{L}\\.)+)")
//      .addInfixPattern("([\\$#]?\\d+(?:[^\\s\\d]{1}\\d+)*)")
//      .addInfixPattern("([\\p{L}\\w]+)")
//
////    val lemma = LemmatizerModel.load("resources/lemma_antbnc_en_2.0.2_2.4_1556480454569/")
////      .setInputCols("document", "token")
////      .setOutputCol("lemmas")
//
//
//    val pos_tag = PerceptronModel.load("resources/pos_anc_en_2.0.2_2.4_1556659930154/")
//      .setInputCols("token", "document")
//      .setOutputCol("pos")
//
//    val pipeline = new Pipeline().setStages(
//      Array(
//        document,
//        token,
//        pos_tag
//      )
//    )
//    val result = Benchmark.time("Time to convert and show") {
//      pipeline.fit(testData).transform(testData)
//    }
//
//
//    result.show(truncate=false)
//    result.printSchema
//  }
//
//  def test3: Unit = {
//    val spark = SparkSession
//      .builder()
//      .master("local[*]")
//      .enableHiveSupport()
//      .getOrCreate()
//    val testData = spark.createDataFrame(Seq(
//      (1, "I'd like to ride a bike"))).toDF("id", "text")
//    val path = "resources/explain_document_ml_en_2.1.0_2.4_1563203154682/"
//    val advancedPipeline = PipelineModel.load(path)
//    // To use the loaded Pipeline for prediction
//    val annotation = advancedPipeline.transform(testData)
//
//    annotation.show()
//  }
//
//  def test4: Unit = {
//    val spark = SparkSession
//      .builder()
//      .master("local[*]")
//      .enableHiveSupport()
//      .getOrCreate()
//
//    import spark.implicits._
//    val data = Seq("I'd like to ride a bike").toDF("text")
//
//    val path = "resources/explain_document_dl_en_2.1.0_2.4_1562946191575/"
//    val advancedPipeline = PipelineModel.load(path)
//    val annotations = advancedPipeline.transform(data)
//    annotations.show(false)
//  }
//
//  def test5: Unit = {
//    val spark = SparkSession
//      .builder()
//      .master("local[*]")
//      .enableHiveSupport()
//      .getOrCreate()
//
//    import spark.implicits._
//
//
//
//
//    val df = Seq(
//      (1, "Hello world!"),
//      (2, "I'd like to ride a bike")).toDF("id", "text")
//
//
//    val documentAssembler = new DocumentAssembler()
//      .setInputCol("text")
//      .setOutputCol("document")
//
//
//
//    val tok = new Tokenizer()
//      .setInputCols("document")
//      .setOutputCol("token")
//      .setInputCols("document")
//      .setOutputCol("token")
//      .addInfixPattern("(\\w+)([^\\s\\p{L}]{1})+(\\w+)")
//      .addInfixPattern("(\\w+'{1})(\\w+)") // (l',air) instead of (l, 'air) in GSD
//      .addInfixPattern("(\\p{L}+)(n't\\b)")
//      .addInfixPattern("((?:\\p{L}\\.)+)")
//      .addInfixPattern("([\\$#]?\\d+(?:[^\\s\\d]{1}\\d+)*)")
//      .addInfixPattern("([\\p{L}\\w]+)")
//
//
////    val tokenized = tok.setInputCol("sentence").setOutputCol("tokens").transform(df)
////    tokenized.show(false)
//
//    val pos_tag = PerceptronModel.load("resources/pos_anc_en_2.0.2_2.4_1556659930154/")
//      .setInputCols("token")
//      .setOutputCol("pos")
//
//    val path = "resources/explain_document_dl_en_2.1.0_2.4_1562946191575/"
//    val advancedPipeline = PipelineModel.load(path)
//
//
//    val lightPipeline = new LightPipeline(advancedPipeline)
//
//    lightPipeline.annotate("I'd like to ride a bike")
//  }
//
//
//}
