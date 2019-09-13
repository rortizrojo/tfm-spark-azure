package tfm.ML


import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.slf4j.LoggerFactory
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, Tokenizer, Word2Vec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, udf}
import tfm.config.Config.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.rdd.RDD

import scala.math.log10
import scala.math.pow

object SimilarityTest {

  def main(args: Array[String]): Unit = {

    lastTry

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

  def testThree: Unit ={

    import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
    import org.apache.spark.ml.linalg.Vectors
    import org.apache.spark.sql.functions.col


    val spark =  tfm.config.Config.spark

    val dfA = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 1.0)),
      (1, Vectors.dense(1.0, -1.0)),
      (2, Vectors.dense(-1.0, -1.0)),
      (3, Vectors.dense(-1.0, 1.0))
    )).toDF("id", "features")

    val dfB = spark.createDataFrame(Seq(
      (4, Vectors.dense(1.0, 0.0)),
      (5, Vectors.dense(-1.0, 0.0)),
      (6, Vectors.dense(0.0, 1.0)),
      (7, Vectors.dense(0.0, -1.0))
    )).toDF("id", "features")

    val key = Vectors.dense(1.0, 0.0)

    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(3)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = brp.fit(dfA)

    // Feature Transformation
    println("The hashed dataset where hashed values are stored in the column 'hashes':")
    model.transform(dfA).show()

    // Compute the locality sensitive hashes for the input rows, then perform approximate
    // similarity join.
    // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
    // `model.approxSimilarityJoin(transformedA, transformedB, 1.5)`
    println("Approximately joining dfA and dfB on Euclidean distance smaller than 1.5:")
    model.approxSimilarityJoin(dfA, dfB, 1.5, "EuclideanDistance")
      .select(col("datasetA.id").alias("idA"),
        col("datasetB.id").alias("idB"),
        col("EuclideanDistance")).show()

    // Compute the locality sensitive hashes for the input rows, then perform approximate nearest
    // neighbor search.
    // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
    // `model.approxNearestNeighbors(transformedA, key, 2)`
    println("Approximately searching dfA for 2 nearest neighbors of the key:")
    model.approxNearestNeighbors(dfA, key, 2).show()
  }

  def another: Unit ={

    import org.apache.spark.ml.feature.MinHashLSH
    import org.apache.spark.ml.linalg.Vectors
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions.col

    val labeledColumn ="Keyword_match_type"
    val columnToClassificate = "Queries"
    val keyword = "Keyword"


    val spark =  tfm.config.Config.spark
    val pathFile = "input/muestraSubido.csv"
    val dfInput = spark.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", true)
      .option("header", true)
      .option("delimiter","\t")
      .csv(pathFile)
      .na.drop(Seq("Queries"))
      .select(keyword,labeledColumn, columnToClassificate )




    val stages = Array(
      new StringIndexer().setInputCol(labeledColumn).setOutputCol("label"), //Primero se convierte la columna de string a double.
      new Tokenizer().setInputCol(columnToClassificate).setOutputCol("tokens"), //Se extraen los tokens de la columna de clasificación
      new CountVectorizer().setInputCol("tokens").setOutputCol("features") //Se aplica la vectorización de conteo de tokens
    )

    val countModel =  new Pipeline().setStages(stages).fit(dfInput)



    val dfA = spark.createDataFrame(Seq(
      (0, Vectors.sparse(6, Seq((0, 1.0), (1, 1.0), (2, 1.0)))),
      (1, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (4, 1.0)))),
      (2, Vectors.sparse(6, Seq((0, 1.0), (2, 1.0), (4, 1.0))))
    )).toDF("id", "features")

    val dfB = spark.createDataFrame(Seq(
      (3, Vectors.sparse(6, Seq((1, 1.0), (3, 1.0), (5, 1.0)))),
      (4, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (5, 1.0)))),
      (5, Vectors.sparse(6, Seq((1, 1.0), (2, 1.0), (4, 1.0))))
    )).toDF("id", "features")

    val key = Vectors.sparse(6, Seq((1, 1.0), (3, 1.0)))


    val mh = new MinHashLSH()
      .setNumHashTables(5)
      .setInputCol("features")
      .setOutputCol("hashes")


    val dfCounted = countModel.transform(dfInput)
    val model = mh.fit(dfCounted)

    // Feature Transformation
    println("The hashed dataset where hashed values are stored in the column 'hashes':")
    model.transform(dfCounted).show()

    // Compute the locality sensitive hashes for the input rows, then perform approximate
    // similarity join.
    // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
    // `model.approxSimilarityJoin(transformedA, transformedB, 0.6)`
//    println("Approximately joining dfA and dfB on Jaccard distance smaller than 0.6:")
    model.approxSimilarityJoin(dfCounted, dfCounted, 0.6, "JaccardDistance")
//      .select(col("datasetA.id").alias("idA"),
//      col("datasetB.id").alias("idB"),
//      col("JaccardDistance"))
      .show()


    // Compute the locality sensitive hashes for the input rows, then perform approximate nearest
    // neighbor search.
    // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
    // `model.approxNearestNeighbors(transformedA, key, 2)`
    // It may return less than 2 rows when not enough approximate near-neighbor candidates are
    // found.
    println("Approximately searching dfA for 2 nearest neighbors of the key:")
    model.approxNearestNeighbors(dfCounted, key, 2).show()
  }
  def cosineDist: Unit ={
    def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName("Project_3").setMaster("local[*]");
      val sc = new SparkContext(conf)

      val gene = "gene_.*_gene".r.toString() // Our filter for gene_SOMETHING_gene

      val lines = sc.textFile(args(0))

      val totalDocs = lines.count() //Get |D| (total number of documents)

      val info = lines.flatMap {
        line => lazy val words = line.split("\\s+")
          /*This code is if we only want to count the total genes per document rather than total words per document
            val filteredWords = words.filter(term => term.matches(gene))
            filteredWords.map(term => ( (term, words.head, filteredWords.length ), 1) )
          */

          words.filter(term => term.matches(gene)).map(term => ( (term, words.head, words.length-1 ), 1) )
      }.reduceByKey(_+_)
      //info contains : (term, documentID, totalDocumentWordCount) => wordCountForTerm

      //tfIdfInfo remaps the info to make term the key
      val tfIdfInfo = info.map( info => ( info._1._1, (info._1._2, info._1._3, info._2 )))
      // It contains : term => (documentID, totalDocumentWordCount, wordCountForTerm)

      //termTfIdf calculates each term's tf/idf  per document based on tfIdfInfo and groups them by the key (term)
      val termTfIdf = tfIdfInfo.groupByKey()
        .mapValues(array => array.map(triple => (triple._1, ( (triple._3/triple._2.toDouble) * log10(totalDocs/array.size.toDouble) ) )))//.filter(x => x._2 > 0)).filter(x => x._2.size > 0)
      //It contains : term => List[documentID, tf/idf]

      //We get all permutations of terms and filter out duplicates based on lexicographical difference giving us the distinct combinations of terms
      val termPairs = termTfIdf.cartesian(termTfIdf).filter(pair => pair._1._1 > pair._2._1)
      //We now have all the information needed to calculate the cosine similarity between pairs

      // We map each term pair to their cosine similarity and pair of term names, filtering out the ones with 0 similarity since most will be 0 and don't give us useful information
      val cosSimPairs =  termPairs.map(pair => (cosSimilarity(pair._1._2,pair._2._2),(pair._1._1,pair._2._1)))
        .filter(x=> x._1 > 0.0)

      //Sort the pairs, collect them  to output their true sorted order for viewing
      cosSimPairs.sortByKey(false).collect.foreach( termPair => println(termPair))
    }

  }

  //Returns the cosine Similarity of two Iterables
  def cosSimilarity(A:Iterable[(String,Double)],B:Iterable[(String,Double)]): Double  = {
    dotProduct(A,B) / productOfNorms(A,B)
  }

  //Returns the product of the norms of two Iterables
  def productOfNorms(A:Iterable[(String,Double)],B:Iterable[(String,Double)]): Double = {
    getNorm(A) * getNorm(B)
  }
  //Returns the norm of an Iterable (Euclidean Distance)
  def getNorm(vector: Iterable[(String,Double)]): Double = {
    pow(vector.map(x => pow(x._2, 2)).sum, .5)
  }
  //Returns the dot product of two Iterables
  def dotProduct(A:Iterable[(String,Double)],B:Iterable[(String,Double)]): Double = {
    /* Append the Iterables to each other and group them by the key (document ID) and get only the values in the Iterable
       where there are at least 2. This will in turn optimize the dot product calculation since we only take the product of
       matching non-zero values */
    val dotProduct = (A ++ B).groupBy(_._1).mapValues(_.map(_._2)).filter(_._2.size > 1)
      .map(x => x._2.product)

    dotProduct.sum
  }

  def lastTry: Unit ={
    import org.apache.spark.ml.feature.{HashingTF, IDF, Normalizer}
    import org.apache.spark.sql.functions._
    val labeledColumn ="Keyword_match_type"
    val columnToClassificate = "Queries"
    val keyword = "Keyword"


    val spark =  tfm.config.Config.spark
    val pathFile = "input/muestraSubido.csv"
    val dfInput = spark.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", true)
      .option("header", true)
      .option("delimiter","\t")
      .csv(pathFile)
      .na.drop(Seq("Queries"))
      .select(keyword,labeledColumn, columnToClassificate )
      .withColumn("ID", monotonically_increasing_id)

    val tokenizer = new Tokenizer().setInputCol(columnToClassificate).setOutputCol("tokens")
    val tokenizerKeyword = new Tokenizer().setInputCol(keyword).setOutputCol("tokensKeyword")
    val dfTok = tokenizer.transform(dfInput)
    val dfTokKeyword = tokenizerKeyword.transform(dfInput)
    val hashingTF = new HashingTF().setInputCol("tokens").setOutputCol("rawFeatures").setNumFeatures(20)
    val hashingTFKeyword = new HashingTF().setInputCol("tokensKeyword").setOutputCol("rawFeaturesKeyword").setNumFeatures(20)
    val featurizedData = hashingTF.transform(dfTok)
    val featurizedDataKeyword = hashingTFKeyword.transform(dfTokKeyword)
    import spark.implicits._

    val tfidf = new IDF().setInputCol("rawFeatures").setOutputCol("features").fit(featurizedData).transform(featurizedData)
    val tfidfKeyword = new IDF().setInputCol("rawFeaturesKeyword").setOutputCol("featuresKeyword").fit(featurizedDataKeyword).transform(featurizedDataKeyword)
    val data = new Normalizer().setInputCol("features").setOutputCol("normFeatures").transform(tfidf)
    val dataKeyword = new Normalizer().setInputCol("featuresKeyword").setOutputCol("normFeaturesKeyword").transform(tfidfKeyword)



//    // the values for the column in each row
//    val col = List(-3.0, -1.5, 0.0, 1.5, 3.0)
//
//    // make two rows of the column values, transpose it,
//    // make Vectors of the result
//    val t = List(col,col).transpose.map(r=>Vectors.dense(r.toArray))
//
//    // make an RDD from the resultant sequence of Vectors, and
//    // make a RowMatrix from that.
//    val test = spark.sparkContext.makeRDD(t)
//    val rm = new RowMatrix(test)

    val udfPrueba = udf { (x: Vector, y: Vector)=>
      new BDV(x.toArray) dot new BDV(y.toArray)

//      val dfX = x.deserialize().toArray.toSeq.toDF()
//      val mat1 = new RowMatrix(dfX.rdd[Vector])
//      mat1
    }

    data.join(dataKeyword, "ID")
    .withColumn("dot1", udfPrueba(col("normFeatures"), col("normFeaturesKeyword")))
      .withColumn("dot2", udfPrueba(col("normFeaturesKeyword"), col("normFeatures")))
    .show(false)
  }
}
