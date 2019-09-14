package tfm.ML

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.ml.feature.{HashingTF, IDF, Normalizer, Tokenizer}

/**
  * Clase que calcula la similitud entre dos sentencias vectorizando las frases y calculando su distancia euclídea calculando
  * el producto escalar de los dos vectores
  */
class SimilartyCalculation {
  def calculateSimilarity(dfTrain: DataFrame, dfInput: DataFrame, column1: String,column2:String  ): DataFrame ={

    val udfDotProduct = udf { (x: Vector, y: Vector)=>
      val vec1 = new BDV(x.toArray)
      val vec2 = new BDV(y.toArray)
      vec1 dot vec2
    }

    val model1 = trainModel(dfTrain, column1)
    val model2 = trainModel(dfTrain, column2)
    val data1 = model1.transform(dfInput.select("ID", column1))
    val data2 = model2.transform(dfInput.select("ID", column2))
    data1.join(data2, "ID")
      .withColumn("similarity", udfDotProduct(col("normFeatures"+column1), col("normFeatures"+column2)))
      .select("ID" , "similarity")
      .join(dfInput , "ID")


  }

  def trainModel(dfTrain: DataFrame, column :String ) = {
    /** Se seleccionan las columnas que se necesitan */
    val dfReduced = dfTrain.select("ID", column)
    val stages = Array(
      new Tokenizer().setInputCol(column).setOutputCol("tokens"), //Se extraen los tokens de la columna de clasificación
      new HashingTF().setInputCol("tokens").setOutputCol("rawFeatures").setNumFeatures(20),
      new IDF().setInputCol("rawFeatures").setOutputCol("features"),
      new Normalizer().setInputCol("features").setOutputCol("normFeatures" + column)
    )
    new Pipeline().setStages(stages).fit(dfReduced)
  }

}
