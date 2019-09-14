package tfm.ml

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner


class SimilartyCalculationTest extends FunSuite with DataFrameSuiteBase {

  test("testCalculateSimilarity") {
    val pathFile = "input/muestraSubido.csv"
    val dfTrain = spark.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", true)
      .option("header", true)
      .option("delimiter","\t")
      .csv(pathFile)
      .na.drop(Seq("Queries"))
      .withColumn("ID", monotonically_increasing_id)

    val listInput = List(  Row("1","water feature battery operated,battery operated outdoor water features", "water feature battery operated"),
      Row("2","glass cases for spectacles", "glass cases for spectacles"))
    val rddInput = sc.parallelize(listInput)
    val schema = StructType(
      List(
        StructField("ID", StringType),
        StructField("Queries", StringType),
        StructField("Keyword", StringType)
      )
    )

    val input : DataFrame = spark.createDataFrame(rddInput,schema).toDF()

    val result = new SimilartyCalculation().calculateSimilarity(dfTrain, input, "Queries", "Keyword")

    val listExpected = List(  Row("1","water feature battery operated,battery operated outdoor water features", "water feature battery operated", "0.8299719179872187"),
      Row("2","glass cases for spectacles", "glass cases for spectacles","0.8299719179872187"))
    val rddExpected = sc.parallelize(listExpected)
    val schemaExpected = StructType(
      List(
        StructField("ID", StringType),
        StructField("similarity", DoubleType),
        StructField("column1", StringType),
        StructField("column2", StringType)

      )
    )
    val expected : DataFrame = spark.createDataFrame(rddExpected,schemaExpected).toDF()

    assertDataFrameEquals(expected,result)
  }

}
