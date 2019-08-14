package tfm

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import tfm.config.Config
import org.apache.spark.sql.functions.col


object Preproceso {
  def main(args : Array[String]) {

    val spark = Config.spark
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")
    val pathFile = "input/muestraFicheroPequenya.csv"
//    val df = spark.read
//      .option("header", true)
//      //.schema(schemaStruct)
//      .option("delimiter","\t")
//      .csv(pathFile)

  }
}
