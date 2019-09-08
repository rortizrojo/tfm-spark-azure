package tfm.config

import org.apache.spark.sql.SparkSession


object Config {
  val spark = SparkSession
    .builder()
    .master("yarn")
    //.master("local[*]")
    .enableHiveSupport()
    .getOrCreate()
}
