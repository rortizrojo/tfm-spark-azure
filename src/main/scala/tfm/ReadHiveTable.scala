package tfm

import org.apache.spark.sql.SparkSession

object ReadHiveTable extends App{
   val spark = SparkSession
    .builder()
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

  spark.sql("select * from tabladatostfm").show()


}
