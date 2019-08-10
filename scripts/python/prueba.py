from pyspark.sql import SparkSession

if __name__== "__main__":

    spark = SparkSession.builder \
        .appName("Separador Columnas") \
        .getOrCreate()
    spark.sparkContext.addPyFile("my_arch.zip")
    import utils.py
    utils.data_splitter()