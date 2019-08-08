from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col
import time


def data_splitter():
    #  El fichero ocupa
    input_file = "muestraFichero.csv"
    spark = SparkSession.builder \
        .appName("Separador Columnas") \
        .getOrCreate()

    df = spark.read.csv(input_file, sep="\t", header="True")

    # input = pd.read_csv(input_file, sep=";")
    columnQueries = "Queries"
    filename = "{0}_{1}.csv".format(input_file.rsplit(".", 1)[0], columnQueries)
    df = df.repartition(8)
    print("Start process")
    startTime = time.time()
    dfFinal = df.withColumn(columnQueries, lower(col(columnQueries)).alias(columnQueries))
    dfFinal.repartition(1).write.csv(filename, mode="overwrite", header=True, sep=";")
    endTime = time.time()
    print("Elapsed time: ", endTime - startTime)

