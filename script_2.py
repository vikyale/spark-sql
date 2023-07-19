from pyspark.sql import SparkSession


# Crea una instancia de SparkSession
spark = SparkSession.builder \
    .appName("SparkSQLExample") \
    .getOrCreate()

# Crea un DataFrame con datos de ejemplo
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name","Age"])


df.createOrReplaceTempView("temp_table")

result = spark.sql("select * from temp_table where Age >= 30")


result.show()

# Cierra la sesión de Spark
spark.stop()