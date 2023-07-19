from pyspark.sql import SparkSession


# Crea una instancia de SparkSession
spark = SparkSession.builder \
    .appName("HelloSpark") \
    .getOrCreate()

# Crea un DataFrame con datos de ejemplo
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Muestra el contenido del DataFrame
df.show()

# Cierra la sesión de Spark
spark.stop()