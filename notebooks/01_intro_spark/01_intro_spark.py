from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Crear la sesión de Spark
spark = SparkSession.builder.appName("PySpark01").getOrCreate()

# Define el schema
schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("views", IntegerType(), True),
    StructField("quality", StringType(), True)
])

# Leer archivo CSV
df = spark.read.option("header", True).schema(schema).csv("../../data/movies.csv")

df.printSchema()
df.show(5)

# Conteo total de registros
print("Total de películas:", df.count())

# Filtrar y seleccionar columnas
df_filtered = df.select("title", "genres", "views", "quality").filter(col("views") > 100000)

# Agrupación por calidad
df_grouped = df_filtered.groupBy("quality").agg(avg("views").alias("avg_views"))
print("Media de visualizaciones de las películas que superan las 100.000 visitas por cada tipo de calidad:")
df_grouped.orderBy(col("avg_views").desc()).show()

# Guardar resultados en formato Parquet
df_grouped.write.mode("overwrite").parquet("data/popular_movies_by_quality.parquet")

# Guardar resultados en particiones
df.write.partitionBy("quality").parquet("data/partition_by_quality.parquet")
# Leer una de las particiones guardadas
medium_quality_data = spark.read.parquet("data/partition_by_quality.parquet/quality=medium")

# Aplicar función de ventana: Top 3 películas por calidad según las visualizaciones
window_spec = Window.partitionBy("quality").orderBy(col("views").desc())
df_with_rank = df.withColumn("rank", row_number().over(window_spec))
df_top3 = df_with_rank.filter(col("rank") <= 3)
print('Top 3 películas más visualizadas por calidad:')
df_top3.show()

# Detener la sesión de Spark
spark.stop()