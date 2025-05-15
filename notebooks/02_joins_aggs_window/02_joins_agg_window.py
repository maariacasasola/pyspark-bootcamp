from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.window import Window

# Crear la sesión de Spark
spark = SparkSession.builder.appName("PySpark02").getOrCreate()

movies = spark.read.csv("../../data/movies.csv", header=True, inferSchema=True)
ratings = spark.read.csv("../../data/ratings.csv", header=True, inferSchema=True)

# Añade los títulos de las películas a los datos de puntuaciones
ratings_with_titles = ratings.join(movies.select("movieId", "title"), on="movieId", how="inner")
ratings_with_titles.show(10)

# Calcula el Top 5 de películas con mayor promedio de rating
movie_stats = ratings_with_titles.groupBy("title") \
    .agg(f.count("rating").alias("num_ratings"), f.avg("rating").alias("avg_rating")) \
    .orderBy("avg_rating", ascending=False)

print('Top 5 de películas con mayor media de valoraciones:')
movie_stats.show(5, truncate=False)

#Película más valorada por cada usuario
window_user = Window.partitionBy("userId").orderBy(ratings_with_titles["rating"].desc())

top_movies_per_user = ratings_with_titles \
    .withColumn("rank", f.row_number().over(window_user)) \
    .filter("rank = 1")

print('Película más valorada por cada usuario:')
top_movies_per_user.select("userId", "title", "rating").show(10)

spark.stop()