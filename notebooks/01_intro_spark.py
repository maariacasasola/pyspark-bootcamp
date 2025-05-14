from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, row_number
from pyspark.sql.window import Window

# Creating a Spark session
spark = SparkSession.builder.appName("PySpark01").getOrCreate()

# Reading CSV file with test data
df = spark.read.option("header", True).option("inferSchema", True).csv("../data/movies.csv")
df.printSchema()
df.show(5)

# Counting of elements
print("Total de películas:", df.count())
# Filtering and selecting columns
df_filtered = df.select("title", "genre", "rating").filter(col("rating") > 8.5)
# Grouping by genre
df_grouped = df_filtered.groupBy("genre").agg(avg("rating").alias("avg_rating"))
df_grouped.orderBy(col("avg_rating").desc()).show()

# Saving results as Parquet format
df_grouped.write.mode("overwrite").parquet("data/top_movies_by_genre.parquet")
# Reading Parquet file
parquet_df = spark.read.parquet("./data/top_movies_by_genre.parquet")
parquet_df.show(5)

# Creating and reading partitions
parquet_df.write.partitionBy("genre").parquet("data/partition_by_genre.parquet")
drama_data = spark.read.parquet("./data/partition_by_genre.parquet/genre=Drama")

# Applying Window function: Top 3 films by genre according to rating
window_spec = Window.partitionBy("genre").orderBy(col("rating").desc())
df_with_rank = df.withColumn("rank", row_number().over(window_spec))
df_top3 = df_with_rank.filter(col("rank") <= 3)
df_top3.show()

# Stop Spark session
spark.stop()