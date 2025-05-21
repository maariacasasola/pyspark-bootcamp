from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySpark04").getOrCreate()
# Cargar el dataset de viajes
yellow_df = spark.read.parquet("../../data/yellow_tripdata_2023-01.parquet")
yellow_df.createOrReplaceTempView("yellow_trips")

# Define una función en Python
def clasificar_viaje(distancia):
    if distancia < 2:
        return 'short'
    elif distancia <= 10:
        return 'medium'
    else:
        return 'long'

# Se registra como UDF
clasificar_udf = udf(clasificar_viaje, StringType())

# Se aplica a un DataFrame
df = yellow_df.withColumn("trip_type", clasificar_udf("trip_distance"))
df.select("trip_distance", "trip_type").show()

# También se puede registrar y llamar desde sql 
spark.udf.register("clasificar_udf", clasificar_viaje, StringType())
spark.sql("""SELECT trip_distance, clasificar_udf(trip_distance) AS trip_type FROM yellow_trips""").show()

spark.stop()