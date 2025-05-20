from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.window import Window

# Crear la sesión de Spark
spark = SparkSession.builder.appName("PySpark03").getOrCreate()

# Cargar datos
df = spark.read.option("header", True).csv("../../data/taxi_zone_lookup.csv")
# Ver las primeras filas
print("Datos de taxi_zones:")
df.show(5)
# Esquema del DataFrame
df.printSchema()

# Crear vista temporal
df.createOrReplaceTempView("taxi_zones")

print("¿Cuántas zonas hay por borough?")
spark.sql("""
    SELECT Borough, COUNT(DISTINCT Zone) AS num_zones
    FROM taxi_zones
    GROUP BY Borough
    ORDER BY num_zones DESC
""").show()

print("¿Cuántos LocationID hay por zona de servicio?")
spark.sql("""
    SELECT service_zone, COUNT(*) AS location_count
    FROM taxi_zones
    GROUP BY service_zone
""").show()

print("Función explain:")
spark.sql("""
    SELECT service_zone, COUNT(*) AS location_count
    FROM taxi_zones
    GROUP BY service_zone
""").explain(True)

# JOIN con Spark SQL
# Cargar el dataset de viajes
trips_df = spark.read.parquet("../../data/yellow_tripdata_2023-01.parquet")
# Mostrar algunas filas
trips_df.show(5)
# Ver el esquema
trips_df.printSchema()

trips_df.createOrReplaceTempView("yellow_trips")

print("¿Cuáles son las zonas con más viajes iniciados?")
spark.sql("""
    SELECT z.Zone AS pickup_zone, COUNT(*) AS num_trips
    FROM yellow_trips t
    JOIN taxi_zones z
    ON t.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY num_trips DESC
    LIMIT 10
""").show()

spark.stop()