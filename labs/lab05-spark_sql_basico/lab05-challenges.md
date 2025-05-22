1. Configuración de entorno
    - Inicia la sesión de Spark
    - Carga los archivos *../data/taxi_zone_lookup.csv* que contiene información sobre zonas de recogida de taxis, y *../data/yellow_tripdata_2023-01.parquet* que contiene datos de viajes en taxi.
    - Crea vistas temporales para trabajar con los dos conjuntos de datos cargados.
2. ¿Cuáles son las 5 zonas con más viajes iniciados?
    + Tips:
        - JOIN con PULocationID
        - Agrupa por zona
        - Cuenta viajes
3. ¿Cuál es la propina promedio (tip_amount) para cada zona de destino?
    + Tips:
        - JOIN con DOLocationID
        - Agrupa por zona
        - Ordena de mayor a menor
4. ¿Qué zonas tienen los viajes más duraderos en promedio?
    + Tips:
        - JOIN con PULocationID
        - Agrupa por zona
        - Calcula diferencia entre momento de salida y momento de llegada
5. ¿Cuántos viajes hubo por Borough de origen y por franja horaria (mañana/tarde/noche/madrugada)?
    + Tips:
        - JOIN con PULocationID + taxi_zones
        - Extrae hora del tpep_pickup_datetime
        - Crea CASE para categorizar horarios:
            ```sql
            CASE 
                WHEN hour BETWEEN 5 AND 11 THEN 'mañana'
                WHEN hour BETWEEN 12 AND 17 THEN 'tarde'
                WHEN hour BETWEEN 18 AND 23 THEN 'noche'
                ELSE 'madrugada'
            END AS franja
            ```
        - Agrupa por borough y franja
6. ¿Qué zonas (de recogida) generan más ingresos totales (total_amount)?
    + Tips:
        - JOIN con PULocationID
        - Agrupa por zona
        - SUM del total_amount
        - Filtra zonas con al menos 10.000 viajes (usa HAVING)
7. Calcula el desvío estándar de tip_amount por zona de destino, y encuentra las zonas con más variación.
    + Tips:
        - Usa STDDEV(tip_amount)
        - JOIN con DOLocationID
        - Ordena de mayor a menor