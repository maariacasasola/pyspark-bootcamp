0. Configuración de entorno
    - Inicia la sesión de Spark
    - Carga los archivos *../data/yellow_tripdata_2023-01.parquet* que contiene datos de viajes en taxi.
1.  Clasificación de distancia de viaje. Crea una UDF que clasifique los viajes según su distancia:
    - "corta" si < 2 millas
    - "media" si entre 2 y 6 millas
    - "larga" si > 6 millas
2. ¿Viaje nocturno o diurno? Crea una UDF que analice la hora del pickup y devuelva:
    - "noche" si es entre 20:00 y 06:00
    - "día" en cualquier otro caso
3. Categoría de propina. Crea una UDF que clasifique el tip_amount en categorías:
    - "sin propina" si tip == 0
    - "baja" si tip < 2
    - "media" si tip entre 2 y 6
    - "alta" si tip > 6
4. Construye un DataFrame con varias UDFs aplicadas, que incluya las columnas:
    - Distancia categorizada (reto 1)
    - Propina categorizada (reto 3)
    - Franja temporal [mañana, tarde, noche, madrugada] (en base a la hora de pickup)
    - Nueva columna: "perfil" que concatene las 3 anteriores (ejemplo: "medio|alta|noche")
    🎯 Este tipo de clasificación por perfiles se usa mucho en feature engineering para modelos de ML.
5. Analiza los perfiles más comunes y el promedio de total_amount por perfil.
6. Crear una columna llamada "viaje_sospechoso" que indique si un viaje es posiblemente anómalo, según una combinación de condiciones:
    - Duración superior a 120 minutos
    - Distancia mayor a 30 millas
    - Precio total excesivo (más de 250)
    - Propina superior al 100% del fare_amount
    Si al menos dos condiciones se cumplen, se debe marcar el viaje como 'sospechoso'. Si no, como 'normal'. Implementa la lógica con una sola UDF.