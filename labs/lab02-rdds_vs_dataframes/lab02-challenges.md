0. Configuración del entorno:
    - Importa los paquetes:
    ```python
        from pyspark.sql import SparkSession
        from pyspark.sql import Row
    ```
    - Inicia una sesión de Spark para comenzar
    - Crea un RDD a partir de una lista de tuplas con nombres y edades:
    ```python
        l = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    ```
1. Filtra a las personas mayores de 40 años.
2. Convierte el RDD a DataFrame y vuelve a aplicar el filtro.
3. Compara el número de líneas de código y legibilidad.