0. Prepara el entorno:
    - Inicia una sesión de Spark y lee el archivo '../../data/movies.csv'
1. Calcula el top 3 de películas según visualizaciones por género, usa:
    - Window.partitionBy().orderBy()
    - Método explode()
2. Guarda el resultado final con estas columnas: [movieId; title; year; genre]. Después, particiona por género. Finalmente, lee solo las películas del género 'Adventure'.