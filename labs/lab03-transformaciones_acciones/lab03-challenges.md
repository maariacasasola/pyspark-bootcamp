1. Configura el entorno:
    - Inicializa una sesión de Spark
    - Carga el fichero *data/movies.csv*
2. Asegura que los datos no tienen películas repetidas por movieId
3. Extrae el año de la columna 'title' en una nueva columna 'year' (Ej: "Toy Story (1995)" → 1995)
    - Tip: investiga regexp_extract y usa una expresión regular
4. Extrae el año más actual
5. Calcula la media de visualizaciones por género
    - Tip: investiga la función 'explode' para separar los géneros de cada película
6. Deten la sesión iniciada de Spark