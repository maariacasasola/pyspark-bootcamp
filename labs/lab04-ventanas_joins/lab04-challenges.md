0. Configuración del entorono:
    - Iniciar sesión de Spark 
    - Cargar *../../data/movies.csv*
    - Cargar *../../data/ratings.csv*
1. Limpieza y preparación
    - Convertir timestamp a tipo fecha.
    - Extraer el año desde el campo title.
    - Validar que movieId es único en movies.
    - Comprobar que todas los ratings hacen referencia a películas existentes en movies.csv
2. Joins
    - Unir ratings y movies por movieId, añadiendo a ratings las columnas de movies: title, views y quality.
3. Aggregations
    - Calcular el rating promedio por cada película
    - Calcular el rating promedio por cada género
    - Calcular el número de votos por cada película
    - Obtener el/los usuario/s que tiene la puntuación más alta y el/los de la más baja
    - Obtener la/s película/s con más de 5 votos que tienen el rating más polarizado (mayor varianza)
4. Función Window
    - Ranking de películas mejor valoradas por género
    - Extraer el Top 2 de cada género y guardar en formato Parquet particionado por género. Tras esto, leer el top para el género 'Children' y 'Comedy'