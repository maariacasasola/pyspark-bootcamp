# 🧪 PySpark Bootcamp Lab

Este repositorio contiene laboratorios prácticos diseñados para ayudarte a aprender a usar **Apache Spark con PySpark** en un entorno controlado y sin errores de configuración local. Aquí encontrarás ejercicios guiados, datasets, y un entorno Docker preconfigurado que evita los problemas típicos de instalación de Java, Spark o Hadoop en Windows.

---

## 📁 Información acerca del repositorio

Este proyecto contiene una serie de laboratorios educativos enfocados en la **ingeniería de datos**. El objetivo principal es facilitar el aprendizaje de tecnologías clave como:

- **Apache Spark / PySpark / Spark SQL**
- Formato de archivos **Parquet**
- Manipulación de datos con **DataFrames**
- Ejecución en entornos **contenedorizados (Docker)**
- Buenas prácticas en notebooks

El repositorio incluye notebooks, código de transformación, datos de ejemplo, y un entorno preconfigurado con Docker sobre Windows. La información aportada en cada laboratorio se estructura de la siguiente manera:

| Laboratorio | Título | Detalle |
| ------------- | -------------------------- | ---------- |
| Lab 01 | **Introducción y arquitectura** | Qué es Spark. Cluster vs. local. Driver, executor, worker, jobs, stages y tasks. Configuración de entorno: inicio y detención de sesiones |
| Lab 02 | **RDDs vs. DataFrames** | Cómo se crean y manipulan. Ventajas y desventajas. Casos en los que aún se usan RDDs |
| Lab 03 | **Transformaciones y acciones** | Lazy evaluation y su importancia. Transformaciones. Acciones. Aggregate. |
| Lab 04 | **Joins y ventanas** | Tipos de Joins. Funciones ventana. Uso en casos reales |
| Lab 05 | **Spark SQL** | Crear tablas temporales, consultas SQL. Optimización con Catalyst |
| Lab 06 | **User Defined Functions (UDFs)** | Funciones definidas por el usuario (UDF) |
| Lab 07 | **Optimización** | Caché y persistencia. Reparticionamiento. Plan de ejecución. Escritura eficiente con Parquet y función explain() en profundidad |

## 🧠 Contenido actual
*Linaje de directiorios*
```
pyspark-bootcamp/
│
├── readme.md│
│
├── data/
│   └── [archivos CSV, Parquet de práctica]
│
└── labs/
    ├── 01_intro_spark/
    │   └── [contenido laboratorio]
    ├── 02_rdds_vs_dataframes/
    │   └── [contenido laboratorio]
    ├── 03_transformaciones_acciones/
    │   └── [contenido laboratorio]
    ├── 04_ventanas_joins/
    │   └── [contenido laboratorio]
    ├── 05_spark_sql_basico/
    │   └── [contenido laboratorio]
    ├── 06_user_defined_functions/
    │   └── [contenido laboratorio]
    └── 07_optimizacion/
        └── [contenido laboratorio]
```

**data/**: contiene datasets con los datos de ejemplo que se usan en los distintos laboratorios.

**labs/**: contiene los directorios de los laboratorios que se usan para realizar este bootcamp.

Todos los laboratorios contienen un archivo *XX_lab_name.ipynb*, siendo XX el número y lab_name el nombre específico del laboratorio. Además, la mayoría de ellos cuentan con un archivo *labXX-challenges.md* adicional con los enunciados de ejercicios propuestos para practicar lo aprendido en el laboratorio que corresponda. Finalmente, los archivos *labXX-solutions.ipynb* contienen las soluciones de dichos ejercicios propuestos.


### Ejecución de Notebooks
Cada notebook está diseñado para ser ejecutado de forma independiente. Se recomienda seguir el orden numérico para una progresión lógica del aprendizaje.

## ✅ Requisitos
Antes de comenzar, asegúrate de tener instalado lo siguiente:

- [Python 3.11](https://www.python.org/downloads/)
    - Añadir a tus [variables de entorno](https://tecnoloco.istocks.club/como-agregar-python-a-la-variable-path-de-windows-wiki-util/2020-10-14/)
- [Visual Studio Code](https://code.visualstudio.com/download) con las extensiones:
    - ["Container Tools"](https://marketplace.visualstudio.com/items?itemName=ms-azuretools.vscode-containers)
    - ["Dev Containers"](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
- [Docker Desktop](https://www.docker.com/get-started/)
- [Git](https://git-scm.com/downloads) (para clonar el repositorio)

## 🚀 Pasos para ejecutar el entorno

1. **Ejecuta el siguiente comando para clonar este repositorio en git bash (o descarga el proyecto en .zip):**

   ```bash
   git clone https://github.com/tu-usuario/pyspark-bootcamp.git
   cd pyspark-bootcamp
   ```

2. **Ejecuta este comando para crear la imagen de Docker, si no está creada, y desplegar un contenedor a partir de ella:**

    ```bash
    docker compose up
    ```

3. **Navega a la pestaña 'Containers', previamente instalada en VSC**
4. **Haz clic derecho encima del contenedor desplegado y selecciona 'Attach Visual Studio Code' (esto abrirá una nueva ventana de VSC en el entorno del contenedor)**
5. **Navega hasta /pyspark/labs/ y abre la carpeta del laboratorio que te interese para comenzar. Lo primero que debes hacer al abrir un notebook es seleccionar el kernel de Python (en mi caso Python 3.11)**

---

# 💡 Conceptos teóricos

## ¿Qué es Apache Spark y cómo funciona internamente?
Apache Spark es un motor de procesamiento distribuido en memoria que permite ejecutar tareas de análisis de datos a gran escala. Su éxito se debe a su velocidad, facilidad de uso y flexibilidad.

### ¿Cómo funciona Spark internamente?
RDDs (Resilient Distributed Datasets)
- Son la unidad de datos fundamental de Spark.
- Son inmutables, tolerantes a fallos, y se dividen en particiones que se distribuyen entre los nodos del clúster.
- Ya no se usan tanto directamente en PySpark, pero son la base sobre la que funcionan los DataFrames.

### DAG (Directed Acyclic Graph)
- Cada acción en Spark (como collect(), write()) genera un plan de ejecución.
- Spark convierte tu código en una serie de transformaciones (como select, filter, groupBy), que forman un grafo dirigido acíclico (DAG).
- El DAG Scheduler divide este grafo en etapas (stages) y tareas (tasks), que luego se ejecutan en paralelo.

### Catalyst Optimizer
- Es el optimizador de consultas de Spark SQL.
- Transforma el plan lógico en un "plan físico" eficiente.
- Aplica reglas como predicado pushdown, reordenamiento de joins, etc.

### Tungsten Engine
- Motor de ejecución optimizado que usa:
 - Generación de código en tiempo de ejecución (runtime code generation).
 - Manejo eficiente de memoria (evita objetos intermedios de JVM).
 - Vectorización de operaciones.
- Esto hace que Spark sea mucho más rápido que Hadoop MapReduce.

## 🧩 PySpark: DataFrames, schema inference y transformaciones
### ¿Cómo funciona DataFrame en PySpark?
Es una colección distribuida de datos organizados en columnas (como una tabla de SQL o un DataFrame de pandas) construido sobre RDDs, pero con optimización automática gracias a Catalyst.
Spark puede inferir automáticamente los tipos de datos (inferSchema=True) o puedes especificarlos manualmente con StructType.


| Característica                | pandas                 | PySpark                            |
| ----------------------------- | ---------------------- | ---------------------------------- |
| Escalabilidad                 | Memoria local          | Procesamiento distribuido          |
| Velocidad en grandes datasets | Lenta                  | Muy rápida (paralelismo + memoria) |
| Lazy evaluation               | ❌ No                   | ✅ Sí                               |
| Tipado                        | Dinámico               | Inferido o explícito               |
| Sintaxis                      | Muy expresiva/flexible | Más declarativa y estricta         |
| Uso típico                    | Exploración local      | Procesamiento a gran escala        |

### ¿Cuándo usar cada uno?
- Pandas: Ideal para datasets < 1 GB, exploración rápida.
- PySpark: Fundamental para Big Data (decenas o cientos de GB), procesamiento distribuido, producción.

---

# 🧑‍💻 Autor
Creado y documentado por María Casasola como parte de un proceso de aprendizaje profesional en ingeniería de datos.
Para dudas, puedes contactarme vía [LinkedIn](https://www.linkedin.com/in/mar%C3%ADa-casasola-calzadilla-303970184/).

## 🤝 Contribuciones
Este repositorio está en constante mejora. Si tienes ideas, correcciones o quieres colaborar, ¡haz un fork y crea un pull request!
