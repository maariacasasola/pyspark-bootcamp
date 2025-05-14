# 🧪 PySpark Bootcamp Lab

Este repositorio contiene laboratorios prácticos diseñados para ayudarte a aprender a usar **Apache Spark con PySpark** en un entorno controlado y sin errores de configuración local. Aquí encontrarás ejercicios guiados, datasets, y un entorno Docker preconfigurado que evita los problemas típicos de instalación de Java, Spark o Hadoop en Windows.

---

## 📁 Información acerca del repositorio

Este proyecto contiene una serie de laboratorios educativos enfocados en la **ingeniería de datos**. El objetivo principal es facilitar el aprendizaje de tecnologías clave como:

- **Apache Spark / PySpark**
- Formato de archivos **Parquet**
- Manipulación de datos con **DataFrames**
- Ejecución en entornos **contenedorizados (Docker)**
- Buenas prácticas en notebooks

El repositorio incluye notebooks, código de transformación, datos de ejemplo, y un entorno preconfigurado con Docker que funciona perfectamente en Windows.

---

## ✅ Requisitos

Antes de comenzar, asegúrate de tener instalado lo siguiente:

- [Visual Studio Code](https://code.visualstudio.com/download) con la extensiones:
    - "Container Tools" (https://marketplace.visualstudio.com/items?itemName=ms-azuretools.vscode-containers)
    - "Dev Containers" (https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
- [Docker Desktop](https://www.docker.com/get-started/)
- Git (para clonar el repositorio)

---


## 🚀 Pasos para ejecutar el entorno

1. **Clona este repositorio:**

   ```bash
   git clone https://github.com/tu-usuario/pyspark-bootcamp.git
   cd pyspark-bootcamp
   ```

2. **Crea la imagen de Docker si no está creada y despliega un contenedor a partir de ella:**

    ```bash
    docker compose up
    ```

3. **Navega a la pestaña de la extensión 'Dev Containers', previamente instalada en VSC**
4. **Haz clic derecho encima del contenedor desplegado y selecciona 'Attach in Visual Studio Code' (esto abrirá una nueva ventana de VSC con el entorno del contenedor)**
5. **Navegar hasta /pyspark/notebooks/ y abrir el primer archivo de laboratorio "01_intro_spark.ipynb" para comenzar**

# Conceptos teóricos
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
- Transforma el plan lógico en plan físico eficiente.
- Aplica reglas como predicado pushdown, reordenamiento de joins, etc.

### Tungsten Engine
- Motor de ejecución optimizado que usa:
 - Generación de código en tiempo de ejecución (runtime code generation).
 - Manejo eficiente de memoria (evita objetos intermedios de JVM).
 - Vectorización de operaciones.
- Esto hace que Spark sea mucho más rápido que Hadoop MapReduce.

## PySpark: DataFrames, schema inference y transformaciones
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
pandas: Ideal para datasets < 1 GB, exploración rápida.
PySpark: Fundamental para Big Data (decenas o cientos de GB), procesamiento distribuido, producción