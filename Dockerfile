FROM jupyter/pyspark-notebook:spark-3.4.0

USER root

# Copia dependencias
COPY requirements.txt /tmp/

# Instala paquetes necesarios (ya trae Python 3.11 y pip)
RUN pip install --upgrade pip && \
    pip install -r /tmp/requirements.txt

# Crea carpeta de trabajo
WORKDIR /home/pyspark

# Expone el puerto de Jupyter
EXPOSE 8888

CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
