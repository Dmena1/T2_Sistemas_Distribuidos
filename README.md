DIEGO CAÑA Y DIEGO MENA

# Tarea 3: Análisis Batch

Sistema de análisis batch que procesa respuestas almacenadas para extraer estadísticas y patrones lingüísticos. Utiliza **Apache Hadoop (HDFS)** y **Apache Pig** para análisis de frecuencia de palabras y comparación de vocabulario entre respuestas de Yahoo! Answers y respuestas generadas por un LLM.

## Características

- Extracción de datos desde PostgreSQL
- Procesamiento distribuido con Hadoop HDFS
- Análisis de texto con Apache Pig (MapReduce)
- Tokenización, limpieza y filtrado de stopwords
- Análisis comparativo de vocabulario
- Generación de visualizaciones (gráficos y nubes de palabras)
- Containerización completa con Docker

## Requisitos

- Docker y docker-compose
- Git Bash o WSL (para ejecutar scripts bash en Windows)

## Instalación y Ejecución

### 1. Preparar datos

```bash
# Levantar PostgreSQL
docker-compose up -d postgres

# Esperar 10 segundos
sleep 10

# Generar datos de prueba
docker-compose run --rm batch-extractor python populate_test_data.py
```

### 2. Ejecutar análisis completo

```bash
# Opción A: Script bash (Linux/Mac/Git Bash)
bash batch_analysis/run_analysis.sh

# Opción B: Script PowerShell (Windows)
.\batch_analysis\run_analysis.ps1
```

El script ejecuta automáticamente:
1. Extracción de datos desde PostgreSQL
2. Carga de datos a HDFS
3. Análisis Pig para Yahoo! Answers
4. Análisis Pig para LLM
5. Análisis comparativo
6. Descarga de resultados
7. Generación de visualizaciones

### 3. Ejecución manual

```bash
# 1. Levantar Hadoop
docker-compose up -d hadoop-namenode hadoop-datanode

# 2. Esperar 60 segundos para que HDFS esté listo
sleep 60

# 3. Extraer datos
docker-compose run --rm batch-extractor python data_extractor.py

# 4. Copiar datos a HDFS
docker-compose exec hadoop-namenode bash -c "export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop && hdfs dfs -put /data/yahoo_answers.txt /input/ && hdfs dfs -put /data/llm_answers.txt /input/"

# 5. Ejecutar análisis Pig
docker-compose exec hadoop-namenode bash -c "export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop && export PIG_CLASSPATH=/opt/hadoop/etc/hadoop && pig -x mapreduce /pig_scripts/wordcount_yahoo.pig"
docker-compose exec hadoop-namenode bash -c "export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop && export PIG_CLASSPATH=/opt/hadoop/etc/hadoop && pig -x mapreduce /pig_scripts/wordcount_llm.pig"
docker-compose exec hadoop-namenode bash -c "export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop && export PIG_CLASSPATH=/opt/hadoop/etc/hadoop && pig -x mapreduce /pig_scripts/comparative_analysis.pig"

# 6. Descargar resultados
docker-compose exec hadoop-namenode bash -c "export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop && hdfs dfs -get /output/* /results/"

# 7. Generar visualizaciones
docker-compose run --rm -v $(pwd)/batch_analysis:/app -v $(pwd)/batch_analysis/results:/results batch-extractor bash -c "pip install -q -r requirements_viz.txt && python visualize_results.py"
```

## Resultados

Los resultados se generan en:
- `batch_analysis/results/yahoo/` - Top 100 palabras de Yahoo!
- `batch_analysis/results/llm/` - Top 100 palabras del LLM
- `batch_analysis/results/comparative/` - Análisis comparativo completo
- `batch_analysis/results/visualizations/` - Gráficos y nubes de palabras

### Visualizaciones generadas

- `yahoo_top50_bar.png` - Gráfico de barras Yahoo!
- `llm_top50_bar.png` - Gráfico de barras LLM
- `yahoo_wordcloud.png` - Nube de palabras Yahoo!
- `llm_wordcloud.png` - Nube de palabras LLM
- `comparison_side_by_side.png` - Comparación lado a lado
- `summary_statistics.csv` - Estadísticas resumidas

## Servicios y Puertos

- **HDFS Web UI**: http://localhost:9870
- **HDFS NameNode**: localhost:9000
- **PostgreSQL**: localhost:55432

## Estructura del Proyecto

```
.
├── batch_analysis/              # Módulo de análisis batch
│   ├── hadoop/                 # Configuración de Hadoop
│   │   ├── config/             # Archivos de configuración HDFS
│   │   └── init-hdfs.sh       # Script de inicialización
│   ├── pig/                    # Scripts de Apache Pig
│   │   ├── wordcount_yahoo.pig
│   │   ├── wordcount_llm.pig
│   │   ├── comparative_analysis.pig
│   │   └── stopwords.txt
│   ├── data_extractor.py       # Extracción de datos
│   ├── populate_test_data.py   # Generador de datos de prueba
│   ├── visualize_results.py    # Generación de visualizaciones
│   ├── run_analysis.sh         # Script maestro (bash)
│   └── run_analysis.ps1        # Script maestro (PowerShell)
├── docker-compose.yml          # Orquestación de servicios
└── README.md                   # Este archivo
```

## Tecnologías Utilizadas

- **Apache Hadoop 3.3.6** - Procesamiento distribuido
- **Apache Pig 0.17.0** - Lenguaje de alto nivel para MapReduce
- **HDFS** - Sistema de archivos distribuido
- **PostgreSQL** - Base de datos relacional
- **Docker** - Containerización
- **Python 3.9** - Scripts de extracción y visualización
