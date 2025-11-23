#!/bin/bash
# Ejecuta el análisis batch completo con Hadoop y Apache Pig

set -e

echo "=========================================="
echo "ANÁLISIS BATCH - HADOOP & PIG"
echo "=========================================="

GREEN='\033[0.32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "\n${YELLOW}[1/5] Extrayendo datos de PostgreSQL...${NC}"
docker-compose run --rm batch-extractor python data_extractor.py

if [ ! -f "./batch_analysis/data/yahoo_answers.txt" ] || [ ! -f "./batch_analysis/data/llm_answers.txt" ]; then
    echo "Error: No se generaron los archivos de datos"
    exit 1
fi

echo -e "${GREEN}Datos extraídos exitosamente${NC}"

echo -e "\n${YELLOW}[2/5] Copiando datos a HDFS...${NC}"
docker-compose exec hadoop-namenode bash -c "
    export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
    hdfs dfs -rm -r /input/* 2>/dev/null || true
    hdfs dfs -put /data/yahoo_answers.txt /input/
    hdfs dfs -put /data/llm_answers.txt /input/
    echo 'Archivos en HDFS:'
    hdfs dfs -ls /input/
"

echo -e "${GREEN}Datos cargados en HDFS${NC}"

echo -e "\n${YELLOW}[3/5] Ejecutando análisis Pig para Yahoo! Answers...${NC}"
docker-compose exec hadoop-namenode bash -c "
    export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
    export PIG_CLASSPATH=/opt/hadoop/etc/hadoop
    hdfs dfs -rm -r /output/yahoo* 2>/dev/null || true
    pig -x mapreduce /pig_scripts/wordcount_yahoo.pig
"

echo -e "${GREEN}Análisis de Yahoo! completado${NC}"

echo -e "\n${YELLOW}[4/5] Ejecutando análisis Pig para LLM...${NC}"
docker-compose exec hadoop-namenode bash -c "
    export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
    export PIG_CLASSPATH=/opt/hadoop/etc/hadoop
    hdfs dfs -rm -r /output/llm* 2>/dev/null || true
    pig -x mapreduce /pig_scripts/wordcount_llm.pig
"

echo -e "${GREEN}Análisis de LLM completado${NC}"

echo -e "\n${YELLOW}[5/5] Ejecutando análisis comparativo...${NC}"
docker-compose exec hadoop-namenode bash -c "
    export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
    export PIG_CLASSPATH=/opt/hadoop/etc/hadoop
    hdfs dfs -rm -r /output/comparative 2>/dev/null || true
    hdfs dfs -rm -r /output/yahoo_unique 2>/dev/null || true
    hdfs dfs -rm -r /output/llm_unique 2>/dev/null || true
    hdfs dfs -rm -r /output/common_differences 2>/dev/null || true
    pig -x mapreduce /pig_scripts/comparative_analysis.pig
"

echo -e "${GREEN}Análisis comparativo completado${NC}"

echo -e "\n${YELLOW}[6/6] Descargando resultados...${NC}"
docker-compose exec hadoop-namenode bash -c "
    export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
    hdfs dfs -get /output/yahoo /results/
    hdfs dfs -get /output/llm /results/
    hdfs dfs -get /output/comparative /results/
    hdfs dfs -get /output/yahoo_unique /results/ 2>/dev/null || true
    hdfs dfs -get /output/llm_unique /results/ 2>/dev/null || true
    hdfs dfs -get /output/common_differences /results/ 2>/dev/null || true
"

echo -e "${GREEN}Resultados descargados${NC}"

echo -e "\n${YELLOW}[7/7] Generando visualizaciones...${NC}"
docker-compose run --rm -v $(pwd)/batch_analysis:/app -v $(pwd)/batch_analysis/results:/results \
    batch-extractor bash -c "
    pip install -q -r requirements_viz.txt && 
    python visualize_results.py
"

echo -e "${GREEN}Visualizaciones generadas${NC}"

echo ""
echo "=========================================="
echo "ANÁLISIS COMPLETADO"
echo "=========================================="
echo ""
echo "Resultados disponibles en:"
echo "  - ./batch_analysis/results/"
echo ""
echo "Visualizaciones disponibles en:"
echo "  - ./batch_analysis/results/visualizations/"
echo ""
echo "Para ver la Web UI de HDFS:"
echo "  http://localhost:9870"
echo ""
