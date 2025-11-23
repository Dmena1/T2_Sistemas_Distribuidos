# Ejecuta el análisis batch completo con Hadoop y Apache Pig (Windows)

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "ANÁLISIS BATCH - HADOOP & PIG" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

Write-Host "`n[1/7] Extrayendo datos de PostgreSQL..." -ForegroundColor Yellow
docker-compose run --rm batch-extractor python data_extractor.py

if (-not (Test-Path ".\batch_analysis\data\yahoo_answers.txt") -or -not (Test-Path ".\batch_analysis\data\llm_answers.txt")) {
    Write-Host "Error: No se generaron los archivos de datos" -ForegroundColor Red
    exit 1
}

Write-Host "Datos extraídos exitosamente" -ForegroundColor Green

Write-Host "`n[2/7] Copiando datos a HDFS..." -ForegroundColor Yellow
docker-compose exec -T hadoop-namenode bash -c @"
    export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
    hdfs dfs -rm -r /input/* 2>/dev/null || true
    hdfs dfs -put /data/yahoo_answers.txt /input/
    hdfs dfs -put /data/llm_answers.txt /input/
    echo 'Archivos en HDFS:'
    hdfs dfs -ls /input/
"@

Write-Host "Datos cargados en HDFS" -ForegroundColor Green

Write-Host "`n[3/7] Ejecutando análisis Pig para Yahoo! Answers..." -ForegroundColor Yellow
docker-compose exec -T hadoop-namenode bash -c @"
    export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
    export PIG_CLASSPATH=/opt/hadoop/etc/hadoop
    hdfs dfs -rm -r /output/yahoo* 2>/dev/null || true
    pig -x mapreduce /pig_scripts/wordcount_yahoo.pig
"@

Write-Host "Análisis de Yahoo! completado" -ForegroundColor Green

Write-Host "`n[4/7] Ejecutando análisis Pig para LLM..." -ForegroundColor Yellow
docker-compose exec -T hadoop-namenode bash -c @"
    export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
    export PIG_CLASSPATH=/opt/hadoop/etc/hadoop
    hdfs dfs -rm -r /output/llm* 2>/dev/null || true
    pig -x mapreduce /pig_scripts/wordcount_llm.pig
"@

Write-Host "Análisis de LLM completado" -ForegroundColor Green

Write-Host "`n[5/7] Ejecutando análisis comparativo..." -ForegroundColor Yellow
docker-compose exec -T hadoop-namenode bash -c @"
    export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
    export PIG_CLASSPATH=/opt/hadoop/etc/hadoop
    hdfs dfs -rm -r /output/comparative 2>/dev/null || true
    hdfs dfs -rm -r /output/yahoo_unique 2>/dev/null || true
    hdfs dfs -rm -r /output/llm_unique 2>/dev/null || true
    hdfs dfs -rm -r /output/common_differences 2>/dev/null || true
    pig -x mapreduce /pig_scripts/comparative_analysis.pig
"@

Write-Host "Análisis comparativo completado" -ForegroundColor Green

Write-Host "`n[6/7] Descargando resultados..." -ForegroundColor Yellow
docker-compose exec -T hadoop-namenode bash -c @"
    export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
    hdfs dfs -get /output/yahoo /results/
    hdfs dfs -get /output/llm /results/
    hdfs dfs -get /output/comparative /results/
    hdfs dfs -get /output/yahoo_unique /results/ 2>/dev/null || true
    hdfs dfs -get /output/llm_unique /results/ 2>/dev/null || true
    hdfs dfs -get /output/common_differences /results/ 2>/dev/null || true
"@

Write-Host "Resultados descargados" -ForegroundColor Green

Write-Host "`n[7/7] Generando visualizaciones..." -ForegroundColor Yellow
$currentPath = (Get-Location).Path
docker-compose run --rm -v "${currentPath}/batch_analysis:/app" -v "${currentPath}/batch_analysis/results:/results" batch-extractor bash -c "pip install -q -r requirements_viz.txt && python visualize_results.py"

Write-Host "Visualizaciones generadas" -ForegroundColor Green

Write-Host "`n==========================================" -ForegroundColor Cyan
Write-Host "ANÁLISIS COMPLETADO" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "`nResultados disponibles en:" -ForegroundColor White
Write-Host "  - .\batch_analysis\results\" -ForegroundColor White
Write-Host "`nVisualizaciones disponibles en:" -ForegroundColor White
Write-Host "  - .\batch_analysis\results\visualizations\" -ForegroundColor White
Write-Host "`nPara ver la Web UI de HDFS:" -ForegroundColor White
Write-Host "  http://localhost:9870" -ForegroundColor Cyan
Write-Host ""
