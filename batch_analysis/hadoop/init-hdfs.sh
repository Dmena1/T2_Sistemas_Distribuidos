#!/bin/bash
# Script de inicialización de HDFS

echo "========================================="
echo "Inicializando HDFS"
echo "========================================="

# Configurar variables de entorno de Hadoop
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root

if [ ! -d "/hadoop/dfs/name/current" ]; then
    echo "Formateando HDFS..."
    hdfs namenode -format -force -clusterID hadoop-cluster
else
    echo "HDFS ya está formateado"
fi

echo "Iniciando NameNode..."
hdfs namenode &

echo "Esperando a que NameNode esté listo..."
for i in {1..30}; do
    if netstat -tuln 2>/dev/null | grep -q ':9000 ' || ss -tuln 2>/dev/null | grep -q ':9000 '; then
        echo "NameNode está escuchando en puerto 9000"
        break
    fi
    echo "Esperando... ($i/30)"
    sleep 2
done

sleep 10

echo "Creando directorios en HDFS..."
hdfs dfs -mkdir -p /input 2>/dev/null || true
hdfs dfs -mkdir -p /output 2>/dev/null || true

echo "HDFS inicializado correctamente"
echo "Directorios creados:"
hdfs dfs -ls / 2>/dev/null || echo "HDFS aún no está completamente listo"

echo "NameNode corriendo. Manteniendo contenedor activo..."
tail -f /dev/null
