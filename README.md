# Tarea2 - Sistemas Distribuidos: Pipeline con Kafka y Flink

## Requisitos

- Docker & docker-compose
- (Opcional) Java si usarás Flink localmente

## Ejecución (modo prueba)

1. Construir y levantar servicios:
   docker-compose up --build

2. Ver logs:
   docker-compose logs -f services

3. La arquitectura: Kafka (9092), Zookeeper (2181), Flink (8081), Postgres (5432)
4. Revisar la tabla `answers` en Postgres para ver las respuestas persistidas.

## Notas

- Ajusta `traffic_generator.py` para usar tu dataset (Tarea1).
- Sustituye la función `score_function` en `flink/flink_job.py` por la función que desarrollaste en la Tarea1.
- Para producción, separa cada worker en su propio servicio Docker.
