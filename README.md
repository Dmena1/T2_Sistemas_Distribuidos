# Tarea 2 – Pipeline Asíncrono con Kafka y Flink

Este proyecto implementa una arquitectura desacoplada para comparar respuestas de un LLM con un dataset histórico (Yahoo! Answers). Incorpora Kafka para la orquestación asíncrona, manejo de errores con reintentos diferenciados y un feedback loop de calidad con Flink (simulado y real opcional).

## Requisitos

- Docker y docker-compose

## Cómo ejecutar
1.-Clonar repositorio:
```bash
   git clone "url del repositorio"
   ```

2. Levantar todo:

   ```bash
   docker-compose up --build -d
   ```
