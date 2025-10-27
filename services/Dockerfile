FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
CMD ["bash", "-c", "python /app/traffic_generator.py & python /app/llm_worker.py & python /app/retry_worker.py & python /app/storage_consumer.py"]
