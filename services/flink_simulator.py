import os
import json
import time
from kafka import KafkaConsumer, KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("flink_simulator")

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
UMBRAL = float(os.getenv("SCORE_UMBRAL", "0.3"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

def create_kafka_connection():
    """Crear conexión a Kafka con reconexión automática"""
    max_retries = 5
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                "llm_responses.success",
                bootstrap_servers=KAFKA,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='flink-simulator-group',
                request_timeout_ms=40000,
                session_timeout_ms=30000
            )
            
            producer_persist = KafkaProducer(
                bootstrap_servers=KAFKA,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3,
                retry_backoff_ms=1000
            )
            
            producer_retry = KafkaProducer(
                bootstrap_servers=KAFKA,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3,
                retry_backoff_ms=1000
            )
            
            producer_dead_letter = KafkaProducer(
                bootstrap_servers=KAFKA,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3,
                retry_backoff_ms=1000
            )
            
            logger.info("Successfully connected to Kafka")
            return consumer, producer_persist, producer_retry, producer_dead_letter
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
    
    raise Exception("Failed to connect to Kafka after all retries")

def score_function(question, answer):
    if not answer or len(answer.strip()) == 0:
        return 0.0
    
    q_words = len(question.split())
    a_words = len(answer.split())
    
    if a_words == 0:
        return 0.0
    
    length_ratio = min(1.0, a_words / max(1, q_words))
    
    completeness = min(1.0, a_words / 50.0)
    
    quality_indicators = 0.0
    if "completa" in answer.lower() or "detallada" in answer.lower():
        quality_indicators += 0.2
    if "ejemplo" in answer.lower() or "ejemplos" in answer.lower():
        quality_indicators += 0.2
    if len(answer) > 100:
        quality_indicators += 0.1
    
    score = (length_ratio * 0.4) + (completeness * 0.3) + (quality_indicators * 0.3)
    return min(1.0, score)

def main():
    logger.info("Iniciando el simulador Flink")
    
    try:
        consumer, producer_persist, producer_retry, producer_dead_letter = create_kafka_connection()
        
        logger.info("Escuchar las respuestas de LLM para calificar")
        
        processed_count = 0
        persisted_count = 0
        retried_count = 0
        
        for message in consumer:
            data = message.value
            processed_count += 1
            
            logger.info(f"Processing [{processed_count}]: {data['id']}")
            
            score = score_function(data["question"], data["answer"])
            data["score"] = round(score, 3)
            data["scored_at"] = time.time()
            
            attempts = data.get("attempts", 0)
            
            if attempts >= MAX_RETRIES:
                data["final_disposition"] = "max_retries_exceeded"
                producer_dead_letter.send("dead_letter", value=data)
                logger.info(f"DEAD LETTER: {data['id']} - Max retries ({attempts})")
                
            elif score >= UMBRAL:
                producer_persist.send("persist", value=data)
                persisted_count += 1
                logger.info(f"PERSIST: {data['id']} - Good score ({score}) - Total persisted: {persisted_count}")
                
            else:
                data["attempts"] = attempts + 1
                data["retry_reason"] = f"low_score_{score}"
                producer_retry.send("questions", value=data)
                retried_count += 1
                logger.info(f"RETRY: {data['id']} - Low score ({score}) - Attempt {data['attempts']} - Total retried: {retried_count}")
            
            producer_persist.flush()
            producer_retry.flush()
            producer_dead_letter.flush()
            
            if processed_count % 10 == 0:
                logger.info(f"FLINK METRICS - Processed: {processed_count}, Persisted: {persisted_count}, Retried: {retried_count}")
            
    except Exception as e:
        logger.error(f"Flink Simulator failed: {e}")

if __name__ == "__main__":
    main()