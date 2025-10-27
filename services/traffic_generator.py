import time, json, os, random
from kafka import KafkaProducer

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
producer = KafkaProducer(bootstrap_servers=KAFKA,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# ejemplo simple de preguntas
questions_pool = [
    {"id": "q1", "text": "¿Qué es una red neuronal?"},
    {"id": "q2", "text": "¿Cómo funciona Kafka?"},
    {"id": "q3", "text": "¿Qué es la Transformada de Fourier?"}
]

def produce():
    i = 0
    while True:
        q = random.choice(questions_pool)
        msg = {
            "id": f"{q['id']}_{int(time.time())}_{i}",
            "question": q["text"],
            "attempts": 0,
            "max_attempts": 5
        }
        producer.send("questions", value=msg)
        producer.flush()
        print("Produced question:", msg["id"])
        i += 1
        time.sleep(2)  # ritmo de generación (ajusta según necesidad)

if __name__ == "__main__":
    produce()
