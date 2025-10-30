import time, json, os, random
from kafka import KafkaProducer

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
producer = KafkaProducer(bootstrap_servers=KAFKA,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

questions_pool = [
    {"id": "q1", "text": "¿Qué es una red neuronal?", "best_answer": "Es un modelo computacional..."},
    {"id": "q2", "text": "¿Cómo funciona Kafka?", "best_answer": "Es un bus de mensajes..."},
    {"id": "q3", "text": "¿Qué es la Transformada de Fourier?", "best_answer": "Es una transformación matemática..."}
]

def produce():
    i = 0
    for i in range(100):
        q = random.choice(questions_pool)
        msg = {
            "id": f"{q['id']}_{int(time.time())}_{i}",
            "question": q["text"],
            "best_answer": q["best_answer"],
            "attempts": 0,
            "max_attempts": 5
        }
        producer.send("incoming", value=msg)
        producer.flush()
        print("Produced incoming request:", msg["id"])
        i += 1
        time.sleep(2)

if __name__ == "__main__":
    produce()
