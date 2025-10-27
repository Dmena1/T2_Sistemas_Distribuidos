import os, json, time
from kafka import KafkaConsumer, KafkaProducer

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
consumer = KafkaConsumer("llm_responses.error",
                         bootstrap_servers=KAFKA,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='retry-worker-group')

producer = KafkaProducer(bootstrap_servers=KAFKA,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

MAX_ATTEMPTS = 5

def schedule_retry(msg):
    attempts = msg.get("attempts", 1)
    if attempts >= MAX_ATTEMPTS:
        # mover a dead_letter (no más reintentos)
        producer.send("dead_letter", value=msg)
        print("Moved to dead_letter:", msg["id"])
        producer.flush()
        return

    err = msg.get("error", "other")
    if err == "overload":
        # Exponential backoff -> publica a retries.overload con field delay_seconds
        delay = 2 ** attempts
        msg["attempts"] = attempts
        msg["delay"] = delay
        producer.send("retries.overload", value=msg)
        print(f"Scheduled overload retry in ~{delay}s for {msg['id']}")
    elif err == "quota":
        # Delay mayor y fix (ej. 60 * attempts)
        delay = 60 * attempts
        msg["attempts"] = attempts
        msg["delay"] = delay
        producer.send("retries.quota", value=msg)
        print(f"Scheduled quota retry in ~{delay}s for {msg['id']}")
    else:
        # reintento simple
        msg["attempts"] = attempts
        msg["delay"] = 10
        producer.send("retries.overload", value=msg)
        print(f"Scheduled generic retry for {msg['id']}")
    producer.flush()

# Estos tópicos de retry son leídos por otro consumidor (sleep simulado para delay)
for message in consumer:
    m = message.value
    schedule_retry(m)

import threading
from kafka import KafkaConsumer

def process_retries(topic):
    c = KafkaConsumer(topic,
                      bootstrap_servers=KAFKA,
                      value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                      auto_offset_reset='earliest',
                      enable_auto_commit=True,
                      group_id=f'retry-{topic}')
    for msg in c:
        m = msg.value
        delay = m.get("delay", 1)
        print(f"Sleeping {delay}s before retrying {m['id']} to llm_requests")
        time.sleep(delay)
        # republish to main questions or llm_requests
        producer.send("questions", value={"id": m["id"], "question": m["question"], "attempts": m.get("attempts",0)})
        producer.flush()
        print("Retried:", m["id"])

if __name__ == "__main__":
    # correr listeners para ambos tópicos
    t1 = threading.Thread(target=process_retries, args=("retries.overload",), daemon=True)
    t2 = threading.Thread(target=process_retries, args=("retries.quota",), daemon=True)
    t1.start(); t2.start()
    # mantener vivo
    while True:
        time.sleep(10)
