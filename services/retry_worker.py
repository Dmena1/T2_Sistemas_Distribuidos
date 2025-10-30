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

MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "5"))

def schedule_retry(msg):
    attempts = msg.get("attempts", 1)
    if attempts >= MAX_ATTEMPTS:
        producer.send("dead_letter", value=msg)
        print("Moved to dead_letter:", msg["id"])
        producer.flush()
        return

    err = msg.get("error", "other")
    if err == "overload":
        delay = 2 ** attempts
        msg["attempts"] = attempts
        msg["delay"] = delay
        producer.send("retries.overload", value=msg)
        print(f"Scheduled overload retry in ~{delay}s for {msg['id']}")
    elif err == "quota":
        delay = 60 * attempts
        msg["attempts"] = attempts
        msg["delay"] = delay
        producer.send("retries.quota", value=msg)
        print(f"Scheduled quota retry in ~{delay}s for {msg['id']}")
    else:
        msg["attempts"] = attempts
        msg["delay"] = 10
        producer.send("retries.overload", value=msg)
        print(f"Scheduled generic retry for {msg['id']}")
    producer.flush()

def consume_errors():
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
        print(f"Sleeping {delay}s before retrying {m['id']} to questions")
        time.sleep(delay)
        producer.send("questions", value={"id": m["id"], "question": m["question"], "attempts": m.get("attempts",0)})
        producer.flush()
        print("Retried:", m["id"], flush=True)

if __name__ == "__main__":
    t_errors = threading.Thread(target=consume_errors, daemon=True)
    t_overload = threading.Thread(target=process_retries, args=("retries.overload",), daemon=True)
    t_quota = threading.Thread(target=process_retries, args=("retries.quota",), daemon=True)

    t_errors.start()
    t_overload.start()
    t_quota.start()

    while True:
        time.sleep(10)
