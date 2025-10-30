import os, json, time, random
from kafka import KafkaConsumer, KafkaProducer

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
consumer = KafkaConsumer("questions",
                         bootstrap_servers=KAFKA,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='llm-worker-group')

producer = KafkaProducer(bootstrap_servers=KAFKA,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def call_llm_simulation(question_text):
    r = random.random()
    if r < 0.7:
        return {"answer": f"Respuesta sintetica a: {question_text}"}
    elif r < 0.85:
        raise Exception("OVERLOAD")
    else:
        raise Exception("QUOTA_EXCEEDED")

for msg in consumer:
    m = msg.value
    try:
        print("LLM worker processing:", m["id"], flush=True)
        res = call_llm_simulation(m["question"])
        out = {
            "id": m["id"],
            "question": m["question"],
            "best_answer": m["best_answer"],
            "answer": res["answer"],
            "attempts": m.get("attempts", 0),
            "meta": {}
        }
        producer.send("llm_responses.success", value=out)
        print("Published success for", m["id"], flush=True)
    except Exception as e:
        err_type = "other"
        if "OVERLOAD" in str(e):
            err_type = "overload"
        if "QUOTA" in str(e) or "QUOTA_EXCEEDED" in str(e):
            err_type = "quota"
        err_msg = {
            "id": m["id"],
            "question": m["question"],
            "attempts": m.get("attempts", 0) + 1,
            "error": err_type,
            "raw": str(e)
        }
        producer.send("llm_responses.error", value=err_msg)
        print("Published error for", m["id"], "type:", err_type)
    producer.flush()
