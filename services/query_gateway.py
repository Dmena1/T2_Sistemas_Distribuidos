import os, json
import psycopg2
from kafka import KafkaConsumer, KafkaProducer

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
PG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "dbname": os.getenv("POSTGRES_DB", "ds_db"),
    "user": os.getenv("POSTGRES_USER", "ds_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "ds_pass"),
}

consumer = KafkaConsumer(
    "incoming",
    bootstrap_servers=KAFKA,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='query-gateway-group'
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def connect_db():
    conn = psycopg2.connect(host=PG["host"], dbname=PG["dbname"], user=PG["user"], password=PG["password"])
    conn.autocommit = True
    return conn

def find_cached_answer(conn, question_id):
    with conn.cursor() as cur:
        cur.execute("SELECT id, question, answer, score FROM answers WHERE id = %s", (question_id,))
        row = cur.fetchone()
        if row:
            return {
                "id": row[0],
                "question": row[1],
                "answer": row[2],
                "score": float(row[3]) if row[3] is not None else None,
                "served_from": "cache"
            }
    return None

def handle_message(msg):
    qid = msg.get("id")
    if not qid:
        producer.send("questions", value=msg)
        return

    conn = None
    try:
        conn = connect_db()
        cached = find_cached_answer(conn, qid)
        if cached and cached.get("score") is not None:
            out = {
                "id": cached["id"],
                "question": cached["question"],
                "answer": cached["answer"],
                "score": cached["score"],
                "meta": {"served_from": "cache"}
            }
            producer.send("persist", value=out)
            print("Gateway: served from cache:", qid, flush=True)
        else:
            producer.send("questions", value=msg)
            print("Gateway: forwarded to questions:", qid, flush=True)
    finally:
        if conn:
            conn.close()
    producer.flush()

for message in consumer:
    handle_message(message.value)


