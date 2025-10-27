import os, json, time
import psycopg2
from kafka import KafkaConsumer

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
PG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "dbname": os.getenv("POSTGRES_DB", "ds_db"),
    "user": os.getenv("POSTGRES_USER", "ds_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "ds_pass"),
}

consumer = KafkaConsumer("persist",
                         bootstrap_servers=KAFKA,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='storage-group')

def connect_db():
    conn = psycopg2.connect(host=PG["host"], dbname=PG["dbname"], user=PG["user"], password=PG["password"])
    conn.autocommit = True
    return conn

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS answers (
                id TEXT PRIMARY KEY,
                question TEXT,
                answer TEXT,
                score FLOAT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

def persist(msg):
    conn = connect_db()
    ensure_table(conn)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO answers (id, question, answer, score) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                    (msg["id"], msg["question"], msg["answer"], msg.get("score", None)))
    conn.close()
    print("Persisted:", msg["id"])

for message in consumer:
    m = message.value
    persist(m)
