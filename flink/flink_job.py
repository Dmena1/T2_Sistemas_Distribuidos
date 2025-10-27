from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
import json
import os

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
UMBRAL = float(os.getenv("SCORE_UMBRAL", "0.7"))

def score_function(question, answer):
    # Aquí pones la función de scoring de la Tarea1.
    # Ejemplo dummy: longitud relativa, etc.
    # Debes reemplazar por tu scoring real.
    q_words = len(question.split())
    a_words = len(answer.split())
    if a_words == 0:
        return 0.0
    score = min(1.0, a_words / (q_words + 1))
    return score

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    props = {
        'bootstrap.servers': KAFKA,
        'group.id': 'flink-scoring',
    }

    consumer = FlinkKafkaConsumer(
        topics='llm_responses.success',
        deserialization_schema=SimpleStringSchema(),
        properties=props
    )

    producer_persist = FlinkKafkaProducer(
        topic='persist',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': KAFKA}
    )

    producer_retry = FlinkKafkaProducer(
        topic='questions',  # reinyectar a questions o a llm_requests según diseño
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': KAFKA}
    )

    ds = env.add_source(consumer)

    def process_line(line):
        m = json.loads(line)
        score = score_function(m["question"], m["answer"])
        m["score"] = score
        if score >= UMBRAL:
            return ("persist", json.dumps(m))
        else:
            # incrementar attempts y decidir reintento
            m["attempts"] = m.get("attempts", 0) + 1
            return ("retry", json.dumps(m))

    mapped = ds.map(process_line)

    def filter_persist(x):
        if x[0] == "persist":
            return True
        return False

    def filter_retry(x):
        return x[0] == "retry"

    mapped.filter(filter_persist).map(lambda x: x[1]).add_sink(producer_persist)
    mapped.filter(filter_retry).map(lambda x: x[1]).add_sink(producer_retry)

    env.execute("scoring-and-feedback-loop")

if __name__ == "__main__":
    main()
