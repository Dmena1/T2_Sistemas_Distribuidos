"""Generador de datos de prueba para análisis batch."""

import os
import psycopg2
import random
import uuid
from datetime import datetime, timedelta

PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "ds_db"),
    "user": os.getenv("POSTGRES_USER", "ds_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "ds_pass"),
}

YAHOO_RESPONSES = [
    "creo q es mejor usar python xq es mas facil",
    "no se mucho del tema pero yo usaria java",
    "jajaja esa pregunta es re facil, googlealo",
    "depende de lo que quieras hacer amigo",
    "yo lo hice asi y me funciono perfecto",
    "ni idea bro, suerte con eso",
    "busca en youtube hay muchos tutoriales",
    "preguntale a tu profe mejor",
    "eso es re complicado, necesitas estudiar mas",
    "la verdad no tengo idea de eso",
]

LLM_RESPONSES = [
    "Basado en tu pregunta, te recomendaría utilizar Python debido a su sintaxis clara y extensa documentación.",
    "Para resolver este problema, es importante considerar varios factores como el rendimiento y la escalabilidad del sistema.",
    "La solución más apropiada dependerá de tus requisitos específicos. Te sugiero evaluar las siguientes opciones.",
    "De acuerdo con las mejores prácticas de la industria, deberías implementar un patrón de diseño robusto.",
    "Es fundamental comprender los conceptos subyacentes antes de proceder con la implementación.",
    "Te recomiendo consultar la documentación oficial para obtener información más detallada sobre este tema.",
    "Existen múltiples enfoques para abordar esta situación, cada uno con sus ventajas y desventajas.",
    "Según mi análisis, la mejor estrategia sería implementar una arquitectura modular y escalable.",
    "Es importante tener en cuenta las implicaciones de seguridad al implementar esta funcionalidad.",
    "Para optimizar el rendimiento, considera utilizar técnicas de caching y procesamiento asíncrono.",
]

QUESTIONS = [
    "¿Cuál es el mejor lenguaje de programación para aprender?",
    "¿Cómo puedo mejorar mi código?",
    "¿Qué framework debería usar?",
    "¿Cómo funciona la programación orientada a objetos?",
    "¿Cuál es la diferencia entre SQL y NoSQL?",
    "¿Cómo puedo optimizar mi base de datos?",
    "¿Qué es un sistema distribuido?",
    "¿Cómo implementar autenticación en mi aplicación?",
    "¿Cuál es la mejor práctica para manejar errores?",
    "¿Cómo puedo aprender desarrollo web?",
]

def connect_db():
    """Conecta a PostgreSQL"""
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        conn.autocommit = True
        print(f"Conectado a PostgreSQL: {PG_CONFIG['dbname']}")
        return conn
    except Exception as e:
        print(f"Error conectando a PostgreSQL: {e}")
        print(f"   Asegúrate de que PostgreSQL esté corriendo en {PG_CONFIG['host']}:{PG_CONFIG['port']}")
        return None

def ensure_table_and_column(conn):
    """Asegura que la tabla y columna source existan"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS answers (
                id TEXT PRIMARY KEY,
                question TEXT,
                answer TEXT,
                score REAL,
                created_at TIMESTAMP DEFAULT NOW(),
                source VARCHAR(10)
            );
        """)
        
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='answers' AND column_name='source';
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE answers ADD COLUMN source VARCHAR(10);")
            print("Columna 'source' agregada")

def generate_test_data(conn, num_yahoo=100, num_llm=100):
    """Genera datos de prueba"""
    print(f"\nGenerando datos de prueba...")
    print(f"- {num_yahoo} respuestas de Yahoo!")
    print(f"- {num_llm} respuestas de LLM")
    
    with conn.cursor() as cur:
        # Generar respuestas de Yahoo!
        for i in range(num_yahoo):
            record_id = f"yahoo_{uuid.uuid4().hex[:8]}"
            question = random.choice(QUESTIONS)
            answer = random.choice(YAHOO_RESPONSES)
            score = random.uniform(0.3, 0.7)
            created_at = datetime.now() - timedelta(days=random.randint(0, 30))
            
            cur.execute("""
                INSERT INTO answers (id, question, answer, score, created_at, source)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (record_id, question, answer, score, created_at, "yahoo"))
        
        for i in range(num_llm):
            record_id = f"llm_{uuid.uuid4().hex[:8]}"
            question = random.choice(QUESTIONS)
            answer = random.choice(LLM_RESPONSES)
            score = random.uniform(0.6, 0.95)
            created_at = datetime.now() - timedelta(days=random.randint(0, 30))
            
            cur.execute("""
                INSERT INTO answers (id, question, answer, score, created_at, source)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (record_id, question, answer, score, created_at, "llm"))
    
    print("Datos de prueba generados exitosamente")

def show_stats(conn):
    """Muestra estadísticas de la base de datos"""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM answers;")
        total = cur.fetchone()[0]
        
        cur.execute("SELECT source, COUNT(*) FROM answers WHERE source IS NOT NULL GROUP BY source;")
        by_source = cur.fetchall()
        
        print(f"\nEstadísticas de la base de datos:")
        print(f"Total de registros: {total}")
        for source, count in by_source:
            print(f"   - {source}: {count}")

def main():
    print("=" * 60)
    print("GENERACIÓN DE DATOS DE PRUEBA")
    print("=" * 60)
    
    conn = connect_db()
    if not conn:
        return
    
    ensure_table_and_column(conn)
    generate_test_data(conn, num_yahoo=150, num_llm=150)
    show_stats(conn)
    
    conn.close()
    
    print("\nProceso completado")
    print("\nAhora puedes ejecutar el data_extractor.py para extraer los datos.")

if __name__ == "__main__":
    main()
