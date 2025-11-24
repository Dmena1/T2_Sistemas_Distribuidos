"""Extracción de datos desde PostgreSQL para análisis batch."""

import os
import psycopg2
import sys

PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "dbname": os.getenv("POSTGRES_DB", "ds_db"),
    "user": os.getenv("POSTGRES_USER", "ds_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "ds_pass"),
}

OUTPUT_DIR = "/data"

def connect_db():
    """Conecta a PostgreSQL."""
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        print(f"Conectado a PostgreSQL: {PG_CONFIG['dbname']}")
        return conn
    except Exception as e:
        print(f"Error conectando a PostgreSQL: {e}")
        sys.exit(1)

def ensure_source_column(conn):
    """Asegura que existe la columna 'source' en la tabla answers."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='answers' AND column_name='source';
            """)
            if not cur.fetchone():
                print("Columna 'source' no existe. Agregándola...")
                cur.execute("ALTER TABLE answers ADD COLUMN source VARCHAR(10);")
                conn.commit()
                print("Columna 'source' agregada")
            else:
                print("Columna 'source' ya existe")
    except Exception as e:
        print(f"Error verificando columna source: {e}")

def get_record_count(conn):
    """Obtiene el conteo de registros por fuente."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM answers;")
        total = cur.fetchone()[0]
        
        cur.execute("SELECT source, COUNT(*) FROM answers WHERE source IS NOT NULL GROUP BY source;")
        by_source = cur.fetchall()
        
        print(f"\nEstadísticas de la base de datos:")
        print(f"Total de registros: {total}")
        for source, count in by_source:
            print(f"   - {source}: {count}")
        
        return total, dict(by_source)

def extract_answers(conn, source_type, output_file):
    """Extrae respuestas de un tipo específico y las guarda en un archivo."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT answer FROM answers WHERE source = %s AND answer IS NOT NULL;",
                (source_type,)
            )
            
            count = 0
            with open(output_file, 'w', encoding='utf-8') as f:
                for row in cur:
                    answer = row[0].strip().replace('\n', ' ').replace('\r', ' ')
                    if answer:
                        f.write(answer + '\n')
                        count += 1
            
            print(f"Extraídas {count} respuestas de '{source_type}' -> {output_file}")
            return count
    except Exception as e:
        print(f"Error extrayendo respuestas de '{source_type}': {e}")
        return 0

def main():
    print("=" * 60)
    print("EXTRACCIÓN DE DATOS PARA ANÁLISIS BATCH")
    print("=" * 60)
    
    conn = connect_db()
    ensure_source_column(conn)
    total, by_source = get_record_count(conn)
    
    if total == 0:
        print("\nNo hay datos en la base de datos.")
        print("Ejecuta el sistema principal primero para generar datos.")
        conn.close()
        sys.exit(0)
    
    if not by_source:
        print("\nNo hay registros con 'source' definido.")
        print("Necesitas actualizar los registros existentes con su fuente.")
        print("Puedes ejecutar el script 'populate_test_data.py' para generar datos de prueba.")
        conn.close()
        sys.exit(0)
    
    yahoo_file = os.path.join(OUTPUT_DIR, "yahoo_answers.txt")
    yahoo_count = extract_answers(conn, "yahoo", yahoo_file)
    
    llm_file = os.path.join(OUTPUT_DIR, "llm_answers.txt")
    llm_count = extract_answers(conn, "llm", llm_file)
    
    conn.close()
    
    print("\n" + "=" * 60)
    print("EXTRACCIÓN COMPLETADA")
    print("=" * 60)
    print(f"Yahoo! Answers: {yahoo_count} registros")
    print(f"LLM Answers: {llm_count} registros")
    print(f"\nArchivos generados en: {OUTPUT_DIR}/")
    
    if yahoo_count == 0 or llm_count == 0:
        print("\nADVERTENCIA: Uno de los conjuntos está vacío.")
        print("El análisis comparativo requiere datos de ambas fuentes.")

if __name__ == "__main__":
    main()
