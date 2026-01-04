import time
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from db.postgres import get_engine
############################################################
# 1. CONFIG
############################################################

ALGORITHMS = [
    "hdbscan",
    "kcore",
    "label_propagation",
    "leiden",
    "louvain",
    "modularity_optimization",
]

BASE_DIR = os.path.dirname(__file__)
SCHEMA_PATH = os.path.join(BASE_DIR, "postgres-schema.sql")

def load_schema():
    with open(SCHEMA_PATH, "r") as f:
        return f.read()

############################################################
# 1. POSTGRES ENGINE & INIT
############################################################

engine = get_engine()

def wait_for_db():
    for i in range(5):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                print("[Postgres] Database ready")
                return
        except OperationalError:
            print("[Postgres] Waiting for database...")
            time.sleep(2)
    raise RuntimeError("Postgres not ready")

def init_db():
    schema_sql = load_schema()
    with engine.begin() as conn:
        conn.execute(text(schema_sql))

        for name in ALGORITHMS:
            conn.execute(
                text("""
                    INSERT INTO algorithm (name)
                    VALUES (:name)
                    ON CONFLICT (name) DO NOTHING
                """),
                {"name": name}
            )

    print("[Postgres] Schema verified & algorithms seeded")

if __name__ == "__main__":
    wait_for_db()
    init_db()