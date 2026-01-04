import os
import pandas as pd
from sqlalchemy import create_engine, text

def get_engine():
    return create_engine(
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER')}:"
        f"{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:"
        f"{os.getenv('POSTGRES_PORT')}/"
        f"{os.getenv('POSTGRES_DB')}",
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )

def get_algorithm_id(engine, name: str):
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT id FROM algorithm WHERE name = :name"),
            {"name": name}
        ).scalar_one()


def insert_clustering_run_record(engine, algorithm_id: str, description: str):
    with engine.begin() as conn:
        return conn.execute(
            text("""
                INSERT INTO clustering_run (algorithm_id, description)
                VALUES (:algorithm_id, :description)
                RETURNING id
                """),
            {"algorithm_id": algorithm_id, "description": description}
        ).scalar()


def expire_community_membership(engine, neo4j_ids):
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE community_membership 
                SET valid_to = NOW()
                WHERE neo4j_id = ANY(:neo4j_ids) AND valid_to IS NULL
                """),
            {'neo4j_ids': neo4j_ids}
        )


def get_communities_from_postgres(engine, run_id: str):
    query = text("""
        SELECT cm.neo4j_id, c.label, c.id
        FROM community_membership cm
        JOIN community c ON cm.community_id = c.id
        WHERE c.run_id = :run_id
    """)
    df = pd.read_sql(query, engine, params={"run_id": run_id})
    return df