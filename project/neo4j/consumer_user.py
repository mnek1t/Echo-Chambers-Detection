from confluent_kafka import Consumer
from neo4j import GraphDatabase
import json

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'neo4j-consumer',
    'auto.offset.reset': 'latest'
})

consumer.subscribe(["user"])

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "lapoland2025"))

def clean(value):
    """Convert None or '' into a safe type for Neo4j."""
    return value if value not in [None, ""] else "unknown"

def insert_user(tx, data):
    tx.run("""
        MERGE (u:User {did: $did})
        SET u.handle       = $handle,
            u.display_name = $display_name

    """,
    did=clean(data.get("did")),
    handle=clean(data.get("handle")),
    display_name=clean(data.get("display_name")),
)

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    # print(msg.value())
    data = json.loads(msg.value().decode("utf-8"))
    print("Received:", data)

    with driver.session() as session:
        session.execute_write(insert_user, data)
