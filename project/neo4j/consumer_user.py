from confluent_kafka import Consumer
from neo4j import GraphDatabase
import json
import os
from dotenv import load_dotenv

load_dotenv()

############################################################
# 1. CONFIG
############################################################

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_USERS")
KAFKA_GROUP = os.getenv("KAFKA_GROUP_NEO4J_USERS")

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

############################################################
# 2. CLIENTS
############################################################

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': KAFKA_GROUP,
    'auto.offset.reset': 'latest'
})

consumer.subscribe([KAFKA_TOPIC])

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

############################################################
# 3. DB LOGIC
############################################################

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

def insert_liked(tx, data):
    tx.run("""
        MERGE (u:User {did: $user_did})
        MERGE (p:Post {uri: $uri})
        MERGE (u)-[:LIKED]->(p)
    """,
        user_did=data["user_did"],
        uri=data["uri"]
    )

print("[NEO4J USERS] Consumer started")
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue

    if msg.error():
        print("[KAFKA ERROR]", msg.error())
        continue
    # print(msg.value())
    data = json.loads(msg.value().decode("utf-8"))
    print("Received:", data)

    with driver.session() as session:
        if "type" in data and data["type"] == "LIKED":
            session.execute_write(insert_liked, data)
        else:
            session.execute_write(insert_user, data)
