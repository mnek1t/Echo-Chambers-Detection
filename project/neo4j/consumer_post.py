from confluent_kafka import Consumer
from neo4j import GraphDatabase
import json
import os

############################################################
# 1. CONFIG
############################################################

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_POSTS")
KAFKA_GROUP = os.getenv("KAFKA_GROUP_NEO4J_POSTS")

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": KAFKA_GROUP,
    "auto.offset.reset": "latest"
})

consumer.subscribe([KAFKA_TOPIC])

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

############################################################
# 3. DB LOGIC
############################################################

def insert_post(tx, data):
    print(data)
    tx.run("""
        MERGE (p:Post {uri: $uri})
        SET p.text = $text,
            p.cid  = $cid
        WITH p
        MERGE (u:User {did: $author})
        MERGE (u)-[:POSTED]->(p)
    """, **data)

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue

    if msg.error():
        print("[KAFKA ERROR]", msg.error())
        continue

    data = json.loads(msg.value().decode("utf-8"))
    print("Received post:", data)

    with driver.session() as session:
        session.execute_write(insert_post, data)
