from confluent_kafka import Consumer
from neo4j import GraphDatabase
import json

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "neo4j-posts",
    "auto.offset.reset": "latest"
})

consumer.subscribe(["posts"])

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "lapoland2025"))

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

    data = json.loads(msg.value().decode("utf-8"))
    print("Received post:", data)

    with driver.session() as session:
        session.execute_write(insert_post, data)
