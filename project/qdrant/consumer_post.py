from confluent_kafka import Consumer
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance
from sentence_transformers import SentenceTransformer
import json
import uuid
import os
from dotenv import load_dotenv

load_dotenv()

############################################################
# 1. CONFIG
############################################################

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_POSTS")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_GROUP = os.getenv("KAFKA_QDRANT_GROUP")

QDRANT_HOST = os.getenv("QDRANT_HOST")
QDRANT_PORT = int(os.getenv("QDRANT_PORT"))
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION")

MODEL_NAME = os.getenv("SENTENCE_MODEL")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": KAFKA_GROUP,
    "auto.offset.reset": "earliest"
})

consumer.subscribe([KAFKA_TOPIC])

qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

model = SentenceTransformer(MODEL_NAME)

############################################################
# 2. INIT QDRANT COLLECTION
############################################################

if not qdrant.collection_exists(QDRANT_COLLECTION):
    qdrant.create_collection(
        collection_name=QDRANT_COLLECTION,
        vectors_config=VectorParams(
            size=384,
            distance=Distance.COSINE
        )
    )
    print(f"[QDRANT] Collection '{QDRANT_COLLECTION}' created")

############################################################
# 3. HELPERS
############################################################

def make_id(uri: str) -> str:
    """
    Stable UUID generated from URI
    """
    return str(uuid.uuid5(uuid.NAMESPACE_URL, uri))
############################################################
# 4. MAIN LOOP
############################################################

print("[QDRANT] Consumer started")

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print("Kafka error:", msg.error())
        continue

    data = json.loads(msg.value().decode("utf-8"))
    print("DATA :", data)
    text = data.get("text", "").strip()
    uri = data.get("cid") or data.get("post_cid")

    if not text :
        print("[SKIP] Missing text")
        continue

    if not uri :
        print("[SKIP] Missing uri")
        continue

    # Embedding
    vector = model.encode(text).tolist()

    # Upsert into Qdrant
    qdrant.upsert(
        collection_name=QDRANT_COLLECTION,
        points=[
            {
                "id": make_id(uri),
                "vector": vector,
                "payload": {
                    "uri": uri,
                    "text": text
                }
            }
        ]
    )

    print(f"[QDRANT] Indexed post: {uri}")
