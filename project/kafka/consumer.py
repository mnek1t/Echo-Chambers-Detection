from confluent_kafka import Consumer
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'embedding-service',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['new_post'])

model = SentenceTransformer("all-MiniLM-L6-v2")
qdrant = QdrantClient(host="localhost", port=6333)

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    text = msg.value().decode("utf-8")
    print("Received:", text)

    # Generate embedding
    vector = model.encode(text).tolist()

    results = qdrant.query_points(
        collection_name="posts",
        query=vector,
        limit=3,
        with_payload=True  # en général c'est True par défaut, mais on explicite
    )

    print("Recommended content:")
    for point in results.points:
        # chaque point est un ScoredPoint avec un attribut .payload (dict)
        print("-", point.payload["text"])
