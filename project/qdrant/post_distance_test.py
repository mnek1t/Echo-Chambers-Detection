import os
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

# Connexion
QDRANT_HOST = os.getenv("QDRANT_HOST")
QDRANT_PORT = int(os.getenv("QDRANT_PORT"))
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION")
MODEL_NAME = os.getenv("SENTENCE_MODEL")

qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
model = SentenceTransformer(MODEL_NAME)

# Fake post / query
query_text = "Police brutality and abuse of power must be documented and exposed"

# Embedding
query_vector = model.encode(query_text).tolist()

# Recherche
results = qdrant.search(
    collection_name=QDRANT_COLLECTION,
    query_vector=query_vector,
    limit=1000,
    with_payload=True
)

sorted_results = sorted(results, key=lambda r: r.score)
farthest = sorted_results[:5]
closest = sorted_results[-5:]

print("CLOSEST")
for r in closest:
    print(f"- score={r.score:.4f}")
    print(f"  {r.payload['text']}\n")

print("FARTHEST")
for r in farthest:
    print(f"- score={r.score:.4f}")
    print(f"  {r.payload['text']}\n")