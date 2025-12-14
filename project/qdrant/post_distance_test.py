from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

# Connexion
qdrant = QdrantClient(host="localhost", port=6333)
model = SentenceTransformer("all-MiniLM-L6-v2")

# Fake post / query
query_text = "Police brutality and abuse of power must be documented and exposed"

# Embedding
query_vector = model.encode(query_text).tolist()

# Recherche
results = qdrant.search(
    collection_name="posts",
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