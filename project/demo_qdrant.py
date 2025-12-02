from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, CreateCollection
from sentence_transformers import SentenceTransformer

client = QdrantClient(host="localhost", port=6333)

collection_name = "posts"

if client.collection_exists(collection_name):
    print(f"Collection '{collection_name}' already exists. Deleting it...")
    client.delete_collection(collection_name=collection_name)

client.create_collection(
    collection_name=collection_name,
    vectors_config=VectorParams(
        size=384,
        distance=Distance.COSINE
    )
)

print(f"Collection '{collection_name}' created successfully!")

model = SentenceTransformer("all-MiniLM-L6-v2")

posts = [
    "I love healthy food and cooking vegetarian meals.",
    "Fast food is the best! I eat burgers every day.",
]

vectors = model.encode(posts).tolist()

client.upsert(
    collection_name="posts",
    points=[
        {
            "id": i,
            "vector": vectors[i],
            "payload": {"text": posts[i]},
        }
        for i in range(len(posts))
    ]
)

print("Inserted sample posts!")

query = "I enjoy salads and vegetables"
query_vector = model.encode(query).tolist()

results = client.search( #trouver nouvelle méthode non déprécié
    collection_name="posts",
    query_vector=query_vector,
    limit=1
)

#print("Search result:")
#for r in results:
    #print(r.payload, "score:", r.score)

print("Mimi le dégradé bien dégradant")



