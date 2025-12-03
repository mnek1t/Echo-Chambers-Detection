from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance
from sentence_transformers import SentenceTransformer

client = QdrantClient(host="localhost", port=6333)
collection_name = "posts"

if client.collection_exists(collection_name):
    client.delete_collection(collection_name)

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
    "Fresh salads are my favorite lunch.",
    "I could eat pizza forever, it’s too good.",
    "Green smoothies give me energy for the whole day.",
    "French fries are the ultimate comfort food.",
    "I’ve started meal-prepping with whole ingredients.",
    "Nothing beats a crispy chicken burger.",
    "I try to avoid sugar and processed foods.",
    "Donuts are my weakness, I can’t resist.",
    "Avocado toast is the perfect breakfast.",
    "I enjoy eating barbecue ribs on weekends.",
    "I’ve been cutting down on sodas lately.",
    "Ice cream is the best dessert ever invented.",
    "I’m trying to cook more plant-based meals.",
    "Tacos are my go-to cheat meal.",
    "I love experimenting with vegan recipes.",
    "Mozzarella sticks are the best snack.",
    "I buy organic vegetables whenever possible.",
    "I never say no to spicy fried chicken.",
    "I like light dinners, mostly soup and veggies.",
    "Loaded nachos always make my day better.",
    "I take vitamins and drink herbal tea daily.",
    "Hot dogs are underrated and delicious.",
    "I eat fruit bowls almost every morning.",
    "Chocolate milkshakes are my guilty pleasure.",
    "I’m learning to bake whole-grain bread.",
    "I love cheesy lasagnas, the more cheese the better.",
    "I replaced soda with sparkling water.",
    "Bacon makes everything taste better."
]

# --- Encode all posts ---
vectors = model.encode(posts).tolist()

# --- Insert into Qdrant ---
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

print("Inserted 30 sample posts!")