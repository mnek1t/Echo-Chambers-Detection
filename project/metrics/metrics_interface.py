import os
import pandas as pd
import numpy as np
import networkx as nx
from qdrant_client import QdrantClient
from neo4j import GraphDatabase
from collections import defaultdict
from metrics import ecs, embedding_variance, compute_modularity, homophily, compute_conductance, per_community_table

QDRANT_HOST = os.getenv("QDRANT_HOST")
QDRANT_PORT = os.getenv("QDRANT_PORT")
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
post_embeddings = {}
all_points = []
offset = None
batch_size = 10000

while True:
    points, next_page_offset = qdrant.scroll(
        collection_name="posts",
        with_vectors=True,
        with_payload=True,
        limit=batch_size,
        offset=offset
    )
    all_points.extend(points)

    if next_page_offset == None:
        break

    offset = next_page_offset

for p in all_points:
    post_embeddings[p.payload["uri"]] = np.array(p.vector)

neo4j = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# communities with label -1 mean noise points and Community were not assigned to them
def clean_csv(input_file: str):
    df = pd.read_csv(input_file)
    df = df[df["label"] != -1]
    return df

# each user embedding should normalized to avoid bias towards more active users
def l2_normalize(v: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(v)
    return v / n if n > 0 else v


def get_user_embeddings():
    posts = defaultdict(list)
    # find users who liked or posted a post
    with neo4j.session() as session:
        result = session.run("""
            MATCH (u:User)-[:LIKED|POSTED]->(p:Post)
            RETURN p.cid AS post_id, u.did AS user_id
            """
        )
        for record in result:
            posts[record["post_id"]].append(record["user_id"])
    
    user_embeddings = defaultdict(list) # for storing each post embedding vector per user 
    for post_id, vector in post_embeddings.items():
        if post_id in posts:
            for user_id in posts[post_id]:
                user_embeddings[user_id].append(l2_normalize(vector))


    user_embeddings = {u: l2_normalize(np.mean(vectors, axis=0)) for u, vectors in user_embeddings.items()} # collapse all post embeddings into one average

    df_communities = clean_csv("../hdbscan_clusters.csv")

    with neo4j.session() as session:
        result = session.run("""
            MATCH (u:User)
            RETURN elementId(u) AS neo4jId, u.did AS did
            """
        )
        id_map = {record["neo4jId"]: record["did"] for record in result}
    
    df_communities["did"] = df_communities["neo4jId"].apply(lambda x: id_map[x])
    communities = dict(zip(df_communities["did"], df_communities["label"]))
    communities = {did: com for did, com in communities.items() if did in user_embeddings}
    user_embeddings = {did: emb for did, emb in user_embeddings.items() if did in communities}

    G = nx.Graph()
    with neo4j.session() as session:
        result = session.run("""
            MATCH (u1:User)-[:LIKED|POSTED]->(p:Post)<-[:LIKED|POSTED]-(u2:User)
            RETURN u1.did AS u1, u2.did AS u2
            """
        )
        for record in result:
            u1, u2 = record["u1"], record["u2"]
            if u1 in user_embeddings and u2 in user_embeddings:
                G.add_edge(u1, u2)

    return G, user_embeddings, communities

#TODO: put these in one function
G, user_embeddings, communities = get_user_embeddings()
print('CALCULATING METRICS GLOBALLY')
ecs_value, cohesion, separation = ecs(G, user_embeddings, communities)
print(f"ECS: {ecs_value:.4f}, Cohesion: {cohesion:.4f}, Separation: {separation:.4f}")
homophily_value = homophily(G, user_embeddings)
print(f"Homophilly: {homophily_value}")
modularity_score = compute_modularity(G, communities)
print(f"Modularity: {modularity_score}")

print('CALCULATING METRICS PER COMMUNITY')
df_comm = per_community_table(G, user_embeddings, communities)

# Save the table for downstream analysis
df_comm.to_csv("per_community_metrics.csv", index=False)

import seaborn as sns
import matplotlib
matplotlib.use("Agg")  # headless plotting inside Docker
import matplotlib.pyplot as plt

# ECS per community
plt.figure(figsize=(10, 6))
sns.barplot(data=df_comm.sort_values("ecs", ascending=False), x="community", y="ecs", color="#4c78a8")
plt.xticks(rotation=90)
plt.ylabel("ECS (cohesion × separation)")
plt.title("Echo Chamber Score by Community")
plt.tight_layout()
plt.savefig("ecs_by_community.png")
plt.close()

# Conductance per community (lower = more insulated)
plt.figure(figsize=(10, 6))
sns.barplot(data=df_comm.sort_values("conductance", ascending=True), x="community", y="conductance", color="#f58518")
plt.xticks(rotation=90)
plt.ylabel("Conductance (lower = more insulated)")
plt.title("Conductance by Community")
plt.tight_layout()
plt.savefig("conductance_by_community.png")
plt.close()

# Variance per community (lower = more homogeneous)
plt.figure(figsize=(10, 6))
sns.barplot(data=df_comm.sort_values("variance", ascending=True), x="community", y="variance", color="#54a24b")
plt.xticks(rotation=90)
plt.ylabel("Mean squared distance to centroid")
plt.title("Embedding Variance by Community")
plt.tight_layout()
plt.savefig("variance_by_community.png")
plt.close()

def print_metrics(csv_name):
    G, user_embeddings, communities = get_user_embeddings(csv_name)

    ecs_value, cohesion, separation, inside_similarities, outside_similarities = ecs(G, user_embeddings, communities)
    print("===")
    print(f"ECS: {ecs_value:.4f}, Cohesion: {cohesion:.4f}, Separation: {separation:.4f}")

    full_variances, filtered_variances = embedding_variance(user_embeddings, communities)
    print("===")
    print("Variances:")
    print(len(filtered_variances))

    modularity_value = compute_modularity(G, communities)
    print("===")
    print("Modularity:", modularity_value)

    homophily_value = homophily(G, user_embeddings)
    print("===")
    print("Homophily:", homophily_value)

    conductance_scores, filtered_conductance_scores = compute_conductance(G, communities)
    print("===")
    print("Conductance scores:")
    print(len(filtered_conductance_scores))
    print("===")

for csv_file in ["hdbscan_clusters.csv", "kcore_clusters.csv", "label_propagation_clusters.csv", "leiden_clusters.csv", "louvain_clusters.csv", "modularity_optimization_clusters.csv"]:
    print(f"Metrics for {csv_file}:")
    print_metrics(csv_file)
    print("\n\n")
