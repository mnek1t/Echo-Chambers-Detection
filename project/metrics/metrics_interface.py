import pandas as pd
import numpy as np
import networkx as nx
from qdrant_client import QdrantClient
from neo4j import GraphDatabase
from collections import defaultdict
from metrics import ecs, embedding_variance, compute_modularity, homophily, compute_conductance

qdrant = QdrantClient(host="localhost", port=6333)
post_embeddings = {}
all_points = []
offset = 0
batch_size = 10000
while True:
    points, scroll_id = qdrant.scroll(
        collection_name="posts",
        with_vectors=True,
        with_payload=True,
        limit=batch_size,
        offset=offset
    )
    if scroll_id == None:
        break
    all_points.extend(points)
    offset = scroll_id
for p in all_points:
    post_embeddings[p.payload["uri"]] = np.array(p.vector)

neo4j = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "lapoland2025"))

def clean_csv(input_file: str):
    df = pd.read_csv(input_file)
    df = df[df["label"] != -1]
    return df

def get_user_embeddings():
    posts = {}
    with neo4j.session() as session:
        result = session.run("""
            MATCH (u:User)-[:LIKED|POSTED]->(p:Post)
            RETURN p.cid AS post_id, u.did AS user_id
            """
        )
        for record in result:
            posts[record["post_id"]] = record["user_id"]
    
    user_embeddings = defaultdict(list)
    for post_id, vector in post_embeddings.items():
        if post_id in posts:
            user_id = posts[post_id]
            user_embeddings[user_id].append(vector)

    user_embeddings = {u: np.mean(vectors, axis=0) for u, vectors in user_embeddings.items()}

    df_communities = clean_csv("hdbscan_clusters.csv")

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
print(list(G.nodes())[0], list(user_embeddings.items())[0], list(communities.items())[0])
ecs_value, cohesion, separation, inside_similarities, outside_similarities = ecs(G, user_embeddings, communities)
print("===")
print(f"ECS: {ecs_value:.4f}, Cohesion: {cohesion:.4f}, Separation: {separation:.4f}")
full_variances, filtered_variances = embedding_variance(user_embeddings, communities)
print("===")
print("Variances:")
print(filtered_variances)
modularity_value = compute_modularity(G, communities)
print(modularity_value)
homophily_value = homophily(G, user_embeddings)
print("===")
print("Homophily:", homophily_value)
conductance_scores, filtered_conductance_scores = compute_conductance(G, communities)
print("===")
print("Conductance scores:")
print(filtered_conductance_scores)
print("===")