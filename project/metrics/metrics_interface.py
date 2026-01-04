import os
import pandas as pd
import numpy as np
import networkx as nx
from qdrant_client import QdrantClient
from neo4j import GraphDatabase
from collections import defaultdict
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.postgres import get_engine, get_communities_from_postgres
from metrics import ecs, embedding_variance, compute_modularity, homophily, compute_conductance, per_community_table
from visualization import display_conductance, display_ecs, display_variance

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


def get_user_embeddings(postgres_engine, run_id):
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
    df_communities = get_communities_from_postgres(postgres_engine, run_id)
    with neo4j.session() as session:
        result = session.run("""
            MATCH (u:User)
            RETURN elementId(u) AS neo4jId, u.did AS did
            """
        )
        id_map = {record["neo4jId"]: record["did"] for record in result}
    
    df_communities["did"] = df_communities["neo4j_id"].apply(lambda x: id_map[x])
    communities = {
        row.did: {
            "label": row.label,
            "community_id": row.id
        }
        for row in df_communities.itertuples(index=False)
    }
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


def build_comm_label_to_id_lookup(communities):
    return {
        comm["label"]: str(comm["community_id"])
        for comm in communities.values()
    }


def run_metrics(algorithm_runs: dict):
    postgres_engine = get_engine()
    for name, run_id in algorithm_runs.items():
        G, user_embeddings, communities = get_user_embeddings(postgres_engine, run_id)
        label_to_community_id = build_comm_label_to_id_lookup(communities)
        print(label_to_community_id)

        print('CALCULATING METRICS GLOBALLY')
        ecs_value, cohesion, separation = ecs(G, user_embeddings, communities)
        print(f"ECS: {ecs_value:.4f}, Cohesion: {cohesion:.4f}, Separation: {separation:.4f}")
        homophily_value = homophily(G, user_embeddings)
        print(f"Homophilly: {homophily_value}")
        modularity_score = compute_modularity(G, communities)
        print(f"Modularity: {modularity_score}")

        print('CALCULATING METRICS PER COMMUNITY')
        df_comm = per_community_table(G, user_embeddings, communities)

        display_conductance(df_comm)
        display_ecs(df_comm)
        display_variance(df_comm)

        df_comm["community_id"] = df_comm["community"].map(label_to_community_id)

        df_comm = df_comm.drop(columns=['community', 'size'])

        df_comm.to_sql("community_metrics", postgres_engine, if_exists="append", index=False)

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