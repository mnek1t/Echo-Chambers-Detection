import os
import uuid
from datetime import datetime
from graphdatascience import GraphDataScience
import pandas as pd
from collections import defaultdict
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from metrics_interface import run_metrics
from db.postgres import get_engine, get_algorithm_id, insert_clustering_run_record, expire_community_membership

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
gds = GraphDataScience(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
engine = get_engine()

def produce_graph_projection():
    if gds.graph.exists("userGraph")["exists"]:
        gds.graph.drop("userGraph")

    G, result = gds.graph.cypher.project(
        """
        MATCH (u1:User)-[:LIKED|POSTED]->(p:Post)<-[:LIKED|POSTED]-(u2:User)
        WHERE id(u1) < id(u2)
        WITH u1, u2, count(p) AS weight
        RETURN gds.graph.project(
            $graph_name,
            u1,
            u2,
            {sourceNodeLabels: $label, targetNodeLabels: $label, relationshipProperties: {weight:weight}, relationshipType: $rel_type},
            {undirectedRelationshipTypes: ['*']}
        )
        """,
        graph_name="userGraph",
        label="User",
        rel_type="weight"
    )

    gds.fastRP.mutate(
        G,
        embeddingDimension=128,
        relationshipWeightProperty="weight",
        mutateProperty="embedding"
    )

    return G

def run_hdbscan(G):
    df = gds.hdbscan.stream(
        G,
        nodeProperty="embedding"
    )
    return df

def run_kcore(G):
    df = gds.kcore.stream(G)
    return df

def run_label_propagation(G):
    df = gds.labelPropagation.stream(G)
    return df

def run_leiden(G):
    df = gds.leiden.stream(
        G,
        relationshipWeightProperty="weight"
    )
    return df

def run_louvain(G):
    df = gds.louvain.stream(
        G,
        relationshipWeightProperty="weight"
    )
    return df

def run_modularity_optimization(G):
    df = gds.modularityOptimization.stream(
        G,
        relationshipWeightProperty="weight"
    )
    return df

def save_communities(df, algorithm_name): 
    algorithm_id = get_algorithm_id(engine, algorithm_name)
    date_str = datetime.now().strftime("%b %d")
    description = f"{date_str} : {algorithm_name}"
    run_id = str(insert_clustering_run_record(engine, algorithm_id, description))
    result_df = pd.DataFrame()
    result_df["neo4j_id"]= df["nodeId"].apply(lambda x: gds.util.asNode(x).element_id)
    
    if "coreValue" in df.columns:
        result_df["label"] = df["coreValue"]
    elif "communityId" in df.columns:
        result_df["label"] = df["communityId"]
    else:
        result_df["label"] = df["label"]

    result_df = result_df[result_df["label"] != -1]

    communities_df = (
        result_df[["label"]]
        .drop_duplicates()
        .copy()
    )

    communities_df["id"] = [str(uuid.uuid4()) for _ in range(len(communities_df))]
    communities_df["run_id"] = run_id
    communities_df.to_sql("community", engine, if_exists="append", index=False)
    
    expire_community_membership(engine, result_df["neo4j_id"].tolist())

    community_memberships_df = (
        result_df
        .merge(communities_df[["id", "label"]], on="label")
        [["id", "neo4j_id"]]
        .rename(columns={"id": "community_id"})
    )
    community_memberships_df.to_sql("community_membership", engine, if_exists="append", index=False)

    return run_id

def run_community_detection():
    G = produce_graph_projection()

    algorithms = {
        "hdbscan": run_hdbscan,
        "kcore": run_kcore,
        "label_propagation": run_label_propagation,
        "leiden": run_leiden,
        "louvain": run_louvain,
        "modularity_optimization": run_modularity_optimization
    }

    algorithm_runs = {}
    for name, func in algorithms.items():
        df = func(G)
        algorithm_runs[name] = save_communities(df, name)

    run_metrics(algorithm_runs)

run_community_detection()