from graphdatascience import GraphDataScience

gds = GraphDataScience("bolt://localhost:7687", auth=("neo4j", "lapoland2025"))

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
    df["neo4jId"] = df["nodeId"].apply(lambda x: gds.util.asNode(x).element_id)
    df = df.drop(columns=["nodeId"])

    df.to_csv(f"{algorithm_name}_clusters.csv", index=False)

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

    for name, func in algorithms.items():
        df = func(G)
        save_communities(df, name)

run_community_detection()