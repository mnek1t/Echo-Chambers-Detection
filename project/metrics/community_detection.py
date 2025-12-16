from graphdatascience import GraphDataScience

gds = GraphDataScience("bolt://localhost:7687", auth=("neo4j", "lapoland2025"))

if gds.graph.exists("userGraph")["exists"]:
    gds.graph.drop("userGraph")

G, result = gds.graph.project.cypher(
    "userGraph",
    "MATCH (u:User) RETURN id(u) AS id",
    """
    MATCH (u1:User)-[:LIKED|POSTED]->(p:Post)<-[:LIKED|POSTED]-(u2:User)
    WHERE elementId(u1) < elementId(u2)
    RETURN elementId(u1) AS source, elementId(u2) AS target, count(p) AS weight
    """
)

print("Graph projected:", G)

gds.fastRP.mutate(
    G,
    embeddingDimension=128,
    relationshipWeightProperty="weight",
    mutateProperty="embedding"
)

df = gds.hdbscan.stream(
    G,
    nodeProperty="embedding",
    minClusterSize=10
)

print(df)

df["neo4jId"] = df["nodeId"].apply(lambda x: gds.util.asNode(x).element_id)
df = df.drop(columns=["nodeId"])

df.to_csv("hdbscan_clusters.csv", index=False)