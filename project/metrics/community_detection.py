import os
from graphdatascience import GraphDataScience

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
gds = GraphDataScience(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

#TODO: refactor as function(s)

if gds.graph.exists("userGraph")["exists"]:
    gds.graph.drop("userGraph")

G, result = gds.graph.project.cypher(
    "userGraph",
    "MATCH (u:User) RETURN id(u) AS id",
    """
    MATCH (u1:User)-[:LIKED|POSTED]->(p:Post)<-[:LIKED|POSTED]-(u2:User)
    WHERE id(u1) < id(u2)
    RETURN id(u1) AS source, id(u2) AS target, count(p) AS weight
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

df["neo4jId"] = df["nodeId"].apply(lambda x: gds.util.asNode(x).element_id)
df = df.drop(columns=["nodeId"])

df.to_csv("hdbscan_clusters.csv", index=False)