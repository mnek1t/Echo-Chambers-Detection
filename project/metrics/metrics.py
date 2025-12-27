import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import networkx as nx
import pandas as pd
from collections import defaultdict

def _cohesive(inside_similarities):
    return np.mean(inside_similarities) if inside_similarities else 0

def _separation(outside_similarities):
    return 1 - np.mean(outside_similarities) if outside_similarities else 0

def ecs(G, embeddings, communities):
    """
        G - Graph interactions - user nodes (from Graph DB). 
        embeddings - ideological postion of each user (from Vector DB).
        communities - calculated by HDBScan
    """
    inside_similarities = []
    outside_similarities = []

    for u, v in G.edges():
        sim = cosine_similarity([embeddings[u]], [embeddings[v]])[0][0]
        
        if communities[u] == communities[v]:
            inside_similarities.append(sim)
        else:
            outside_similarities.append(sim) 

    cohesion = _cohesive(inside_similarities)
    separation = _separation(outside_similarities)
    
    ecs = cohesion * separation
    return ecs, cohesion, separation

def embedding_variance(embeddings, communities):
    variances = {}
    
    for c in set(communities.values()):
        members = [u for u in communities.keys() if communities[u] == c]
        vecs = np.array([embeddings[u] for u in members if u in embeddings])

        if vecs.size == 0:
            variances[c] = -1 # none of the members posted anything -> no embeddings
            continue

        centroid = vecs.mean(axis=0)
        var = np.mean(np.linalg.norm(vecs - centroid, axis=1)**2)
        variances[c] = var
    
    filtered_variances = {c: score for c, score in variances.items() if score != -1}

    return variances, filtered_variances


def compute_modularity(G, communities):
    full_communities = {}
    for u in G.nodes():
        full_communities[u] = communities.get(u, -1)  # -1 = noise / unassigned

    com_sets = defaultdict(set)
    for u, c in full_communities.items():
        com_sets[c].add(u)

    return nx.community.modularity(G, list(com_sets.values()))


def homophily(G, embeddings):
    sims = []
    for u, v in G.edges():
        sim = cosine_similarity([embeddings[u]], [embeddings[v]])[0][0]
        sims.append(sim)
    return np.mean(sims)

def compute_conductance(G, communities):
    conductance_scores = {}
    
    for c in set(communities.values()):
        C = {u for u in G.nodes() if communities[u] == c}
        outside = set(G.nodes()) - C
        
        cut_edges = sum(1 for u, v in G.edges() if (u in C and v in outside) or (v in C and u in outside))
        
        vol_C = sum(G.degree(u) for u in C)
        vol_out = sum(G.degree(u) for u in outside)
        
        conductance_scores[c] = cut_edges / min(vol_C, vol_out) if min(vol_C, vol_out) > 0 else 10000 # arbitrarily large number
    
    filtered_conductance_scores = {c: score for c, score in conductance_scores.items() if score < 10000}

    return conductance_scores, filtered_conductance_scores


def per_community_table(G, embeddings, communities):
    """
    Compute per-community cohesion, separation, ECS (cohesion*separation), conductance,
    embedding variance, size, and internal edge density—consistent with your metric definitions.
    """
    # group nodes by community
    comm_nodes = defaultdict(set)
    G_nodes = set(G.nodes())

    for u, c in communities.items():
        if u in embeddings and u in G_nodes:
            comm_nodes[c].add(u)

    rows = []
    # Precompute edge similarities once for reuse
    edge_sims = {}
    for u, v in G.edges():
        if u in embeddings and v in embeddings:
            edge_sims[(u, v)] = cosine_similarity([embeddings[u]], [embeddings[v]])[0][0]

    for c, members in comm_nodes.items():
        members = set(members)
        outside = set(G.nodes()) - members

        # Internal & external edge similarities
        internal_sims = []
        external_sims = []
        for (u, v), sim in edge_sims.items():
            cu, cv = communities.get(u), communities.get(v)
            if u in members and v in members:
                internal_sims.append(sim)
            elif (u in members and v in outside) or (v in members and u in outside):
                external_sims.append(sim)

        cohesion = float(np.mean(internal_sims)) if len(internal_sims) > 0 else np.nan
        separation = 1.0 - float(np.mean(external_sims)) if len(external_sims) > 0 else np.nan
        ecs_val = (cohesion * separation) if np.isfinite(cohesion) and np.isfinite(separation) else np.nan

        # Conductance (same formula as your function, per community)
        cut_edges = sum(1 for u, v in G.edges()
                        if (u in members and v in outside) or (v in members and u in outside))
        
        vol_C = sum(G.degree[u] for u in members)
        vol_out = sum(G.degree[u] for u in outside)


        conductance = (cut_edges / min(vol_C, vol_out)) if min(vol_C, vol_out) > 0 else np.nan

        # Embedding variance (same as your function, but inlined for per-community convenience)
        vecs = np.array([embeddings[u] for u in members if u in embeddings])
        if vecs.size > 0:
            centroid = vecs.mean(axis=0)
            variance = np.mean(np.linalg.norm(vecs - centroid, axis=1) ** 2)
        else:
            variance = np.nan

        homophily = float(np.mean(internal_sims)) if len(internal_sims) > 0 else np.nan

        # Size and internal density
        subG = G.subgraph(list(members))
        size = subG.number_of_nodes()
        possible_edges = size * (size - 1) / 2
        density_internal = subG.number_of_edges() / possible_edges if possible_edges > 0 else np.nan

        rows.append({
            "community": c,
            "size": size,
            "cohesion": cohesion,
            "separation": separation,
            "ecs": ecs_val,
            "conductance": conductance,
            "variance": variance,
            "homophilly": homophily,
            "density_internal": density_internal,
            "internal_edge_count": subG.number_of_edges(),
        })

    df_comm = pd.DataFrame(rows)
    return df_comm.sort_values("ecs", ascending=False)
