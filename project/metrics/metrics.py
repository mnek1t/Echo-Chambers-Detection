import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

def _cohesive(inside_similarities):
    return np.mean(inside_similarities) if inside_similarities else 0

def _separation(outside_similarities):
    return 1 - np.mean(outside_similarities) if outside_similarities else 0

def ecs(G, embeddings, communities):
    """
        G - Graph interactions - user nodes (from Graph DB). 
        embeddings - ideological postion of each user (from Vector DB).
        communities - calculated by Louvain (python-louvain library)
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
    com_sets = {}
    for node, com in communities.items():
        com_sets.setdefault(com, set()).add(node)
        
    return modularity(G, list(com_sets.values()), weight='weight') #TODO: check this


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

