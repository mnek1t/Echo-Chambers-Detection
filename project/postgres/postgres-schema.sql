DROP TABLE IF EXISTS community_membership CASCADE;
DROP TABLE IF EXISTS community_metrics CASCADE;
DROP TABLE IF EXISTS community CASCADE;
DROP TABLE IF EXISTS algorithm CASCADE;
DROP TABLE IF EXISTS clustering_run CASCADE;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE algorithm (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE clustering_run (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    algorithm_id UUID NOT NULL REFERENCES algorithm(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    description TEXT
);

CREATE TABLE community (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES clustering_run(id),
    label INTEGER NOT NULL,
    total_amount INTEGER DEFAULT 0,
    UNIQUE (run_id, label)
);

CREATE TABLE community_metrics (
    community_id UUID PRIMARY KEY REFERENCES community(id) ON DELETE CASCADE,
    homophily DOUBLE PRECISION,
    cohesion DOUBLE PRECISION,
    separation DOUBLE PRECISION,
    ecs DOUBLE PRECISION,
    variance DOUBLE PRECISION,
    conductance DOUBLE PRECISION,
    density_internal DOUBLE PRECISION,
    internal_edge_count INTEGER,
    subgraph_size INTEGER
);

CREATE TABLE community_membership (
    community_id UUID NOT NULL REFERENCES community(id) ON DELETE CASCADE,
    neo4j_id VARCHAR(255) NOT NULL,
    valid_from TIMESTAMP NOT NULL DEFAULT NOW(),
    valid_to TIMESTAMP,
    PRIMARY KEY (community_id, neo4j_id)
);