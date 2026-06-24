-- Additive cloud migration. Run manually against navamesh before deploying a Pi.
-- The legacy public.mesh_nodes table is intentionally left untouched.
BEGIN;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS public.mesh_nodes_farm1 (
    node_id TEXT PRIMARY KEY, last_seen TIMESTAMPTZ DEFAULT now(),
    lat DOUBLE PRECISION, lon DOUBLE PRECISION, metadata JSONB,
    geom geometry(Point, 4326)
);
CREATE TABLE IF NOT EXISTS public.mesh_nodes_farm2 (
    node_id TEXT PRIMARY KEY, last_seen TIMESTAMPTZ DEFAULT now(),
    lat DOUBLE PRECISION, lon DOUBLE PRECISION, metadata JSONB,
    geom geometry(Point, 4326)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mesh_nodes_farm1_node_id ON public.mesh_nodes_farm1 (node_id);
CREATE INDEX IF NOT EXISTS idx_mesh_nodes_farm1_geom ON public.mesh_nodes_farm1 USING GIST (geom);
CREATE UNIQUE INDEX IF NOT EXISTS idx_mesh_nodes_farm2_node_id ON public.mesh_nodes_farm2 (node_id);
CREATE INDEX IF NOT EXISTS idx_mesh_nodes_farm2_geom ON public.mesh_nodes_farm2 USING GIST (geom);

COMMIT;
