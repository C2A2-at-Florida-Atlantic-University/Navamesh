# Farm isolation deployment

Local Pi storage continues to use `public.mesh_nodes`; only cloud writes use a
farm-specific table. Do not copy legacy rows based on `metadata->>'location'`:
overlapping node IDs may already have replaced their original farm identity.

## Per-Pi environment

```dotenv
# Farm 1
FARM_ID=farm1
LOCATION_NAME=FAU Garden
INFLUX_CLOUD_BUCKET=navamesh_soil

# Farm 2 (use these three values instead on the second Pi)
FARM_ID=farm2
LOCATION_NAME=Spirit Farm
INFLUX_CLOUD_BUCKET=navamesh_soil_2
```

Legacy `farm_1` and `farm_2` IDs normalize to their canonical forms. Any other
value stops startup rather than selecting the wrong table.

## Safe deployment order

1. Review and manually run `sql/001_add_farm_tables.sql` in cloud PostgreSQL.
2. Update/restart the Farm 1 ingestor, then the Farm 2 ingestor.
3. Let retained/live Pi data populate each table and run the checks below.
4. Deploy the updated cloud API, then frontend.
5. Retain legacy `public.mesh_nodes` for rollback. Do not delete Influx buckets.

## Read-only verification

```sql
SELECT 'farm1' AS farm, count(*) FROM public.mesh_nodes_farm1
UNION ALL SELECT 'farm2', count(*) FROM public.mesh_nodes_farm2;

SELECT f1.node_id FROM public.mesh_nodes_farm1 f1
JOIN public.mesh_nodes_farm2 f2 USING (node_id) ORDER BY f1.node_id;

SELECT 'farm1' AS farm, metadata->>'location' AS location, count(*)
FROM public.mesh_nodes_farm1 GROUP BY metadata->>'location'
UNION ALL
SELECT 'farm2', metadata->>'location', count(*)
FROM public.mesh_nodes_farm2 GROUP BY metadata->>'location';
```

```sh
curl -fsS 'https://nextg-ag.org/api/nodes?farm=farm1'
curl -fsS 'https://nextg-ag.org/api/nodes?farm=farm2'
curl -fsS 'https://nextg-ag.org/api/history?farm=farm1&node_id=NODE_ID&metric=soil&range=24h'
curl -i 'https://nextg-ag.org/api/nodes?farm=invalid'
curl -i 'https://nextg-ag.org/api/history?farm=invalid&node_id=NODE_ID'
```

The last two requests must return HTTP 400.

## Rollback

1. Restore the prior cloud API/frontend release to read legacy `mesh_nodes`.
2. Restore/restart the prior Pi ingestor release; local storage is unchanged.
3. Keep both additive farm tables and Influx buckets intact while investigating.
