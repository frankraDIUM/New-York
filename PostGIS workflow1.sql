-- 1. Add PK and spatial index
ALTER TABLE osm_vertices ADD PRIMARY KEY (id);
CREATE INDEX osm_vertices_geom_gist_idx ON osm_vertices USING GIST (geom);

-- 2. Populate source/target on edges (exact match first)
UPDATE osm_edges e
SET 
    source = v_start.id,
    target = v_end.id
FROM osm_vertices v_start,
     osm_vertices v_end
WHERE v_start.geom = ST_StartPoint(e.geometry)
  AND v_end.geom   = ST_EndPoint(e.geometry);

-- Fallback for any remaining NULLs (nearest neighbor, slower but robust)
UPDATE osm_edges
SET source = (
    SELECT id 
    FROM osm_vertices 
    ORDER BY geom <-> ST_StartPoint(geometry) 
    LIMIT 1
)
WHERE source IS NULL;

UPDATE osm_edges
SET target = (
    SELECT id 
    FROM osm_vertices 
    ORDER BY geom <-> ST_EndPoint(geometry) 
    LIMIT 1
)
WHERE target IS NULL;

-- 5. Indexes
CREATE INDEX IF NOT EXISTS osm_edges_source_idx ON osm_edges (source);
CREATE INDEX IF NOT EXISTS osm_edges_target_idx ON osm_edges (target);


-- Final Verification

SELECT 
    (SELECT COUNT(*) FROM osm_vertices) AS vertex_count,
    (SELECT COUNT(*) FROM osm_edges WHERE source IS NOT NULL AND target IS NOT NULL) AS routed_edges,
    (SELECT COUNT(*) FROM osm_edges WHERE source IS NULL OR target IS NULL) AS broken_edges;



-- Snap Subway Entrances to the Closest Network Vertex
-- 1. Add a column for the nearest network node ID
ALTER TABLE subway_entrances
    ADD COLUMN IF NOT EXISTS nearest_node_id BIGINT;

-- 2. Populate it with the closest vertex (using <-> operator for speed, thanks to GIST index)
UPDATE subway_entrances se
SET nearest_node_id = (
    SELECT v.id
    FROM osm_vertices v
    ORDER BY v.geom <-> se.geometry
    LIMIT 1
);

-- 3. Add index for faster lookups later
CREATE INDEX IF NOT EXISTS subway_entrances_nearest_node_idx 
    ON subway_entrances (nearest_node_id);

-- 4. Quick verification
SELECT 
    COUNT(*) AS total_entrances,
    COUNT(nearest_node_id) AS snapped_entrances,
    COUNT(DISTINCT nearest_node_id) AS unique_nodes_used
FROM subway_entrances;



-- Add the missing primary key column
ALTER TABLE subway_entrances
    ADD COLUMN IF NOT EXISTS entrance_id BIGSERIAL PRIMARY KEY;

-- Verify
SELECT 
    entrance_id,
    ST_AsText(geometry) AS point_sample,
    name,
    entrance,
    railway
FROM subway_entrances
ORDER BY entrance_id
LIMIT 5;





-- Compute 10-minute walking isochrones from subway entrances
-- 1. Drop & recreate the result table (safe)
DROP TABLE IF EXISTS subway_isochrones_10min;
CREATE TABLE subway_isochrones_10min (
    entrance_id BIGINT,
    node_id BIGINT,
    distance_feet DOUBLE PRECISION,
    geom GEOMETRY(POLYGON, 2263)
);

-- 2. Compute reachable nodes within ~10 min walk (2640 ft)
INSERT INTO subway_isochrones_10min (entrance_id, node_id, distance_feet, geom)
SELECT
    se.entrance_id,
    dd.node,
    dd.cost AS distance_feet,
    NULL  -- polygons filled in next step
FROM subway_entrances se
CROSS JOIN LATERAL (
    SELECT *
    FROM pgr_drivingDistance(
        'SELECT edge_id AS id, source, target, ST_Length(geometry) AS cost FROM osm_edges',
        ARRAY[se.nearest_node_id::BIGINT],
        2640,          -- 10 minutes ≈ 2640 feet at ~3 mph
        false,         -- undirected graph (walk both directions)
        false          -- no edge details needed
    )
) dd;



--Turn reachable nodes into nice polygon isochrones
-- 1. Create final isochrone polygons table
DROP TABLE IF EXISTS subway_isochrones_10min_polygons;
CREATE TABLE subway_isochrones_10min_polygons (
    entrance_id BIGINT PRIMARY KEY,
    reachable_area GEOMETRY(POLYGON, 2263),
    area_sqft DOUBLE PRECISION
);

-- 2. Compute concave hull (isochrone polygon) for each entrance
INSERT INTO subway_isochrones_10min_polygons (entrance_id, reachable_area, area_sqft)
SELECT 
    entrance_id,
    ST_ConcaveHull(
        ST_Collect(
            ARRAY_AGG(
                v.geom
            )
        ),
        0.99,          -- concaveness: 0.99 = fairly tight around points (0.8–0.99 common for isochrones)
        false          -- allow holes = false (usually better for walk areas)
    ) AS reachable_area,
    ST_Area(ST_ConcaveHull(
        ST_Collect(ARRAY_AGG(v.geom)),
        0.99,
        false
    )) AS area_sqft
FROM subway_isochrones_10min i
JOIN osm_vertices v ON i.node_id = v.id
GROUP BY entrance_id;


-- Quick checks
-- How many polygons did we get?
SELECT COUNT(*) AS polygon_count FROM subway_isochrones_10min_polygons;

-- Sample a few (look at area in square feet)
SELECT 
    entrance_id,
    ST_AsText(reachable_area) AS polygon_wkt_short,  -- first 100 chars
    area_sqft / 43560 AS area_acres,                 -- convert to acres
    area_sqft
FROM subway_isochrones_10min_polygons
LIMIT 5;



-- Export 10-min polygons in lat/lon (EPSG:4326)
COPY (
    SELECT jsonb_build_object(
        'type', 'FeatureCollection',
        'features', jsonb_agg(
            jsonb_build_object(
                'type', 'Feature',
                'geometry', ST_AsGeoJSON(ST_Transform(p.reachable_area, 4326))::jsonb,
                'properties', jsonb_build_object(
                    'entrance_id', p.entrance_id,
                    'name', e.name,
                    'wheelchair', e.wheelchair
                )
            )
        )
    )::text
    FROM subway_isochrones_10min_polygons p
    JOIN subway_entrances e ON p.entrance_id = e.entrance_id
) TO 'I:\GEO DATA ANALYSIS\Nyc Transit\Web\nyc_10min_isochrones_latlon.geojson' 
WITH (FORMAT TEXT);





-- 1. Compute reachable nodes within ~15 min (3960 ft)
-- Create table for 15-min reachable nodes
DROP TABLE IF EXISTS subway_isochrones_15min;
CREATE TABLE subway_isochrones_15min (
    entrance_id BIGINT,
    node_id BIGINT,
    distance_feet DOUBLE PRECISION,
    geom GEOMETRY(POLYGON, 2263)
);

-- Insert reachable nodes (this will take longer than 10-min — probably 1.5–3 hours)
INSERT INTO subway_isochrones_15min (entrance_id, node_id, distance_feet, geom)
SELECT
    se.entrance_id,
    dd.node,
    dd.cost AS distance_feet,
    NULL
FROM subway_entrances se
CROSS JOIN LATERAL (
    SELECT *
    FROM pgr_drivingDistance(
        'SELECT edge_id AS id, source, target, ST_Length(geometry) AS cost FROM osm_edges',
        ARRAY[se.nearest_node_id::BIGINT],
        3960,          -- 15 minutes ≈ 3960 feet
        false,         -- undirected
        false          -- no edge details
    )
) dd;



--2.Create polygons (concave hull) from the reachable nodes
-- Create final 15-min polygons table
DROP TABLE IF EXISTS subway_isochrones_15min_polygons;
CREATE TABLE subway_isochrones_15min_polygons (
    entrance_id BIGINT PRIMARY KEY,
    reachable_area GEOMETRY(POLYGON, 2263),
    area_sqft DOUBLE PRECISION
);

-- Generate concave hull polygons
INSERT INTO subway_isochrones_15min_polygons (entrance_id, reachable_area, area_sqft)
SELECT 
    entrance_id,
    ST_ConcaveHull(
        ST_Collect(ARRAY_AGG(v.geom)),
        0.99,     -- 0.99 = tight hull, change to 0.95 if too spiky
        false     -- no holes
    ) AS reachable_area,
    ST_Area(ST_ConcaveHull(
        ST_Collect(ARRAY_AGG(v.geom)),
        0.99,
        false
    )) AS area_sqft
FROM subway_isochrones_15min i
JOIN osm_vertices v ON i.node_id = v.id
GROUP BY entrance_id;

--3. Quick checks
-- Number of polygons created
SELECT COUNT(*) AS polygon_count_15min FROM subway_isochrones_15min_polygons;

-- Sample areas (in acres)
SELECT 
    entrance_id,
    area_sqft / 43560 AS area_acres,
    area_sqft
FROM subway_isochrones_15min_polygons
ORDER BY area_sqft DESC
LIMIT 10;


-- Export the polygons to GeoJSON
COPY (
    SELECT jsonb_build_object(
        'type', 'FeatureCollection',
        'features', jsonb_agg(
            jsonb_build_object(
                'type', 'Feature',
                'geometry', ST_Transform(p.reachable_area, 4326)::jsonb,
                'properties', jsonb_build_object(
                    'entrance_id', p.entrance_id,
                    'name', e.name,
                    'wheelchair', e.wheelchair
                )
            )
        )
    )::text
    FROM subway_isochrones_15min_polygons p
    JOIN subway_entrances e ON p.entrance_id = e.entrance_id
) TO 'I:\GEO DATA ANALYSIS\Nyc Transit\Web\nyc_15min_isochrones_latlon.geojson' 
WITH (FORMAT TEXT);




-- OPTIONAL Point layer for subway entrances (For wheelchair in Kepler)
COPY (
    SELECT json_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(ST_Transform(geometry, 4326))::json,
        'properties', json_build_object('name', name, 'wheelchair', wheelchair, 'entrance_id', entrance_id)
    )::text
    FROM subway_entrances
) TO 'I:\GEO DATA ANALYSIS\Nyc Transit\Web\subway_entrances_points.geojson' WITH (FORMAT TEXT);