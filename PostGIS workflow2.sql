-- 1. Verify the import of demographics geojson file
-- Number of tracts (should be ~2167–2325)
SELECT COUNT(*) AS num_tracts FROM nyc_census_tracts_demographics;

-- Sample data (look for population, median_income, geometry)
SELECT geoid, population, median_income, ST_GeometryType(geometry) AS geom_type
FROM nyc_census_tracts_demographics
LIMIT 5;

-- Check CRS (should be 4326 since GeoJSON defaults to lat/lon)
SELECT ST_SRID(geometry) FROM nyc_census_tracts_demographics LIMIT 1;




-- 2. Reproject to EPSG:2263 (for accurate distance in feet)
-- Add projected geometry column
ALTER TABLE nyc_census_tracts_demographics
ADD COLUMN geom_2263 GEOMETRY(MULTIPOLYGON, 2263);

-- Transform
UPDATE nyc_census_tracts_demographics
SET geom_2263 = ST_Transform(geometry, 2263);

-- Spatial index (makes distance queries fast)
CREATE INDEX nyc_tracts_2263_gist ON nyc_census_tracts_demographics USING GIST (geom_2263);




--3. Calculate distance from each tract to nearest subway entrance
-- Add distance column (in feet)
ALTER TABLE nyc_census_tracts_demographics
ADD COLUMN dist_to_nearest_subway_ft DOUBLE PRECISION;

-- Compute min distance
UPDATE nyc_census_tracts_demographics t
SET dist_to_nearest_subway_ft = (
    SELECT MIN(ST_Distance(t.geom_2263, se.geometry))
    FROM subway_entrances se
);


-- Quick stats
SELECT 
    COUNT(*) AS total_tracts,
    COUNT(*) FILTER (WHERE dist_to_nearest_subway_ft IS NOT NULL) AS tracts_with_distance,
    AVG(dist_to_nearest_subway_ft) AS avg_distance_ft,
    MIN(dist_to_nearest_subway_ft) AS min_distance_ft,
    MAX(dist_to_nearest_subway_ft) AS max_distance_ft
FROM nyc_census_tracts_demographics;



-- 4. Flag transit deserts
UPDATE nyc_census_tracts_demographics
SET is_transit_desert = FALSE;

-- Re-flag using correct threshold (2625 ft)
UPDATE nyc_census_tracts_demographics
SET is_transit_desert = TRUE
WHERE dist_to_nearest_subway_ft > 2625 AND dist_to_nearest_subway_ft IS NOT NULL;


-- Summary stats
SELECT 
    COUNT(*) AS total_tracts,
    COUNT(*) FILTER (WHERE is_transit_desert) AS desert_tracts,
    ROUND(AVG(population_num) FILTER (WHERE is_transit_desert), 0) AS avg_pop_in_deserts,
    ROUND(AVG(median_income_num) FILTER (WHERE is_transit_desert), 0) AS avg_income_in_deserts
FROM nyc_census_tracts_demographics;


--Export
COPY (
    SELECT jsonb_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(ST_Transform(geom_2263, 4326))::jsonb,
        'properties', jsonb_build_object(
            'geoid', geoid,
            'population', population_num,
            'median_income', median_income_num,
            'dist_ft', dist_to_nearest_subway_ft,
            'desert', is_transit_desert
        )
    )::text
    FROM nyc_census_tracts_demographics
) TO 'I:\GEO DATA ANALYSIS\Nyc Transit\Web\nyc_tracts_deserts.geojson' --Add Path
WITH (FORMAT TEXT);




-- Reset and re-flag with 2000 ft
UPDATE nyc_census_tracts_demographics SET is_transit_desert = FALSE;

UPDATE nyc_census_tracts_demographics
SET is_transit_desert = TRUE
WHERE dist_to_nearest_subway_ft > 2000 AND dist_to_nearest_subway_ft IS NOT NULL;

-- Re-run summary
SELECT 
    COUNT(*) AS total_tracts,
    COUNT(*) FILTER (WHERE is_transit_desert) AS desert_tracts,
    ROUND(AVG(population_num) FILTER (WHERE is_transit_desert), 0) AS avg_pop_in_deserts,
    ROUND(AVG(median_income_num) FILTER (WHERE is_transit_desert), 0) AS avg_income_in_deserts
FROM nyc_census_tracts_demographics;



COPY (
    SELECT jsonb_build_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(ST_Transform(geom_2263, 4326))::jsonb,
        'properties', jsonb_build_object(
            'geoid', geoid,
            'boroname', boroname,
            'population', population_num,
            'median_income', median_income_num,
            'dist_ft', dist_to_nearest_subway_ft,
            'desert_2000ft', is_transit_desert
        )
    )::text
    FROM nyc_census_tracts_demographics
) TO 'I:\GEO DATA ANALYSIS\Nyc Transit\Web\nyc_tracts_deserts_2000ft.geojson' 
WITH (FORMAT TEXT);



ALTER TABLE nyc_census_tracts_demographics
ADD COLUMN desert_2000ft_str TEXT;

UPDATE nyc_census_tracts_demographics
SET desert_2000ft_str = CASE
    WHEN is_transit_desert THEN 'true'
    ELSE 'false'
END;


--Run this part in psql
\copy (
    SELECT
        jsonb_build_object(
            'type', 'FeatureCollection',
            'features', jsonb_agg(
                jsonb_build_object(
                    'type', 'Feature',
                    'geometry', ST_AsGeoJSON(
                        ST_Transform(geom_2263, 4326)
                    )::jsonb,
                    'properties', jsonb_build_object(
                        'geoid', geoid,
                        'boroname', boroname,
                        'population', population_num,
                        'median_income', median_income_num,
                        'dist_ft', dist_to_nearest_subway_ft,
                        'desert_2000ft', is_transit_desert
                    )
                )
            )
        )
    FROM nyc_census_tracts_demographics
) TO 'I:/GEO DATA ANALYSIS/Nyc Transit/Web/nyc_tracts_deserts_2000ft.geojson';


