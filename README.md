
---

# 🚇 Transit Accessibility Analysis in New York City

**PostGIS · pgRouting · OSMnx · GTFS · Kepler.gl** (https://bit.ly/3Oh2605)

## Overview

New York City operates one of the world’s largest rapid transit systems, yet **significant accessibility gaps remain**, particularly in outer boroughs. This project analyzes **walking-based access to subway entrances**, generates **10- and 15-minute walking isochrones**, and identifies **transit deserts**,census tracts where subway access exceeds a realistic walking threshold.

The analysis integrates:

* **OpenStreetMap** walking networks (via OSMnx)
* **PostGIS + pgRouting** for spatial analysis and routing
* **U.S. Census TIGER/Line & ACS** demographics
* **Kepler.gl** for interactive visualization

📍 **Key outcome:** 674 NYC census tracts (≈29%) qualify as transit deserts under a 2,000-ft walking threshold.

---

## Research Questions

* How far is each NYC census tract from the nearest subway entrance?
* Which tracts qualify as transit deserts under realistic walking assumptions?
* Do transit deserts disproportionately affect high-population or low-income communities?

---

## Methodological Note: Walking Threshold

While **800 m (½ mile)** is often used as a standard walkable distance, NYC’s dense station spacing and observed rider behavior suggest a **shorter effective catchment**.

**This study adopts a 2,000 ft (~610 m, ~7–8 minute walk) threshold**, reflecting:

* High subway entrance density
* Shorter tolerated walking distances among NYC riders
* More conservative, behavior-aware accessibility modeling



## Data Sources

| Dataset              | Source                     | CRS       | Description               |
| -------------------- | -------------------------- | --------- | ------------------------- |
| Walking network      | OpenStreetMap (OSMnx)      | EPSG:2263 | ~269k nodes, ~865k edges  |
| Subway entrances     | OpenStreetMap              | EPSG:2263 | 2,024 entrances           |
| Census tracts (2020) | TIGER/Line + NYC Open Data | EPSG:4326 | 2,325 tracts              |
| ACS 2021 (5-yr)      | U.S. Census (DP03)         | —         | Population, median income |


Walking Network of New York City
<div>
  <img src="https://github.com/frankraDIUM/Transit-Accessibility-Analysis-in-New-York-City/blob/main/Figure_1.png"/>
</div> 


## Methodology

### 1. Walking Network Construction

* Downloaded NYC walkable street network using OSMnx.
* Reprojected to **EPSG:2263** (feet-based CRS).
* Loaded into PostGIS and built routable topology.
* Snapped subway entrances to nearest network vertices.

**Scripts:**
`download_osm_data.py`, `load_osm_to_postgis.py`, `prepare_network.sql`

---

### 2. Isochrone Generation

* Used `pgr_drivingDistance` (undirected graph).
* Walking speed: ~3 mph.
* Thresholds:

  * **10 minutes:** 2,640 ft
  * **15 minutes:** 3,960 ft
* Converted reachable nodes into polygons using `ST_ConcaveHull`.
* Reprojected to EPSG:4326 for web mapping.

---

### 3. Transit Desert Identification

* Reprojected census tracts to EPSG:2263.
* Calculated minimum distance from each tract to any subway entrance.
* Flagged tracts as **transit deserts** if distance > **2,000 ft**.

---

### 4. Demographic Integration

* Joined ACS 2021 DP03 data to tracts via `GEO_ID`.
* Cleaned and converted numeric fields (population, income).
* Handled suppressed ACS values (`"-"` → `NULL`).

---

### 5. Visualization

* Exported final GeoJSON with:

  * Geometry
  * Distance to subway
  * Desert flag
  * Population
  * Median household income
* Visualized in **Kepler.gl**:

  * Tracts colored by desert status
  * Heights scaled by population
  * 10- and 15-minute isochrones overlaid

---

## Results

### Distance Summary

* **Total tracts:** 2,325
* **Average distance to subway:** ~2,622 ft (~800 m)
* **Minimum:** 0 ft
* **Maximum:** ~58,786 ft (~11 miles)

---

### Transit Deserts (2,000 ft Threshold)

* **Desert tracts:** 674 (≈29%)
* **Average population per desert tract:** 2,538
* **Average median income (deserts):** $84,289
  *(Higher than NYC average of ~$75k–$80k)*

---

### Distance Buckets

| Distance       | Tracts | Avg Population | Avg Income |
| -------------- | ------ | -------------- | ---------- |
| ≤1,000 ft      | 1,315  | 3,408          | $76,777    |
| 1,001–2,000 ft | 336    | 2,715          | $71,217    |
| 2,001–2,625 ft | 107    | 2,567          | $75,812    |
| 2,626–5,000 ft | 243    | 2,321          | $77,805    |
| >5,000 ft      | 324    | 2,692          | $91,977    |

**Key insight:**
NYC transit deserts are **not concentrated in low-income areas**; many occur in **wealthier, auto-dependent neighborhoods**.

Transit Accessibility map
<div>
  <img src="https://github.com/frankraDIUM/Transit-Accessibility-Analysis-in-New-York-City/blob/main/kepler.gl.png"/>
</div> 
---

## Discussion

* ~29% of NYC tracts qualify as transit deserts at a 2,000-ft threshold.
* Higher-income outer-borough areas dominate extreme distances.
* Unlike many cities, NYC transit deserts reflect **urban form and density**, not poverty concentration.

### Limitations

* Straight-line distance (not full pedestrian routing).
* Station entrances may involve long internal walks.
* Results sensitive to threshold selection.

### Future Work

* Integrate buses and GTFS schedules.
* Add race/ethnicity and poverty indicators (ACS DP05).
* Population-weighted accessibility metrics.

---

## Conclusion

This project builds a city-scale walking network, generates realistic subway isochrones, and identifies **674 transit desert census tracts** in NYC using a behavior-aware 2,000-ft threshold. Results show that NYC’s transit deserts are largely **low-density and auto-oriented**, rather than income-driven, highlighting structural gaps in subway coverage.

---

## Tools & Technologies

PostgreSQL · PostGIS · pgRouting · OSMnx · GeoPandas · QGIS · Kepler.gl

## Data Sources

OpenStreetMap · U.S. Census TIGER/Line (2020) · ACS 2021 · NYC Open Data

📌 **Live demo:** Kepler.gl map (https://bit.ly/3Oh2605)

---

