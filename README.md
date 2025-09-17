# Montana DOT Public Crash Data Analysis
Analysis code and data for the Montana DOT crash-rate article. This repository contains the scripts, intermediate outputs, and final CSVs used to match crash records to highway route segments, compute traffic-weighted rates, and build the highway-to-highway "trips" used in the published analysis.

## Quick checklist of what this README contains

- How to set up the environment (Python version, venv, install requirements).
- How to run the main scripts to reproduce the analysis outputs.
- A plain-language explainer describing the data and how conclusions were reached (suitable to append to an article).
- A concise engineering-level methodology describing the matching, traffic averaging, and rate calculations, plus key choices and limitations.

## Prerequisites
- Python3 (we used 3.13). 
    - Directions on how to do that on Windows, macOS and Linux can be found [in this guide](https://realpython.com/installing-python/)
- Create and activate a virtual environment and install dependencies:

```bash
# On macOS/Linux or other Unix-like operating systems:
python3.13 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```
- Windows directions for creating a venv can be found [in the official python documentation](https://docs.python.org/3/library/venv.html)



## Quick run:

- For convenience, there is a shell script attached `run-data-process.sh` to use it:
``` bash
# Make the script executable 
chmod +x ./run-data-process.sh

# run the script
./run-data-process.sh
```
#### The script does the following:
- Checks for the prescence of a python virtual environment or "venv" and activates it if it exists
- Invokes the following python files:
```
merge_traffic.py
analyze_data.py
manually_segment_routes.py
```

The repository contains multiple scripts. A typical order to reproduce core outputs is:

1. Merge traffic and crash data: `merge_traffic.py` and `segment_merge_traffic_accidents.py` (these combine traffic counts with segment definitions and crash tallies).
2. Create higher-level summaries or aggregates: `create_highway_summaries.py`, `aggregate_major_highways.py`, `aggregate_top_routes.py`.
3. Produce the per-segment and per-trip CSVs: look for `per_mile.csv`, `per_car.csv`, `crash_rates.csv`, and the `aggregated-highways/` outputs.

Example minimal command to run one of the core analysis scripts:

```bash
source .venv/bin/activate
python analyze_data.py
```

Note: scripts accept different inputs and may assume certain files exist (for example, preprocessed `route_segments.csv` or `montana_route_segments_with_safety.csv`). Inspect the top of each script for required arguments or inline comments.

## Primary output files

- `per_mile.csv`, `per_car.csv`, `crash_rates.csv` — rate tables used in the article.
- `montana_route_segments_with_safety.csv` — master route segments with safety/traffic fields.
- `route_segments.csv` / `logical_highway_segments.csv` — route geometry and segment metadata.
- `aggregated-highways/` — grouped outputs and per-highway aggregated CSVs used to build the maps and top-lists.

If you need a specific command to recreate any single CSV, open the corresponding script file (top matter usually documents required inputs) and run it while the venv is active.

## Plain-language explainer (suitable to append to an article)

This repository contains the data and scripts used to analyze crashes on Montana's state highways. In short: we matched reported crashes to the DOT's traffic-count segments, estimated traffic for each stretch of road, and calculated crash rates that account for how many vehicles use a road.

Data used
- Crash counts: FARS-style crash records for 2019–2023 obtained from MDT's site.
- Traffic counts: the MDT TYC/traffic-count exports (several yearly files) that list AADT (average annual daily traffic) by segment.
- Route/segment definitions: MDT route segment files (DEPT_ID, SITE_ID, CORRIDOR, corridor milepoints) used to locate crashes on the network.

How we reached the conclusions
- Each crash record was matched to a DOT route segment using the corridor identifier and the reported reference point / milepost.
- The public highway name (for readers) was taken from the MDT "Montana on System Routes" field `SIGNED_ROUTE`.
- Traffic estimates: 2023 was preferred when present. If a 2023 value was absent we used 2022, and only used 2021 when both 2023 and 2022 were missing. For segment-level averages across years we only combined years that exactly matched on `DEPT_ID`, `SITE_ID`, `CORRIDOR`, `CORR_MP`, and `CORR_ENDMP`.
- Crash rates: we calculated crashes per AADT, crashes per mile (using AADT × segment length as a rough scale), and crashes per 100 million vehicle-miles traveled. For the latter we multiplied daily traffic by 365.25 and by 5 years to estimate vehicle-miles over the analysis period (2019–2023).

What readers should know
- Rate metrics account for how much traffic a road sees; a busy road with more crashes can still have a lower crash rate than a quieter but more dangerous stretch.
- We limited the presented analysis to Interstates, US Highways, Montana Highways, and Secondary Montana Highways.

## Engineer-focused methodology and choices (concise)

1. Matching crashes to segments
	- Matched 2019–2023 crash rows from the MDT/FARS exports to route segments from the TYC data by `CORRIDOR` and the crash `ref_point`/milepost.
	- Used the `SIGNED_ROUTE` ("Montana on System Routes") field to map to the public highway name.

2. Choosing which traffic year(s) to use
	- Preferred 2023 traffic counts when available for a segment. If a 2023 count was missing, used 2022, then 2021 only when both newer years were absent.
	- Where a segment had exact matches across multiple years (exact `DEPT_ID`/`SITE_ID`/`CORRIDOR`/`CORR_MP`/`CORR_ENDMP`), averaged AADT across the years that matched exactly. The segment had to match exactly to be included in the mean.
	- Rationale: segment boundaries change over time (splits/merges). Averaging only exact matches minimizes bias from changed geometry, at the cost of dropping data from years where segments were defined differently.

3. Dealing with non-unique site IDs
	- Discovered `SITE_ID` is not always globally unique (e.g., around some I-90 runs). Therefore matching required multiple fields to ensure we were averaging the same physical segment.

4. Aggregation and trip-building
	- Generated per-segment crash totals and three rate metrics (per-AADT, per-mile, per-100M VMT).
	- Selected the top-100 segments per metric, expanded to the highways that contain them, and then manually defined origin/destination city pairs (cities with population ≥ ~900 connected by that highway) to form a "trip".
	- For each trip we assembled the contiguous segments between the chosen start/end, exported per-segment crash lists, and computed trip-level crash rates using a segment-length-weighted average AADT.

5. Calculations
	- Per-mile approximation: use segment AADT × length (SEC_LNT_MI) as a proxy for exposure.
	- Per-100M VMT: daily VMT = AADT × length; 5-year VMT ≈ daily VMT × 365.25 × 5; scale crashes accordingly to get crashes per 100 million vehicle-miles.

6. Manual review
	- For the final trip boundaries, used the 2024 MDT traffic map and changes in posted speed limit as heuristics to find where the highway transitions into a town roadway. For interstates, split roughly at the middle of the town segment since interstates don't always bisect towns the same way.

## Limitations, caveats, and design choices

- Segment geometry changes: when segments split or merge across years, the strict exact-match averaging approach cannot use traffic values from non-matching years even if they physically cover the same stretch. This avoids mixing non-comparable segment definitions.
- Manual trip boundaries: trips were defined by hand for the top candidates; this step required human judgment and relies on the 2024 traffic map being consistent with the 2023 segment definitions used in the analysis.
- Crash geolocation quality: crash mileposts / reference points can be imprecise — matching uses the available corridor/milepost data and may occasionally miss or misplace single crashes.
- Site ID collisions: some `SITE_ID` values are reused or not unique; matching uses a combination of identifiers to avoid accidental aggregation.

## Reproducibility notes and suggestions

- Keep originals: store raw downloads (MDT/FARS/TYC) under `data/raw` so you can trace exactly which inputs produced which outputs.
- Run steps in small pieces: convert/clean → merge traffic → attach crashes → aggregate → manual trip build. That order is how the scripts are laid out and how intermediate CSVs are used.

## Files of interest

See the top-level scripts for more details and usage hints: `analyze_data.py`, `merge_traffic.py`, `segment_merge_traffic_accidents.py`, `create_highway_summaries.py`, `aggregate_major_highways.py` and the various `aggregate_*` helpers. The `aggregated-highways/` folder contains grouped outputs for use in mapping and reporting.

## Contact

If you need a tailored extract or a runnable pipeline (Docker container / Makefile / full reproducible run), open an issue or contact the repository owner.

## Data pipeline order and artifact map

1) `merge_traffic.py` (first)
	- Takes in TYC traffic data from `data/Traffic_Yearly_Counts_{year}/` (for example `TYC_2023.csv`) and crash data (`2019-2023-crash-data.csv`) and uses `Montana_On_System_Routes.csv` to map internal MDT `DEPT_ID` values to the highway names shown on road signs. It uses corridor IDs and mileposts from the traffic files to define road segments, matches crashes to the segment whose milepost range contains them, prefers 2023 data and only uses previous years to fill gaps, and averages AADT only across exact segment matches (SITE_ID/DEPT_ID/CORR_MP/CORR_ENDMP).
	- Key outputs include merged segment-level data with crash totals and rates (for example `processed-data/merged-data/merged_traffic_average.csv`) and per-route CSV and GeoJSON files for later mapping and analysis in the `all-roads` directory.

2) `analyze_data.py` (first pass)
	- Takes the merged traffic+crash file (`merged_traffic_average.csv`), filters out unsigned and very low-volume roads, and ranks the remaining segments by crash risk using two metrics: vehicles per accident and miles per accident.
	- Outputs include CSVs with the top-100 ranked segments by crashes-per-daily-traffic and by crashes-per-mile, per-department CSVs for DEPT_IDs that appear in those lists, and GeoJSON used for visual inspection and debugging.

3) `manually_segment_routes.py`
	- Takes the manual segmentation table (`manual-route-segments.csv`) that was generated by segmenting up the roads that were at the top of the `by_car` output of `analyze_data.py`, along with route-level CSVs and GeoJSONs from `all-roads` produced by `merge_traffic.py`, and uses the defined milepost ranges to select constituent TYC rows and features.
	- Outputs one CSV and one GeoJSON per manual segment, plus combined files (`all-routes.csv` and `all-routes.geojson`) which are primarily used for debugging.

4) `analyze_trips.py`
	- Reads the per-segment CSV and GeoJSON files created in `manual-segments/`, aggregates traffic (length-weighted averages), length, and crash data for each manual segment, and computes crash-rate metrics (vehicles per accident, miles per accident, crashes per 100M vehicle-miles traveled).
	- Outputs ranked CSV summaries into `trip_analysis/` for each metric and GeoJSON bundles of the top segments for mapping; `per_vmt.csv` and the `per_vmt/` GeoJSONs are used as the preferred source of truth since crashes-per-VMT reduces noise and follows industry practice.

Notes and tips
- The pipeline assumes a small iterate-and-review loop: produce averaged traffic, inspect top segments, define manual trip boundaries, then re-run ranking so the final exported CSVs and maps reflect the manual choices.
- If you want a fully automated run (no manual step), you can script the manual segmentation CSV creation, but that will remove the human judgment used to select sensible city boundary breaks.

## This is the data we used (folder: `data/`)

Below are the primary inputs that were used in the analysis and what each is used for.

- `2019-2023-crash-data.csv` — the cleaned crash records (2019–2023) with corridor and reference-point information that we match to route segments.
- `Traffic_Yearly_Counts_{year}/TYC_{year}.csv` (2019–2024) — the MDT yearly traffic-count exports. These provide AADT by segment and the mapping/milepost fields (`CORR_ID`, `SITE_ID`, `CORR_MP`, `CORR_ENDMP`) we use to match crashes and normalize crash counts by traffic and miles driven.
- `Montana_On_System_Routes_OD.csv` — MDT's on-system route lookup used to map internal departmental route IDs to the public highway name (`SIGNED_ROUTE` / `ROUTE_NAME`) shown to readers.
- `manual-route-segments.csv` — a human-edited CSV that defines origin/destination segment boundaries for each highway trip we report on (cities, start/end CORR_MP values). This file is edited during the review step and drives the trip-building script.
- GeoJSONs and helper files (e.g. `mt-interstates.geojson`, `crash-geojson/`) — used for mapping, manual inspection, and visual QA during the review process.

If you keep raw downloads, we recommend storing them under `data/raw/` (not required) so it's obvious which files were downloaded versus produced by the pipeline.

## Scripts we ran (high-level) and how to reproduce

These are the primary scripts used in the analysis with a short single-line description of each. To reproduce the pipeline locally with an interactive pause for the manual edit step, make the helper script executable and run it:

```bash
chmod +x ./run-data-process.sh
./run-data-process.sh
```

- `merge_traffic.py` — load TYC yearly counts, enrich segments with on-system metadata, and produce averaged per-segment traffic tables and per-department outputs (creates `processed-data/merged-data/merged_traffic_average.csv` and `all_roads/` files).
- `analyze_data.py` — filter and rank segments, export top-N lists and per-department CSVs, and produce GeoJSONs for mapping/top-segments (reads the merged output from `merge_traffic.py`).
- `manually-segment-routes.py` — use `data/manual-route-segments.csv` to build contiguous "trip" definitions from segment boundaries; produces aggregated trip CSVs used in the final reporting.
- `analyze_trips.py` / `create_highway_summaries.py` — perform trip-level aggregation and produce the final per-trip statistics and highway summary CSVs used in the article.

Technical detail and the full methodology are documented in the sections above (Engineer-focused methodology, Limitations, and Reproducibility notes).

