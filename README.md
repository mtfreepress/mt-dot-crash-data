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
analyze_trips.py
```

## High Level:

This repository contains the data and scripts used to analyze crashes on Montana's state highways. In short: we matched reported crashes to the DOT's traffic-count segments, estimated traffic for each stretch of road, and calculated crash rates that account for how many vehicles use a road.

Data used
- All input data is in the `data/` directory of this project
- Crash counts: (MDT FARS crash records for 2019–2023)[https://www.mdt.mt.gov/publications/datastats/crashdata.aspx].
- Traffic counts: the MDT TYC/traffic-count exports (several yearly files) that list AADT (average annual daily traffic) by segment.
- "On System Routes" to map internal DEPT_ID to common highway names.

How we reached the conclusions
- Each crash record was matched to a DOT route segment using the corridor identifier and the reported reference point / milepost.
- The public highway name (for readers) was taken from the MDT "Montana on System Routes" field `SIGNED_ROUTE`.
- Traffic estimates: 2023 was preferred when present. If a 2023 value was absent we used 2022, and only used 2021 when both 2023 and 2022 were missing etc. For segment-level averages across years we only combined years that exactly matched on `DEPT_ID`, `SITE_ID`, `CORRIDOR`, `CORR_MP`, and `CORR_ENDMP`.
- Crash rates: we calculated crashes per AADT, crashes per average daily mile (using AADT × segment length as a rough scale), and crashes per 100 million vehicle-miles traveled. For the latter we multiplied daily miles by 365.25 and by 5 years to estimate vehicle-miles over the analysis period (2019–2023). Crashes per 100 million vehicle-miles traveled is what we lean on in the story because it is an industry standard metric. 

What readers should know
- Rate metrics account for how much traffic a road sees; a busy road with more crashes can still have a lower crash rate than a quieter but more dangerous stretch.
- We limited the presented analysis to Interstates, US Highways, Montana Highways, and Secondary Montana Highways.

## Data used (folder: `data/`)

Below are the primary inputs that were used in the analysis and what each is used for.

- `2019-2023-crash-data.csv` — the cleaned crash records (2019–2023) with corridor and reference-point information that we match to route segments.
- `Traffic_Yearly_Counts_{year}/TYC_{year}.csv` (2019–2024) — the MDT yearly traffic-count exports. These provide AADT by segment and the mapping/milepost fields (`CORR_ID`, `SITE_ID`, `CORR_MP`, `CORR_ENDMP`) we use to match crashes and normalize crash counts by traffic and miles driven. This includes the original data from MDT as well as a `.csv` file generated from the supplied `.dbf` for easier python processing.
- `Montana_On_System_Routes_OD.csv` — MDT's on-system route lookup used to map internal departmental route IDs to the public highway name (`SIGNED_ROUTE` / `ROUTE_NAME`) shown to readers.
- `manual-route-segments.csv` — a human-edited CSV that defines origin/destination segment boundaries for each highway trip we report on (cities, start/end CORR_MP values). This file is edited during the review step and drives the trip-building script.
- GeoJSONs and helper files (e.g. `mt-interstates.geojson`, `crash-geojson/`) — used for mapping, manual inspection, and visual QA during the review process.

## Data pipeline order and outputs

1) `merge_traffic.py`
	- Takes in TYC traffic data from `data/Traffic_Yearly_Counts_{year}/` (for example `TYC_2023.csv`) and crash data (`2019-2023-crash-data.csv`) and uses `Montana_On_System_Routes.csv` to map internal MDT `DEPT_ID` values to the highway names shown on road signs. It uses corridor IDs and mileposts from the traffic files to define road segments, matches crashes to the segment whose milepost range contains them, prefers 2023 data and only uses previous years to fill gaps, and averages AADT only across exact segment matches (SITE_ID/DEPT_ID/CORR_MP/CORR_ENDMP).
	- Key outputs include merged segment-level data with crash totals and rates (for example `processed-data/merged-data/merged_traffic_average.csv`) and per-route CSV and GeoJSON files for later mapping and analysis in the `all-roads` directory.

2) `analyze_data.py`
	- Takes the merged traffic+crash file (`merged_traffic_average.csv`), filters out unsigned and very low-volume roads, and ranks the remaining segments by crash risk using two metrics: vehicles per accident and miles per accident.
	- Outputs include CSVs with the top-100 ranked segments by crashes-per-daily-traffic and by crashes-per-mile, per-department CSVs for DEPT_IDs that appear in those lists, and GeoJSON used for visual inspection and debugging.
	- `manual-route-segments.csv` generated from top 100 segments by crashes

3) `manually_segment_routes.py`
	- Takes the manual segmentation table (`manual-route-segments.csv`) that was generated by segmenting up the roads that were at the top of the `by_car` output of `analyze_data.py`, along with route-level CSVs and GeoJSONs from `all-roads` produced by `merge_traffic.py`, and uses the defined milepost ranges to select constituent TYC rows and features.
	- Outputs one CSV and one GeoJSON per manual segment, plus combined files (`all-routes.csv` and `all-routes.geojson`) which are primarily used for debugging.

4) `analyze_trips.py`
	- Reads the per-segment CSV and GeoJSON files created in `manual-segments/`, aggregates traffic (and then does segment length weighted averaging), length, and crash data for each manual segment, and computes crash-rate metrics (vehicles per accident, miles per accident, crashes per 100M vehicle-miles traveled).
	- Outputs ranked CSV summaries into `trip_analysis/` for each metric and GeoJSON bundles of the top segments for mapping; `per_vmt.csv` and the `per_vmt/` GeoJSONs are used as the preferred source of truth since crashes-per-VMT reduces noise and follows industry practice.


## Limitations, caveats, and design choices

- Segment geometry changes: when segments split or merge across years, the strict exact-match averaging approach cannot use traffic values from non-matching years even if they physically cover the same stretch. This avoids mixing non-comparable segment definitions.
- Manual trip boundaries: trips were defined by hand for the top candidates; this step required human judgment and relies on the 2024 traffic map being consistent with the 2023 segment definitions used in the analysis.
- Crash geolocation quality: crash mileposts / reference points can be imprecise — matching uses the available corridor/milepost data from MDT data.
- Site ID collisions: some `SITE_ID` values are reused or not unique; matching uses a combination of identifiers to avoid accidental aggregation.


###### _Note: crash data used is incomplete and doesn't contain crash severity. MDT uses other factors to determine what roads to focus resources on to improve safety. MTFP has submitted a records request for more data_