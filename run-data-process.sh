#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

# pick sha256 tool depending on system
if command -v sha256sum >/dev/null 2>&1; then
  SHA256="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
  SHA256="shasum -a 256"
else
  echo "Warning: no sha256 tool found (need sha256sum or shasum). Skipping file comparison check."
  SHA256=""
fi

# activate venv if present
if [ -f "$ROOT_DIR/.venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.venv/bin/activate"
  echo "Activated virtualenv .venv"
fi

python merge_traffic.py
python analyze_data.py
python manually_segment_routes.py
python analyze_trips.py

echo "Pipeline complete. Check output/data_analysis/, output/merged_data/, output/all_roads/, output/manual-segments/, and output/trip_analysis/ for outputs."

# only run comparison if sha256 tool available
if [ -n "$SHA256" ]; then
  # files to compare
  csv1="mtfp_result_data/trip_analysis/per_vmt.csv"
  csv2="output/trip_analysis/per_vmt.csv"
  json1="mtfp_result_data/trip_analysis/per_vmt/top_50.json"
  json2="output/trip_analysis/per_vmt/top_50.json"

  # compare hashes
  csv_match=false
  json_match=false

  hash_csv1=$($SHA256 "$csv1" | awk '{print $1}')
  hash_csv2=$($SHA256 "$csv2" | awk '{print $1}')
  if [ "$hash_csv1" = "$hash_csv2" ]; then
    csv_match=true
  fi

  hash_json1=$($SHA256 "$json1" | awk '{print $1}')
  hash_json2=$($SHA256 "$json2" | awk '{print $1}')
  if [ "$hash_json1" = "$hash_json2" ]; then
    json_match=true
  fi

  # report results
  if [ "$csv_match" = true ] && [ "$json_match" = true ]; then
    echo "✅ GeoJSON and CSV files match expected output!"
  elif [ "$csv_match" = true ] && [ "$json_match" = false ]; then
    echo "❌ GeoJSON does not match expected output!"
  elif [ "$csv_match" = false ] && [ "$json_match" = true ]; then
    echo "❌ CSV does not match expected output!"
  else
    echo "❌ GeoJSON and CSV do not match! Check input files in data directory."
  fi
fi
