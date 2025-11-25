#!/bin/bash
input_file="output/merged_data/merged_traffic_lines.geojson"
base=$(basename "$input_file" .geojson)

for scale in 1000 100 10 1; do
    mapshaper "$input_file" \
        -simplify keep-shapes interval=${scale} \
        -o gj2008 precision=0.00001 "data/processed/${base}-${scale}m.geojson"
done
