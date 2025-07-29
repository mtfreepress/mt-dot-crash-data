import json
from pathlib import Path

# List the years you want to process
years = [2019, 2020]  # Add or remove years as needed

# Always use the script's directory as the base
script_dir = Path(__file__).parent

for year in years:
    input_path = script_dir / f"data/Traffic_Yearly_Counts_{year}/TYC_{year}.geojson"
    output_path = script_dir / f"data/Traffic_Yearly_Counts_{year}/TYC_{year}_formatted.geojson"
    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        continue
    with input_path.open("r", encoding="utf-8") as infile:
        data = json.load(infile)
    with output_path.open("w", encoding="utf-8") as outfile:
        json.dump(data, outfile, indent=2, ensure_ascii=False)
    print(f"Formatted GeoJSON written to {output_path.resolve()}")