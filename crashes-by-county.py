import os
import csv
from collections import defaultdict

INPUT_FILE = './data/2019-2023-crash-data.csv'
OUTPUT_DIR = 'county-level'
BY_COUNTY_DIR = os.path.join(OUTPUT_DIR, 'crashes-by-county')

# Ensure output directories exist
os.makedirs(BY_COUNTY_DIR, exist_ok=True)

# Read the data
with open(INPUT_FILE, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    rows = list(reader)
    fieldnames = reader.fieldnames

# Count crashes per county and group rows by county (all years)
county_counts = defaultdict(int)
county_rows = defaultdict(list)

for row in rows:
    county = row['COUNTY'].strip() if row['COUNTY'].strip() else 'UNKNOWN'
    county_counts[county] += 1
    county_rows[county].append(row)

# Write crashes-per-county.csv (sorted descending by crash count)
with open(os.path.join(OUTPUT_DIR, 'crashes-per-county.csv'), 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['COUNTY', 'CRASH_COUNT'])
    for county, count in sorted(county_counts.items(), key=lambda x: x[1], reverse=True):
        writer.writerow([county, count])

# Write one CSV per county (all years)
for county, rows_ in county_rows.items():
    safe_county = county.replace(' ', '_').replace('/', '_')
    if safe_county == '':
        safe_county = 'UNKNOWN'
    out_path = os.path.join(BY_COUNTY_DIR, f'{safe_county}.csv')
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_)

# --- Per-year outputs ---
years = sorted(set(row['CRASH_YEAR'] for row in rows if row['CRASH_YEAR'].strip()))

for year in years:
    # Per-year crash count per county
    year_counts = defaultdict(int)
    year_rows = defaultdict(list)
    for row in rows:
        if row['CRASH_YEAR'].strip() == year:
            county = row['COUNTY'].strip() if row['COUNTY'].strip() else 'UNKNOWN'
            year_counts[county] += 1
            year_rows[county].append(row)

    # Write crashes-per-county-{YEAR}.csv
    with open(os.path.join(OUTPUT_DIR, f'crashes-per-county-{year}.csv'), 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['COUNTY', 'CRASH_COUNT'])
        for county, count in sorted(year_counts.items(), key=lambda x: x[1], reverse=True):
            writer.writerow([county, count])

    # Write one CSV per county for this year
    by_county_year_dir = os.path.join(OUTPUT_DIR, f'crashes-by-county-{year}')
    os.makedirs(by_county_year_dir, exist_ok=True)
    for county, rows_ in year_rows.items():
        safe_county = county.replace(' ', '_').replace('/', '_')
        if safe_county == '':
            safe_county = 'UNKNOWN'
        out_path = os.path.join(by_county_year_dir, f'{safe_county}.csv')
        with open(out_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows_)

print("Done! Check the 'county-level' directory.")