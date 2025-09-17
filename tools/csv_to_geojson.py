import os
import pandas as pd
import geojson
from pyproj import Transformer
import math

CSV_FILENAME = '2019-2023-crash-data.csv'
YEARS = [2019, 2020, 2021, 2022, 2023]
LAT_COL = 'LATITUDE'
LON_COL = 'LONGITUDE'
X_COL = 'SMS_X_CORD'
Y_COL = 'SMS_Y_CORD'
YEAR_COL = 'CRASH_YEAR'

# *Output is not used* in pipeline, but here for reference. May be useful for future work.

# NAD83 / UTM zone 12N to WGS84 (lat/lon)
transformer = Transformer.from_crs("EPSG:26912", "EPSG:4326", always_xy=True)

def get_lat_lon(row):
    # try lat/lon first
    try:
        lat = float(row[LAT_COL])
        lon = float(row[LON_COL])
        if not math.isnan(lat) and not math.isnan(lon) and lat != 0 and lon != 0:
            return lat, lon
    except Exception:
        pass
    # try SMS_X_CORD/SMS_Y_CORD
    try:
        x = float(row[X_COL])
        y = float(row[Y_COL])
        if not math.isnan(x) and not math.isnan(y) and x != 0 and y != 0:
            lon, lat = transformer.transform(x, y)
            return lat, lon
    except Exception:
        pass
    return None, None

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, '../data')
    csv_path = os.path.join(data_dir, CSV_FILENAME)
    out_dir = os.path.join(data_dir, 'crash-geojson')
    os.makedirs(out_dir, exist_ok=True)

    df = pd.read_csv(csv_path)
    no_location_rows = []

    # function to create a GeoJSON Feature from a row
    def row_to_feature(row):
        lat, lon = get_lat_lon(row)
        if lat is None or lon is None:
            return None
        props = row.to_dict()
        for k, v in props.items():
            if isinstance(v, float) and math.isnan(v):
                props[k] = None
        point = geojson.Point((lon, lat))
        return geojson.Feature(geometry=point, properties=props)

    features = []
    features_by_year = {year: [] for year in YEARS}
    for idx, row in df.iterrows():
        feat = row_to_feature(row)
        if feat:
            features.append(feat)
            try:
                crash_year = int(row[YEAR_COL])
            except Exception:
                crash_year = None
            if crash_year in YEARS:
                features_by_year[crash_year].append(feat)
        else:
            no_location_rows.append(row)

    feature_collection = geojson.FeatureCollection(features)
    all_out_path = os.path.join(out_dir, 'crash-geojson-all.geojson')
    with open(all_out_path, 'w') as f:
        geojson.dump(feature_collection, f, indent=2)
    print("All crashes GeoJSON written to {}".format(all_out_path))

    # By year ---
    for year in YEARS:
        year_features = features_by_year[year]
        year_collection = geojson.FeatureCollection(year_features)
        year_out_path = os.path.join(out_dir, f'crash-geojson-{year}.geojson')
        with open(year_out_path, 'w') as f:
            geojson.dump(year_collection, f, indent=2)
        print(f"{year} crashes GeoJSON written to {year_out_path}")

    # Write no-location rows to CSV ---
    if no_location_rows:
        no_loc_path = os.path.join(out_dir, 'crash-data-no-location.csv')
        pd.DataFrame(no_location_rows).to_csv(no_loc_path, index=False)
        print(f"Wrote {len(no_location_rows)} rows with no location to {no_loc_path}")

if __name__ == '__main__':
    main()