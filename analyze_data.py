import os
import pandas as pd
import geopandas as gpd

def get_script_directory():
    return os.path.dirname(os.path.abspath(__file__))

def setup_output_directories():
    script_dir = get_script_directory()
    base_output_dir = os.path.join(script_dir, 'data_analysis')
    by_car_dir = os.path.join(base_output_dir, 'by_car')
    by_mileage_dir = os.path.join(base_output_dir, 'by_milage')
    os.makedirs(base_output_dir, exist_ok=True)
    os.makedirs(by_car_dir, exist_ok=True)
    os.makedirs(by_mileage_dir, exist_ok=True)
    return base_output_dir, by_car_dir, by_mileage_dir

def natural_sort_site_id(site_id):
    import re
    parts = re.split(r'(\d+)', str(site_id))
    return [int(part) if part.isdigit() else part for part in parts]

def analyze_traffic_data():
    TYC_AADT_CUTOFF = 1000 
    TOP_N_ENTRIES = 100

    script_dir = get_script_directory()
    base_output_dir, by_car_dir, by_mileage_dir = setup_output_directories()
    input_file = os.path.join(script_dir, 'processed-data', 'merged-data', 'merged_traffic_average.csv')

    try:
        df = pd.read_csv(input_file)
        # print(f"Loaded {len(df)} records from {input_file}")
            
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # exclude routes without a SIGNED_ROUTE value
    if 'SIGNED_ROUTE' in df.columns:
        before_count = len(df)
        
        
        df = df[df['SIGNED_ROUTE'].notna() & df['SIGNED_ROUTE'].astype(str).str.strip().ne('')].copy()
        removed = before_count - len(df)
        print(f"Excluded {removed} records without SIGNED_ROUTE; {len(df)} records remain")
        
        
        if len(df) == 0:
            print("No records remain after excluding missing SIGNED_ROUTE")
            return
    else:
        print("Warning: 'SIGNED_ROUTE' column not found — proceeding without SIGNED_ROUTE-based exclusion")

     # 1. filter
    filtered_df = df[df['TYC_AADT'] > TYC_AADT_CUTOFF].copy()
    print(f"After filtering for TYC_AADT > {TYC_AADT_CUTOFF}: {len(filtered_df)} records")
    
    if len(filtered_df) == 0:
        print("No records meet the TYC_AADT criteria")
        return

    # 2. sort and select top N entries
    cars_sorted = filtered_df.sort_values('CARS_PER_ACC', ascending=True).copy()
    
    # only include sections over 1 mile for miles/accident to avoid skewed results
    miles_filtered = filtered_df[filtered_df['SEC_LNT_MI'] > 1].copy()
    print(f"After filtering for SEC_LNT_MI > 1: {len(miles_filtered)} records")
    miles_sorted = miles_filtered.sort_values('MILES_PER_ACC', ascending=True).copy()
    top_cars = cars_sorted.head(TOP_N_ENTRIES).copy()
    top_miles = miles_sorted.head(TOP_N_ENTRIES).copy()

    # 3. write CSV outputs
    cars_output_file = os.path.join(base_output_dir, 'lowest_cars_per_accident.csv')
    miles_output_file = os.path.join(base_output_dir, 'lowest_miles_per_accident.csv')
    top_cars.to_csv(cars_output_file, index=False)
    top_miles.to_csv(miles_output_file, index=False)
    # print(f"Created cars per accident analysis: {cars_output_file} (top {TOP_N_ENTRIES} entries)")
    # print(f"Created miles per accident analysis: {miles_output_file} (top {TOP_N_ENTRIES} entries)")

    # 4. write by_car and by_mileage outputs (optional, as before)
    for dept_id in set(top_cars['DEPT_ID']):
        dept_records = filtered_df[filtered_df['DEPT_ID'] == dept_id].copy()
        dept_records['site_id_sort_key'] = dept_records['SITE_ID'].apply(natural_sort_site_id)
        dept_records = dept_records.sort_values('site_id_sort_key').drop('site_id_sort_key', axis=1)
        output_file = os.path.join(by_car_dir, f"{dept_id}.csv")
        dept_records.to_csv(output_file, index=False)
        # print(f"  Created {output_file} with {len(dept_records)} records")

    for dept_id in set(top_miles['DEPT_ID']):
        dept_records = filtered_df[filtered_df['DEPT_ID'] == dept_id].copy()
        dept_records['site_id_sort_key'] = dept_records['SITE_ID'].apply(natural_sort_site_id)
        output_file = os.path.join(by_mileage_dir, f"{dept_id}.csv")
        dept_records.to_csv(output_file, index=False)
        # print(f"  Created {output_file} with {len(dept_records)} records")

    # 5. GeoJSON output
    def export_top_routes_geojson(top_cars, filtered_df, script_dir, TOP_N_ENTRIES):
        years = [2023, 2022, 2021, 2020, 2019]
        found_features = []
        missing_pairs = set(zip(top_cars['DEPT_ID'], top_cars['SITE_ID']))
        highlight_pairs = set(missing_pairs)  # Save for highlight column

        for year in years:
            if not missing_pairs:
                break
            shp_dir = os.path.join(script_dir, 'data', f'Traffic_Yearly_Counts_{year}')
            shp_path = os.path.join(shp_dir, f'TYC_{year}.shp')
            if not os.path.exists(shp_path):
                continue
            gdf = gpd.read_file(shp_path)
            gdf['DEPT_ID'] = gdf['DEPT_ID'].astype(str).str.strip()
            gdf['SITE_ID'] = gdf['SITE_ID'].astype(str).str.strip()
            gdf = gdf[gdf.apply(lambda row: (row['DEPT_ID'], row['SITE_ID']) in missing_pairs, axis=1)]
            found_features.append(gdf)
            found_pairs = set(zip(gdf['DEPT_ID'], gdf['SITE_ID']))
            missing_pairs -= found_pairs

        # combine all found features
        if found_features:
            all_found_gdf = pd.concat(found_features, ignore_index=True)
        else:
            # print("No matching features found in any year.")
            return

        # highlight column
        all_found_gdf['highlight'] = all_found_gdf.apply(
            lambda row: 'Y' if (row['DEPT_ID'], row['SITE_ID']) in highlight_pairs else 'N', axis=1
        )

        # reproject to WGS84 for GeoJSON/web mapping compatibility
        all_found_gdf = all_found_gdf.to_crs("EPSG:4326")

        output_geojson = os.path.join(script_dir, 'data_analysis', f'top_{TOP_N_ENTRIES}_by_cars.geojson')
        simple_geojson = os.path.join(script_dir, 'data_analysis', 'simple_by_cars.geojson')

        all_found_gdf.to_file(output_geojson, driver='GeoJSON')
        # print(f"GeoJSON with highlights written to: {output_geojson}")

        # only keep features that are in the top N pairs (highlight_pairs)
        top_pairs = set(zip(top_cars['DEPT_ID'].astype(str).str.strip(), top_cars['SITE_ID'].astype(str).str.strip()))
        simple_gdf = all_found_gdf[
            all_found_gdf.apply(lambda row: (str(row['DEPT_ID']).strip(), str(row['SITE_ID']).strip()) in top_pairs, axis=1)
        ]

        # emove duplicates
        simple_gdf = simple_gdf.drop_duplicates(subset=['DEPT_ID', 'SITE_ID'])

        # only keep the top N — should already be N, but just in case™
        simple_gdf = simple_gdf.head(TOP_N_ENTRIES)

        simple_gdf.to_file(simple_geojson, driver='GeoJSON')
        # print(f"GeoJSON with only highlight:Y written to: {simple_geojson}")

        if missing_pairs:
            print("Still missing after all years:", missing_pairs)

    export_top_routes_geojson(top_cars, filtered_df, script_dir, TOP_N_ENTRIES)

    # show sample of top entries
    print("\nTop 5 entries by CARS_PER_ACC:")
    print(top_cars[['SITE_ID', 'DEPT_ID', 'TYC_AADT', 'TOTAL_CRASHES', 'AVG_CRASHES', 'CARS_PER_ACC']].head().to_string(index=False))
    print("\nTop 5 entries by MILES_PER_ACC:")
    print(top_miles[['SITE_ID', 'DEPT_ID', 'TYC_AADT', 'TOTAL_CRASHES', 'AVG_CRASHES', 'MILES_PER_ACC']].head().to_string(index=False))

if __name__ == "__main__":
    analyze_traffic_data()