import pandas as pd
import json
import os
from pathlib import Path

def parse_milepost(mp_str):
    """Convert milepost string like '062+0.153' to float"""
    if pd.isna(mp_str) or mp_str == '':
        return None
    parts = str(mp_str).split('+')
    if len(parts) == 2:
        return float(parts[0]) + float(parts[1])
    return float(mp_str)

def format_city_name(city):
    """Format city name for filename (replace spaces with hyphens, handle special cases)"""
    return city.replace(' ', '-').replace('/', '-').replace('(', '').replace(')', '')

def main():
    # read the manual route segments file
    manual_segments = pd.read_csv('./data/manual-route-segments.csv')
    # normalize DEPT_ID
    if 'DEPT_ID' in manual_segments.columns:
        manual_segments['DEPT_ID'] = manual_segments['DEPT_ID'].astype(str).str.strip()
    
    # parse mileposts on the manual table so we can look up "Far Side of" rows
    manual_segments['CORR_MP_FLOAT'] = manual_segments['CORR_MP'].apply(parse_milepost)
    manual_segments['CORR_ENDMP_FLOAT'] = manual_segments['CORR_ENDMP'].apply(parse_milepost)
    
    # check if there are any issues with the CSV reading
    if 'CITY' not in manual_segments.columns:
        print("ERROR: 'CITY' column not found in the CSV file!")
        print("Please check the CSV file format and ensure it has the correct headers.")
        return
    
    # output directory
    output_dir = Path('./manual-segments')
    output_dir.mkdir(exist_ok=True)
    
    # initialize containers for combined data
    all_csv_data = []
    all_geojson_features = []
    
    # optional aliases: map a displayed Route to additional DEPT_IDs that is considered part of the route by MDT
    ALIASES = {
        'S-210': ['U-8133'],
        'S-518': ['U-5832'],
        # MDT considers final segment part of S-345
        'S-345': ['U-1216'],
        'S-347': ['U-602']
    }

    # group by route AND DEPT_ID (since same route can have different dept IDs)
    route_groups = manual_segments.groupby(['Route', 'DEPT_ID'])
    
    for (route, dept_id), route_manual in route_groups:
        # print(f"Processing route: {route} ({dept_id})")
        
        # sort by milepost
        route_manual = route_manual.sort_values('CORR_MP').copy()
        
        # read the complete route data
        try:
            route_csv_path = f'./all_roads/{route}.csv'
            route_geojson_path = f'./all_roads/{route}.geojson'
            
            if not os.path.exists(route_csv_path):
                print(f"Warning: {route_csv_path} not found, skipping route {route}")
                continue
                
            route_df = pd.read_csv(route_csv_path)

            # normalize DEPT_ID in route CSV
            if 'DEPT_ID' in route_df.columns:
                route_df['DEPT_ID'] = route_df['DEPT_ID'].astype(str).str.strip()

            # filter route_df to only include segments matching this DEPT_ID
            route_df_orig = route_df.copy()

            # allowed DEPT_IDs set: primary DEPT_ID plus any aliases for the route
            allowed_dept_ids = {dept_id}
            for alias in ALIASES.get(route, []):
                allowed_dept_ids.add(alias)

            # filter route_df to rows whose DEPT_ID is in allowed_dept_ids
            route_df = route_df[route_df['DEPT_ID'].isin(allowed_dept_ids)].copy()

            if route_df.empty:
                # DEPT_ID not found - try to find matching rows by SITE_ID from the manual table
                candidate_sites = route_manual['SITE_ID'].dropna().unique()
                matches = route_df_orig[route_df_orig['SITE_ID'].isin(candidate_sites)].copy()
                if not matches.empty:
                    print(f"Note: DEPT_ID '{dept_id}' not present in {route}.csv; using rows matched by SITE_ID ({len(matches)} rows)")
                    route_df = matches
                else:
                    # As a last-resort fallback, use all rows for this route (different DEPT_ID may be used in CSV)
                    print(f"Warning: DEPT_ID '{dept_id}' not found in {route}.csv and no SITE_ID matches; falling back to all DEPT_IDs for route {route}")
                    route_df = route_df_orig
            
            # parse mileposts for easier comparison
            route_df['CORR_MP_FLOAT'] = route_df['CORR_MP'].apply(parse_milepost)
            route_df['CORR_ENDMP_FLOAT'] = route_df['CORR_ENDMP'].apply(parse_milepost)
            route_manual['CORR_MP_FLOAT'] = route_manual['CORR_MP'].apply(parse_milepost)
            route_manual['CORR_ENDMP_FLOAT'] = route_manual['CORR_ENDMP'].apply(parse_milepost)
            
            # load GeoJSON
            with open(route_geojson_path, 'r') as f:
                route_geojson = json.load(f)
            
        except Exception as e:
            print(f"Error reading route data for {route}: {e}")
            continue
        
        # process each segment pair
        for i in range(len(route_manual)):
            current_row = route_manual.iloc[i]
            
            # Handle "Use Entire Route"
            if 'Use Entire Route' in str(current_row['CITY']):
                print(f"  Using entire route for {route} ({dept_id})")
                
                # sort data
                sorted_df = route_df.sort_values(['DEPT_ID', 'CORR_MP_FLOAT'])
                
                # add segment name column
                sorted_df_with_segment = sorted_df.copy()
                sorted_df_with_segment['SEGMENT_NAME'] = f"{route}_{dept_id}_ENTIRE_ROUTE"
                
                # add to combined data
                all_csv_data.append(sorted_df_with_segment.drop(['CORR_MP_FLOAT', 'CORR_ENDMP_FLOAT'], axis=1))
                
                # add features to combined GeoJSON with segment name. Handle aliases
                for feature in route_geojson['features']:
                    if feature['properties'].get('DEPT_ID') in allowed_dept_ids:
                        feature_copy = feature.copy()
                        feature_copy['properties']['SEGMENT_NAME'] = f"{route}_{dept_id}_ENTIRE_ROUTE"
                        # annotate which DEPT_ID was used
                        feature_copy['properties']['SOURCE_DEPT_ID'] = feature['properties'].get('DEPT_ID')
                        all_geojson_features.append(feature_copy)
                
                # create filename with DEPT_ID
                filename = f"{format_city_name(route)}_{dept_id}"
                
                # save CSV
                csv_output = sorted_df.drop(['CORR_MP_FLOAT', 'CORR_ENDMP_FLOAT'], axis=1)
                csv_output.to_csv(output_dir / f'{filename}.csv', index=False)
                
                # filter GeoJSON to only this DEPT_ID or its aliases
                filtered_geojson = {
                    "type": "FeatureCollection", 
                    "features": [f for f in route_geojson['features'] if f['properties'].get('DEPT_ID') in allowed_dept_ids]
                }
                
                # save GeoJSON  
                with open(output_dir / f'{filename}.geojson', 'w') as f:
                    json.dump(filtered_geojson, f, indent=2)
                
                break  # Only process once for "Use Entire Route"
            
            # skip "Far Side of" entries as starting points
            if current_row['CITY'].startswith('Far Side of'):
                continue
                
            # find the next city (handling "Far Side of" entries)
            next_city_idx = i + 1
            while (next_city_idx < len(route_manual) and 
                   route_manual.iloc[next_city_idx]['CITY'].startswith('Far Side of')):
                next_city_idx += 1
            
            if next_city_idx >= len(route_manual):
                continue
                
            next_row = route_manual.iloc[next_city_idx]
            
            # print(f"  Processing segment: {current_row['CITY']} to {next_row['CITY']}")
            
            # determine start point: prefer a global "Far Side of <city>" row for this route
            far_city_name = f"Far Side of {current_row['CITY']}"

            # look for a global Far Side row (may be under a different DEPT_ID)
            global_far = manual_segments[
                (manual_segments['Route'] == route) &
                (manual_segments['CITY'] == far_city_name)
            ]

            if not global_far.empty:
                ref_mp = None
                try:
                    ref_mp = current_row['CORR_ENDMP_FLOAT'] if pd.notna(current_row['CORR_ENDMP_FLOAT']) else current_row['CORR_MP_FLOAT']
                except Exception:
                    ref_mp = current_row.get('CORR_MP_FLOAT', None)

                candidates = global_far[global_far['CORR_MP_FLOAT'].notnull()].copy()
                if not candidates.empty:
                    if ref_mp is not None:
                        downstream = candidates[candidates['CORR_MP_FLOAT'] > ref_mp]
                        if not downstream.empty:
                            chosen = downstream.sort_values('CORR_MP_FLOAT').iloc[0]
                        else:
                            chosen = candidates.sort_values('CORR_MP_FLOAT').iloc[0]
                    else:
                        chosen = candidates.sort_values('CORR_MP_FLOAT').iloc[0]

                    start_milepost = chosen['CORR_MP_FLOAT']
                    print(f"    Starting from global {far_city_name} (DEPT_ID={chosen.get('DEPT_ID')}): {start_milepost}")
                else:
                    global_far = pd.DataFrame()

            if global_far.empty:
                far_side_current_idx = i + 1
                if (far_side_current_idx < len(route_manual) and 
                    route_manual.iloc[far_side_current_idx]['CITY'] == far_city_name):
                    # start from the "Far Side of" current city (inclusive)
                    start_milepost = route_manual.iloc[far_side_current_idx]['CORR_MP_FLOAT']
                    print(f"    Starting from local Far Side of {current_row['CITY']}: {start_milepost}")
                else:
                    # no "Far Side of" - check if this is the first segment or a continuation
                    if i == 0:
                        # start from the beginning of current city
                        start_milepost = current_row['CORR_MP_FLOAT']
                        print(f"    Starting from beginning of {current_row['CITY']}: {start_milepost}")
                    else:
                        # start from the end of current city
                        start_milepost = current_row['CORR_ENDMP_FLOAT']
                        print(f"    Starting from end of {current_row['CITY']}: {start_milepost}")
            
            # determine end 
            end_milepost = next_row['CORR_ENDMP_FLOAT']
            print(f"    Ending after {next_row['CITY']}: {end_milepost}")
            
            # get all segments in this range using strict interior overlap (except for single point - ie where two segments meet)
            eps = 1e-6
            if start_milepost is None:
                start_milepost = 0
            if end_milepost is None:
                # no end specified, include segments whose end is after start
                segments = route_df[route_df['CORR_ENDMP_FLOAT'] > start_milepost + eps]
            else:
                # include segments with non-zero overlap: segment.start < manual_end AND segment.end > manual_start
                segments = route_df[
                    (route_df['CORR_MP_FLOAT'] < end_milepost - eps) & 
                    (route_df['CORR_ENDMP_FLOAT'] > start_milepost + eps)
                ]

            # try:
            #     if 'TOTAL_CRASHES' in route_df.columns:
            #         total_src = pd.to_numeric(route_df['TOTAL_CRASHES'], errors='coerce').fillna(0).sum()
            #         print(f"    source TOTAL_CRASHES sum for {route}/{dept_id}: {total_src}")
            #     if 'TOTAL_CRASHES' in segments.columns:
            #         total_sel = pd.to_numeric(segments['TOTAL_CRASHES'], errors='coerce').fillna(0).sum()
            #         print(f"    selected TOTAL_CRASHES sum for segment {current_row['CITY']}->{next_row['CITY']}: {total_sel} (rows: {len(segments)})")
            # except Exception:
            #     pass
            
            # # Debug: Check for the A-015 case specifically for MT-200
            # if route == 'MT-200' and start_milepost <= 2.564 <= end_milepost:
            #     print(f"    DEBUG: Looking for A-015 segment in range {start_milepost} to {end_milepost}")
            #     a015_segments = route_df[route_df['SITE_ID'] == 'A-015']
            #     if len(a015_segments) > 0:
            #         print(f"    DEBUG: Found A-015 segment: MP {a015_segments.iloc[0]['CORR_MP_FLOAT']} to {a015_segments.iloc[0]['CORR_ENDMP_FLOAT']}")
            #         print(f"    DEBUG: A-015 included in filter: {len(segments[segments['SITE_ID'] == 'A-015']) > 0}")
            #     else:
            #         print("    DEBUG: A-015 not found in route data")

            #     # Show all segments in this range
            #     print("    DEBUG: All segments in range:")
            #     for _, seg in segments.iterrows():
            #         print(f"      {seg['SITE_ID']}: {seg['CORR_MP_FLOAT']} to {seg['CORR_ENDMP_FLOAT']}")
            
            if segments.empty:
                print(f"    Warning: No segments found between {current_row['CITY']} and {next_row['CITY']}")
                continue
            
            # sort segments
            segments = segments.sort_values(['DEPT_ID', 'CORR_MP_FLOAT'])
            
            # create segment name for debugging
            start_city = format_city_name(current_row['CITY'])
            end_city = format_city_name(next_row['CITY'])
            filename = f"{route}_{dept_id}_{start_city}_{end_city}"
            segment_name = f"{route}_{dept_id}_{filename}"
            
            # add segment name column for debugging
            segments_with_name = segments.copy()
            segments_with_name['SEGMENT_NAME'] = segment_name
            
            # add to combined data
            all_csv_data.append(segments_with_name.drop(['CORR_MP_FLOAT', 'CORR_ENDMP_FLOAT'], axis=1))
            
            # save CSV
            csv_output = segments.drop(['CORR_MP_FLOAT', 'CORR_ENDMP_FLOAT'], axis=1)
            csv_output.to_csv(output_dir / f'{filename}.csv', index=False)

            filtered_features = []
            selected_site_ids = set(segments['SITE_ID'].dropna().astype(str).unique())
            # seg_start = start_milepost if start_milepost is not None else 0
            # seg_end = end_milepost

            for feature in route_geojson['features']:
                # Only include features that match one of the SITE_IDs and DEPT_ID
                feat_site = feature['properties'].get('SITE_ID')
                feat_dept = feature['properties'].get('DEPT_ID')
                if feat_dept not in allowed_dept_ids:
                    continue
                if str(feat_site) not in selected_site_ids:
                    continue

                feature_copy = feature.copy()
                feature_copy['properties']['SEGMENT_NAME'] = segment_name
                feature_copy['properties']['SOURCE_DEPT_ID'] = feat_dept
                filtered_features.append(feature_copy)
                all_geojson_features.append(feature_copy)
            
            # create GeoJSON
            filtered_geojson = {
                "type": "FeatureCollection",
                "name": f"{route}_{filename}",
                "crs": route_geojson.get("crs", {}),
                "features": filtered_features
            }
            
            # save GeoJSON
            with open(output_dir / f'{filename}.geojson', 'w') as f:
                json.dump(filtered_geojson, f, indent=2)
            
            # print(f"    Created {filename}.csv with {len(segments)} segments")
            # print(f"    Created {filename}.geojson with {len(filtered_features)} features")
            # print(f"    Segment range: {start_milepost} to {end_milepost}")
    
    # Create combined files
    if all_csv_data:
        # print("\nCreating combined files...")
        
        # Combine all CSV data and write to a separate all-routes/ directory
        combined_csv = pd.concat(all_csv_data, ignore_index=True)
        combined_csv = combined_csv.sort_values(['DEPT_ID', 'CORR_MP'])
        all_routes_dir = Path('./manual-segments/all-routes')
        all_routes_dir.mkdir(exist_ok=True)
        combined_csv.to_csv(all_routes_dir / 'all-routes.csv', index=False)
        # print(f"Created all-routes/all-routes.csv with {len(combined_csv)} total segments")
        
        # Create combined GeoJSON and write it to all-routes/
        combined_geojson = {
            "type": "FeatureCollection",
            "name": "all-routes",
            "crs": {
                "type": "name",
                "properties": {
                    "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
                }
            },
            "features": all_geojson_features
        }
        
        with open(all_routes_dir / 'all-routes.geojson', 'w') as f:
            json.dump(combined_geojson, f, indent=2)
        # print(f"Created all-routes/all-routes.geojson with {len(all_geojson_features)} total features")

if __name__ == "__main__":
    main()