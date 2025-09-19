import os
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

def parse_milepost(mp_str):
    if pd.isna(mp_str):
        return None
    parts = str(mp_str).split('+')
    if len(parts) != 2:
        return None
    try:
        # handle leading zeros, e.g. "002+0.619" -> 2.619
        return float(parts[0].lstrip('0') or '0') + float(parts[1])
    except ValueError:
        return None

def load_on_system_routes(path='raw-mdt-source-data/Montana_On_System_Routes_OD.csv'):
    """Load on-system routes CSV for metadata enrichment"""
    if not os.path.exists(path):
        # print(f"[DEBUG] On-system file not found: {path}")
        return None
    df = pd.read_csv(path, dtype=str)
    # print(f"[DEBUG] Successfully loaded on-system file: {path} with {len(df)} rows")
    df.columns = [c.strip() for c in df.columns]
    # normalize important columns
    for col in ['BEGIN REFERENCE POINT', 'END REFERENCE POINT']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round(3)
    if 'DEPARTMENTAL ROUTE' in df.columns:
        df['DEPARTMENTAL ROUTE'] = df['DEPARTMENTAL ROUTE'].astype(str).str.strip().str.upper()
        df['DEPT_KEY'] = df['DEPARTMENTAL ROUTE'].str.replace(r'([A-Z])+$', '', regex=True)
    return df

def enrich_segments_with_metadata(segments_df, on_system_df):
    """Add on-system metadata to segments"""
    if on_system_df is None or on_system_df.empty:
        # print("[DEBUG] No on-system data available for enrichment")
        # check columns exist
        for col in ['ROUTE_NAME', 'SIGNED_ROUTE', 'SYSTEM']:
            if col not in segments_df.columns:
                segments_df[col] = None
        return segments_df
    
    # check columns exist
    for col in ['ROUTE_NAME', 'SIGNED_ROUTE', 'SYSTEM']:
        if col not in segments_df.columns:
            segments_df[col] = None
    
    # create route lookup
    grouped_by_route = {}
    for _, r in on_system_df.iterrows():
        route_id = str(r.get('ROUTE ID', '')).strip().upper()
        route_base = route_id[:-1] if route_id and route_id[-1].isalpha() else route_id
        grouped_by_route.setdefault(route_base, []).append(r)
    
    # print(f"[DEBUG] Enriching {len(segments_df)} segments with on-system metadata")
    
    def find_metadata_match(row):
        # match by CORR_ID or DEPT_ID
        corr_id = str(row.get('CORR_ID', '')).strip().upper()
        dept_id = str(row.get('DEPT_ID', '')).strip().upper()
        
        candidates = grouped_by_route.get(corr_id) or grouped_by_route.get(dept_id)
        
        if not candidates:
            return None
        
        # use milepost matching
        start_mp = parse_milepost(row.get('CORR_MP'))
        end_mp = parse_milepost(row.get('CORR_ENDMP'))
        
        if pd.notna(start_mp) and pd.notna(end_mp):
            mid = (start_mp + end_mp) / 2.0
            
            for candidate in candidates:
                b = candidate.get('BEGIN REFERENCE POINT')
                e = candidate.get('END REFERENCE POINT')
                if pd.notna(b) and pd.notna(e):
                    if b <= mid <= e:
                        return candidate
        
        # return first candidate if no milepost match
        return candidates[0] if candidates else None
    
    matches = 0
    for idx, row in segments_df.iterrows():
        match = find_metadata_match(row)
        if match is not None:
            matches += 1
            segments_df.at[idx, 'ROUTE_NAME'] = match.get('ROUTE NAME') or match.get('ROUTE_NAME')
            segments_df.at[idx, 'SIGNED_ROUTE'] = match.get('SIGNED ROUTE') or match.get('SIGNED_ROUTE')
            segments_df.at[idx, 'SYSTEM'] = match.get('SYSTEM')
    
    # print(f"[DEBUG] Found {matches} metadata matches out of {len(segments_df)} segments")
    return segments_df

def load_base_segments_2023():
    """Load 2023 segments as the base for all road sections"""
    base_csv = 'data/Traffic_Yearly_Counts_2023/TYC_2023.csv'
    
    if not os.path.exists(base_csv):
        print(f"[ERROR] Base 2023 CSV not found: {base_csv}")
        return None
    
    # use 2023 traffic data as base
    segments_df = pd.read_csv(base_csv, dtype=str)
    
    # normalize key columns
    segments_df['CORR_ID'] = segments_df['CORR_ID'].astype(str).str.strip().str.upper()
    segments_df['DEPT_ID'] = segments_df['DEPT_ID'].astype(str).str.strip().str.upper()
    
    # create composite unique key to handle duplicate SITE_IDs
    segments_df['SEGMENT_KEY'] = (segments_df['CORR_ID'] + '_' + 
                                 segments_df['CORR_MP'] + '_' + 
                                 segments_df['CORR_ENDMP'] + '_' + 
                                 segments_df['DEPT_ID'])
    
    # add milepost floats
    segments_df['CORR_MP_FLOAT'] = segments_df['CORR_MP'].apply(parse_milepost)
    segments_df['CORR_ENDMP_FLOAT'] = segments_df['CORR_ENDMP'].apply(parse_milepost)
    
    #  on-system-routes
    on_system = load_on_system_routes('raw-mdt-source-data/Montana_On_System_Routes_OD.csv')
    segments_df = enrich_segments_with_metadata(segments_df, on_system)
    
    return segments_df

def calculate_averaged_traffic(base_segments_df, years=[2023, 2022, 2021, 2020, 2019]):
    """Calculate averaged traffic data for exact segment matches"""
    # print(f"[DEBUG] Calculating averaged traffic across years: {years}")
    
    # start with base segments and their 2023 traffic data
    base_segments_df['TYC_AADT'] = pd.to_numeric(base_segments_df['TYC_AADT'], errors='coerce')
    base_segments_df['YEARS_WITH_DATA'] = 1  # Start with 2023
    
    # For each additional year, try to find exact matches and average
    for year in years[1:]:  # skip 2023 since it's our base
        csv_path = f'data/Traffic_Yearly_Counts_{year}/TYC_{year}.csv'
        if not os.path.exists(csv_path):
            # print(f"[DEBUG] Traffic CSV not found for year {year}: {csv_path}")
            continue
            
        year_df = pd.read_csv(csv_path, dtype=str)
        year_df['CORR_ID'] = year_df['CORR_ID'].astype(str).str.strip().str.upper()
        year_df['DEPT_ID'] = year_df['DEPT_ID'].astype(str).str.strip().str.upper()
        
        # create composite key for matching
        year_df['SEGMENT_KEY'] = (year_df['CORR_ID'] + '_' + 
                                 year_df['CORR_MP'] + '_' + 
                                 year_df['CORR_ENDMP'] + '_' + 
                                 year_df['DEPT_ID'])
        
        year_df['TYC_AADT'] = pd.to_numeric(year_df['TYC_AADT'], errors='coerce')
        
        # print(f"[DEBUG] Loaded {len(year_df)} segments from year {year}")
        
        # find exact matches and update averages
        matches = 0
        for idx, base_row in base_segments_df.iterrows():
            segment_key = base_row['SEGMENT_KEY']
            year_matches = year_df[year_df['SEGMENT_KEY'] == segment_key]
            
            if len(year_matches) > 0:
                year_aadt = year_matches.iloc[0]['TYC_AADT']
                if pd.notna(year_aadt):
                    current_aadt = base_segments_df.at[idx, 'TYC_AADT']
                    current_years = base_segments_df.at[idx, 'YEARS_WITH_DATA']
                    
                    # calculate new average
                    if pd.notna(current_aadt):
                        new_total = (current_aadt * current_years) + year_aadt
                        new_years = current_years + 1
                        new_average = new_total / new_years
                        
                        base_segments_df.at[idx, 'TYC_AADT'] = new_average
                        base_segments_df.at[idx, 'YEARS_WITH_DATA'] = new_years
                        matches += 1
        
        # print(f"[DEBUG] Found {matches} exact matches for year {year}")
    
    # print("[DEBUG] Averaged traffic data complete")
    # print(f"[DEBUG] Segments with data from all {len(years)} years: {(base_segments_df['YEARS_WITH_DATA'] == len(years)).sum()}")
    
    return base_segments_df

def build_corridor_index(segments_df):
    """Build corridor index for crash matching"""
    index = {}
    for _, row in segments_df.iterrows():
        corr_id = row['CORR_ID']
        section = {
            'SEGMENT_KEY': row['SEGMENT_KEY'],
            'CORR_ID': row['CORR_ID'],
            'DEPT_ID': row['DEPT_ID'],
            'SEC_LNT_MI': pd.to_numeric(row.get('SEC_LNT_MI', None), errors='coerce'),
            'TYC_AADT': pd.to_numeric(row.get('TYC_AADT', None), errors='coerce'),
            'LOCATION': row.get('SITE_ID'),
            'SITE_ID': row.get('SITE_ID'),
            'CORR_MP': row.get('CORR_MP'),
            'CORR_ENDMP': row.get('CORR_ENDMP'),
            'CORR_MP_FLOAT': row.get('CORR_MP_FLOAT'),
            'CORR_ENDMP_FLOAT': row.get('CORR_ENDMP_FLOAT'),
        }
        section['MILES_DRIVEN'] = section['SEC_LNT_MI'] * section['TYC_AADT'] if pd.notna(section['SEC_LNT_MI']) and pd.notna(section['TYC_AADT']) else None
        
        # metadata
        for extra in ('ROUTE_NAME', 'SIGNED_ROUTE', 'SYSTEM'):
            if extra in row and pd.notna(row[extra]):
                section[extra] = row[extra]
        
        index.setdefault(corr_id, []).append(section)
    
    return index

def match_crash_to_section(crash, corridor_index):
    """Match a crash to a road section"""
    corridor = crash['CORRIDOR']
    ref_point = crash['REF_POINT_FLOAT']
    
    if pd.isna(ref_point) or corridor not in corridor_index:
        return None
    
    for section in corridor_index[corridor]:
        if section['CORR_MP_FLOAT'] is not None and section['CORR_ENDMP_FLOAT'] is not None:
            if section['CORR_MP_FLOAT'] <= ref_point <= section['CORR_ENDMP_FLOAT']:
                out = {
                    'SEGMENT_KEY': section['SEGMENT_KEY'],
                    'CORRIDOR': corridor,
                    'CORR_ID': section['CORR_ID'],
                    'SEC_LNT_MI': section['SEC_LNT_MI'],
                    'TYC_AADT': section['TYC_AADT'],
                    'MILES_DRIVEN': section.get('MILES_DRIVEN'),
                    'LOCATION': section['LOCATION'],
                    'COUNTY': crash.get('COUNTY'),
                    'SITE_ID': section['SITE_ID'],
                    'CORR_MP': section['CORR_MP'],
                    'CORR_ENDMP': section['CORR_ENDMP'],
                    'DEPT_ID': section['DEPT_ID'],
                    'CRASHES': 1,
                }
                # metadata
                for extra in ('ROUTE_NAME', 'SIGNED_ROUTE', 'SYSTEM'):
                    if extra in section:
                        out[extra] = section.get(extra)
                return out
    return None

def create_simplified_average_output(
    crash_csv='raw-mdt-source-data/2019-2023-crash-data.csv',
    merged_dir='output/merged_data',
    years=[2023, 2022, 2021, 2020, 2019]
):
    """Create only the averaged output file"""
    
    # check for merged_dir
    os.makedirs(merged_dir, exist_ok=True)
    
    print("=== SIMPLIFIED AVERAGE MERGE PROCESSING ===")
    
    # base segments from 2023
    base_segments_df = load_base_segments_2023()
    if base_segments_df is None:
        print("[ERROR] Could not load base segments")
        return
    
    # calculate averaged traffic data
    averaged_segments_df = calculate_averaged_traffic(base_segments_df, years)
    
    # build corridor index for crash matching
    corridor_index = build_corridor_index(averaged_segments_df)
    
    # load and process crash data
    crash_df = pd.read_csv(crash_csv, dtype=str)
    crash_df['CORRIDOR'] = crash_df['CORRIDOR'].str.strip().str.upper()
    crash_df['REF_POINT_FLOAT'] = crash_df['REF_POINT'].apply(parse_milepost)
    
    # print(f"[DEBUG] Loaded {len(crash_df)} crashes")
    
    # match crashes to segments
    matched_sections = []
    matched_crashes_full = []  # preserve full crash rows with attached DEPT_ID for per-dept crash files
    for idx, crash in crash_df.iterrows():
        match = match_crash_to_section(crash, corridor_index)
        if match:
            matched_sections.append(match)
            # preserve original crash fields and attach DEPT_ID from the matched section
            try:
                crash_record = crash.to_dict()
            except Exception:
                # fallback if crash is not a Series
                crash_record = dict(crash)
            # attach useful matching metadata so we can later group/write per-route crash files
            crash_record['DEPT_ID'] = match.get('DEPT_ID')
            crash_record['SEGMENT_KEY'] = match.get('SEGMENT_KEY')
            # attach route metadata if available
            if match.get('ROUTE_NAME') is not None:
                crash_record['ROUTE_NAME'] = match.get('ROUTE_NAME')
            if match.get('SIGNED_ROUTE') is not None:
                crash_record['SIGNED_ROUTE'] = match.get('SIGNED_ROUTE')
            # ensure CORRIDOR is present/normalized
            crash_record['CORRIDOR'] = crash_record.get('CORRIDOR', match.get('CORRIDOR'))
            # ensure REF_POINT_FLOAT is present
            if 'REF_POINT_FLOAT' not in crash_record or pd.isna(crash_record.get('REF_POINT_FLOAT')):
                crash_record['REF_POINT_FLOAT'] = parse_milepost(crash_record.get('REF_POINT'))
            matched_crashes_full.append(crash_record)

    # print(f"[DEBUG] Found {len(matched_sections)} crash-to-section matches")
    # report how many crash records were loaded and how many matched crash records were preserved
    try:
        total_crashes_loaded = len(crash_df)
    except Exception:
        total_crashes_loaded = 'unknown'
    # print(f"[DEBUG] Total crashes loaded: {total_crashes_loaded}; matched crash records preserved: {len(matched_crashes_full)}")
    
    # create complete output with ALL segments (including 0-crash segments)
    # print(f"[DEBUG] Creating complete output with all {len(averaged_segments_df)} segments")
    
    # all segments
    complete_df = averaged_segments_df.copy()
    complete_df['TOTAL_CRASHES'] = 0
    complete_df['COUNTY'] = complete_df.get('CNTY_NM', '')
    
    # aggregate crashes by segment
    if matched_sections:
        crash_agg_df = pd.DataFrame(matched_sections)
        
        # group crashes by SEGMENT_KEY
        crash_grouped = crash_agg_df.groupby('SEGMENT_KEY', as_index=False).agg({
            'CRASHES': 'sum',
            'COUNTY': 'first'
        })
        
        # print(f"[DEBUG] Aggregated crashes to {len(crash_grouped)} segments with crashes")

        # merge crash counts into complete segments
        complete_df = complete_df.merge(
            crash_grouped[['SEGMENT_KEY', 'CRASHES', 'COUNTY']], 
            on='SEGMENT_KEY', 
            how='left',
            suffixes=('', '_crash')
        )
        
        # update crash counts
        complete_df['TOTAL_CRASHES'] = complete_df['CRASHES'].fillna(0).astype(int)
        complete_df['COUNTY'] = complete_df['COUNTY_crash'].fillna(complete_df['COUNTY'])
        
        # clean up
        complete_df = complete_df.drop(columns=[col for col in complete_df.columns if col.endswith('_crash')])
    
    # 7. calculate derived metrics
    complete_df['SEC_LNT_MI'] = pd.to_numeric(complete_df['SEC_LNT_MI'], errors='coerce')
    complete_df['TYC_AADT'] = pd.to_numeric(complete_df['TYC_AADT'], errors='coerce')
    complete_df['MILES_DRIVEN'] = complete_df['SEC_LNT_MI'] * complete_df['TYC_AADT']
    
    total_years = len(years)
    complete_df['AVG_CRASHES'] = complete_df['TOTAL_CRASHES'] / total_years
    
    # alculate rates
    complete_df['CARS_PER_ACC'] = None
    complete_df['MILES_PER_ACC'] = None
    
    mask_crashes = complete_df['AVG_CRASHES'] > 0
    complete_df.loc[mask_crashes, 'CARS_PER_ACC'] = (
        complete_df.loc[mask_crashes, 'TYC_AADT'] / complete_df.loc[mask_crashes, 'AVG_CRASHES']
    )
    complete_df.loc[mask_crashes, 'MILES_PER_ACC'] = (
        complete_df.loc[mask_crashes, 'MILES_DRIVEN'] / complete_df.loc[mask_crashes, 'AVG_CRASHES']
    )
    
    # prepare final output columns
    desired_columns = [
        'CORRIDOR', 'SITE_ID', 'CORR_MP', 'CORR_ENDMP', 'DEPT_ID', 'TOTAL_CRASHES',
        'SEC_LNT_MI', 'TYC_AADT', 'MILES_DRIVEN', 'LOCATION', 'COUNTY',
        'ROUTE_NAME', 'SIGNED_ROUTE', 'SYSTEM', 'AVG_CRASHES', 'CARS_PER_ACC', 'MILES_PER_ACC'
    ]
    
    # check all columns exist
    for col in desired_columns:
        if col not in complete_df.columns:
            complete_df[col] = ''
    
    # add LOCATION column if missing
    if 'LOCATION' not in complete_df.columns:
        complete_df['LOCATION'] = complete_df['SITE_ID']
    
    # order columns
    output_df = complete_df[desired_columns].copy()
    
    # sort by corridor and milepost
    output_df = output_df.sort_values(['CORRIDOR', 'CORR_MP'])
    
    # write output
    merged_csv = os.path.join(merged_dir, 'merged_traffic_average.csv')
    sort_car_csv = os.path.join(merged_dir, 'sort_car_merged_traffic_average.csv')
    sort_mile_csv = os.path.join(merged_dir, 'sort_mile_merged_traffic_average.csv')
    
    output_df.to_csv(merged_csv, index=False)
    print(f"Average output written to {merged_csv} with {len(output_df)} rows.")
    
    # write sorted outputs (only sections with crashes)
    sections_with_crashes = output_df[output_df['TOTAL_CRASHES'] > 0].copy()
    
    if len(sections_with_crashes) > 0:
        sections_with_crashes.sort_values('CARS_PER_ACC', ascending=True).to_csv(sort_car_csv, index=False)
        print(f"Sorted by cars/accident written to {sort_car_csv} with {len(sections_with_crashes)} rows.")
        
        sections_with_crashes.sort_values('MILES_PER_ACC', ascending=True).to_csv(sort_mile_csv, index=False)
        print(f"Sorted by miles/accident written to {sort_mile_csv} with {len(sections_with_crashes)} rows.")
    
    # 10. Final check
    print("\n=== FINAL RESULTS ===")
    print(f"Total segments: {len(output_df)}")
    print(f"Segments with crashes: {(output_df['TOTAL_CRASHES'] > 0).sum()}")
    print(f"Segments without crashes: {(output_df['TOTAL_CRASHES'] == 0).sum()}")

    all_roads_dir = os.path.join('output', 'all_roads')
    os.makedirs(all_roads_dir, exist_ok=True)

    # load TYC geojson features prioritized by year (prefer 2023, then 2022...)
    def load_tyc_geojson_map(years, base_dir='data/Traffic_Yearly_Counts'):
        """Return dict mapping SEGMENT_KEY -> geojson feature, preferring earlier years in the list.

        SEGMENT_KEY is constructed as in CSV: CORR_ID_CORR_MP_CORR_ENDMP_DEPT_ID
        """
        combined = {}
        for year in years:
            # try a couple of likely paths (user specified data/Traffic_Yearly_Counts/TYC-{year}.json)
            candidates = [
                os.path.join(base_dir, f'TYC_{year}.json'),
                os.path.join(base_dir + f'_{year}', f'TYC_{year}.json'),
                os.path.join(base_dir + f'_{year}', f'TYC_{year}.JSON'),
            ]
            found = False
            for path in candidates:
                if os.path.exists(path):
                    try:
                        with open(path, 'r') as fh:
                            js = json.load(fh)
                    except Exception as e:
                        # print(f"[DEBUG] Failed to load geojson {path}: {e}")
                        continue

                    for feat in js.get('features', []):
                        props = feat.get('properties', {})
                        corr_id = str(props.get('CORR_ID', '')).strip().upper()
                        dept_id = str(props.get('DEPT_ID', '')).strip().upper()
                        corr_mp = str(props.get('CORR_MP', ''))
                        corr_endmp = str(props.get('CORR_ENDMP', ''))
                        key = f"{corr_id}_{corr_mp}_{corr_endmp}_{dept_id}"
                        # only set if not present so earlier (preferred) years win
                        if key not in combined:
                            combined[key] = feat
                    found = True
                    # print(f"[DEBUG] Loaded {len(js.get('features', []))} features from {path}")
                    break
            if not found:
                print(f"[DEBUG] No TYC geojson found for year {year} in {base_dir} or {base_dir}_{year}")
        return combined

    geojson_map = load_tyc_geojson_map(years)

    # crash columns for per-dept crash files
    dept_crash_cols = [
    'SEGMENT_KEY',
        'CORRIDOR','DIR','REF_POINT','SMT_CITY_NAME','COUNTY','CRASH_MONTH','CRASH_YEAR','DAY_OF_WEEK',
        'SMS_X_CORD','SMS_Y_CORD','LATITUDE','LONGITUDE','REF_POINT_FLOAT'
    ]

    # use the final `complete_df` (which includes TOTAL_CRASHES and derived fields) as source
    export_segments_df = complete_df.copy()
    # checked for ROUTE_NAME and SIGNED_ROUTE 
    if 'ROUTE_NAME' not in export_segments_df.columns:
        export_segments_df['ROUTE_NAME'] = ''
    if 'SIGNED_ROUTE' not in export_segments_df.columns:
        export_segments_df['SIGNED_ROUTE'] = ''
    export_segments_df['ROUTE_NAME'] = export_segments_df['ROUTE_NAME'].fillna('').astype(str).str.strip()
    export_segments_df['SIGNED_ROUTE'] = export_segments_df['SIGNED_ROUTE'].fillna('').astype(str).str.strip()

    # SIGNED_ROUTE if present, else ROUTE_NAME
    def get_route_name(row):
        return row['SIGNED_ROUTE'] if row['SIGNED_ROUTE'] else row['ROUTE_NAME']
    export_segments_df['ROUTE_FILE_NAME'] = export_segments_df.apply(get_route_name, axis=1)

    # only keep non-empty route names
    unique_routes = export_segments_df['ROUTE_FILE_NAME'].dropna().unique().tolist()
    unique_routes = [r for r in unique_routes if r]

    # Precompute shared data to avoid repeated work in each worker
    base_columns = list(base_segments_df.columns)
    extra_cols = ['SEGMENT_KEY','CORR_MP_FLOAT','CORR_ENDMP_FLOAT','ROUTE_NAME','SIGNED_ROUTE','SYSTEM',
                  'YEARS_WITH_DATA','TOTAL_CRASHES','COUNTY','CRASHES','MILES_DRIVEN','AVG_CRASHES',
                  'CARS_PER_ACC','MILES_PER_ACC','LOCATION','ROUTE_FILE_NAME']
    write_cols = list(base_columns) + [c for c in extra_cols if c not in base_columns]

    # Pre-build crashes DataFrame once if we have matched crashes
    crashes_df = None
    if matched_crashes_full:
        try:
            crashes_df = pd.DataFrame.from_records(matched_crashes_full)
            # ensure ROUTE_NAME and SIGNED_ROUTE exist
            if 'ROUTE_NAME' not in crashes_df.columns:
                crashes_df['ROUTE_NAME'] = ''
            if 'SIGNED_ROUTE' not in crashes_df.columns:
                crashes_df['SIGNED_ROUTE'] = ''
            crashes_df['ROUTE_FILE_NAME'] = crashes_df.apply(get_route_name, axis=1)
        except Exception:
            crashes_df = None

    def process_route(route_name):
        if not route_name or str(route_name).strip() == '':
            return (route_name, 0, 0)

        route_rows = export_segments_df[export_segments_df['ROUTE_FILE_NAME'] == route_name].copy()
        if route_rows.empty:
            return (route_name, 0, 0)

        # Sanitize route name for filesystem use (replace problematic characters)
        safe_route_name = str(route_name).replace('/', '_').replace('\\', '_').replace(':', '_').replace('*', '_').replace('?', '_').replace('"', '_').replace('<', '_').replace('>', '_').replace('|', '_')

        out_path = os.path.join(all_roads_dir, f"{safe_route_name}.csv")
        written_rows = 0
        try:
            # Use precomputed write_cols and reindex to add missing columns efficiently
            route_rows = route_rows.reindex(columns=write_cols, fill_value='')
            
            # Vectorized sanitization of string columns
            str_cols = route_rows.select_dtypes(include=['object']).columns
            for c in str_cols:
                route_rows[c] = route_rows[c].astype(str).str.replace('\n', ' ', regex=False).str.replace('\r', ' ', regex=False)
            
            # Sanitize column names
            route_rows.columns = [str(col).replace('\n',' ').replace('\r',' ') for col in route_rows.columns]

            route_rows.to_csv(out_path, index=False)
            written_rows = len(route_rows)
        except Exception as e:
            print(f"[ERROR] Writing all_roads file for {route_name}: {e}")

        # build geojson FeatureCollection for this route - optimized approach
        features_out = []
        # Use to_dict('records') for faster iteration than iterrows()
        for row in route_rows.to_dict('records'):
            seg_key = row.get('SEGMENT_KEY')
            
            # Build feature properties efficiently
            props = {}
            for col, val in row.items():
                if pd.isna(val) or val is None:
                    props[col] = ""
                else:
                    props[col] = val
            
            # Ensure metric extras are present
            for extra in ('TOTAL_CRASHES', 'AVG_CRASHES', 'CARS_PER_ACC', 'MILES_PER_ACC'):
                if extra not in props or pd.isna(props.get(extra)):
                    props[extra] = ""
            
            # Create feature with geometry reference (avoid deep copy)
            if seg_key and seg_key in geojson_map:
                template_feature = geojson_map[seg_key]
                feature = {
                    'type': 'Feature',
                    'geometry': template_feature.get('geometry'),  # reuse geometry reference
                    'properties': props
                }
            else:
                feature = {'type': 'Feature', 'geometry': None, 'properties': props}

            features_out.append(feature)

        geojson_out = {'type': 'FeatureCollection', 'features': features_out}
        geo_path = os.path.join(all_roads_dir, f"{safe_route_name}.geojson")
        try:
            with open(geo_path, 'w') as gh:
                json.dump(geojson_out, gh)
            # print(f"Wrote geojson for {route_name}: {geo_path} ({len(features_out)} features)")
        except Exception as e:
            print(f"[ERROR] Writing geojson for {route_name}: {e}")

        # write crashes matched to this route - use precomputed crashes_df
        crash_written = 0
        if crashes_df is not None:
            route_crashes = crashes_df[crashes_df['ROUTE_FILE_NAME'] == route_name].copy()
            if not route_crashes.empty:
                # Reindex to ensure all crash columns exist
                route_crashes = route_crashes.reindex(columns=dept_crash_cols, fill_value='')
                crash_out_path = os.path.join(all_roads_dir, f"{safe_route_name}-crashes.csv")
                try:
                    route_crashes.to_csv(crash_out_path, index=False)
                    crash_written = len(route_crashes)
                    # print(f"Wrote crash file for {route_name}: {crash_out_path} ({crash_written} rows)")
                except Exception as e:
                    print(f"[ERROR] Writing crash file for {route_name}: {e}")

        return (route_name, written_rows, crash_written)

    # Increase parallelization - use more workers for I/O-bound tasks
    max_workers = min(16, (os.cpu_count() or 4) * 2)
    print(f"[DEBUG] Processing {len(unique_routes)} routes with {max_workers} workers")
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for route_name in unique_routes:
            futures.append(ex.submit(process_route, route_name))

        for fut in as_completed(futures):
            try:
                route_name, rows, crashes = fut.result()
            except Exception as e:
                print(f"[ERROR] Route worker failed: {e}")
    
if __name__ == "__main__":
    create_simplified_average_output()
