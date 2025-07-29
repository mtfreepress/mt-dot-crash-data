import os
import pandas as pd

def parse_milepost(mp_str):
    if pd.isna(mp_str):
        return None
    parts = mp_str.split('+')
    if len(parts) != 2:
        return None
    try:
        return float(parts[0]) + float(parts[1])
    except ValueError:
        return None

def build_corridor_index(traffic_df):
    """Builds a dict: CORR_ID -> list of section dicts with pre-parsed floats."""
    index = {}
    for _, row in traffic_df.iterrows():
        corr_id = row['CORR_ID']
        section = {
            'DEPT_ID': row['DEPT_ID'],
            'SEC_LNT_MI': pd.to_numeric(row['SEC_LNT_MI'], errors='coerce'),
            'TYC_AADT': pd.to_numeric(row['TYC_AADT'], errors='coerce'),
            'MILES_DRIVEN': pd.to_numeric(row['SEC_LNT_MI'], errors='coerce') * pd.to_numeric(row['TYC_AADT'], errors='coerce'),
            'LOCATION': row['SITE_ID'],
            'SITE_ID': row['SITE_ID'],
            'CORR_MP': row['CORR_MP'],
            'CORR_ENDMP': row['CORR_ENDMP'],
            'CORR_MP_FLOAT': parse_milepost(row['CORR_MP']),
            'CORR_ENDMP_FLOAT': parse_milepost(row['CORR_ENDMP']),
        }
        index.setdefault(corr_id, []).append(section)
    return index

def match_crash_to_section(crash, corridor_index):
    corridor = crash['CORRIDOR']
    ref_point = crash['REF_POINT_FLOAT']
    if pd.isna(ref_point) or corridor not in corridor_index:
        return None
    for section in corridor_index[corridor]:
        if section['CORR_MP_FLOAT'] is not None and section['CORR_ENDMP_FLOAT'] is not None:
            if section['CORR_MP_FLOAT'] <= ref_point <= section['CORR_ENDMP_FLOAT']:
                return {
                    'CORRIDOR': corridor,
                    'SEC_LNT_MI': section['SEC_LNT_MI'],
                    'TYC_AADT': section['TYC_AADT'],
                    'MILES_DRIVEN': section['MILES_DRIVEN'],
                    'LOCATION': section['LOCATION'],
                    'COUNTY': crash['COUNTY'],
                    'SITE_ID': section['SITE_ID'],
                    'CORR_MP': section['CORR_MP'],
                    'CORR_ENDMP': section['CORR_ENDMP'],
                    'DEPT_ID': section['DEPT_ID'],
                    'CRASHES': 1,
                }
    return None

def compile_corridor_miles_for_year(
    year,
    crash_csv='data/2019-2023-crash-data.csv',
    traffic_csv_template='data/Traffic_Yearly_Counts_{year}/TYC_{year}.csv',
    output_csv_template='merged_traffic_{year}.csv',
    routes_dir='routes'
):
    # file paths
    traffic_csv = traffic_csv_template.format(year=year)
    output_csv = output_csv_template.format(year=year)
    routes_year_dir = os.path.join(routes_dir, str(year))
    os.makedirs(routes_year_dir, exist_ok=True)

    # load data
    crash_df = pd.read_csv(crash_csv, dtype=str)
    traffic_df = pd.read_csv(traffic_csv, dtype=str)

    crash_df['CORRIDOR'] = crash_df['CORRIDOR'].str.strip().str.upper()
    traffic_df['CORR_ID'] = traffic_df['CORR_ID'].str.strip().str.upper()

    # crashes for current year we are processing
    crash_df = crash_df[crash_df['CRASH_YEAR'] == str(year)].copy()
    crash_df['REF_POINT_FLOAT'] = crash_df['REF_POINT'].apply(parse_milepost)
    traffic_df['CORR_MP_FLOAT'] = traffic_df['CORR_MP'].apply(parse_milepost)
    traffic_df['CORR_ENDMP_FLOAT'] = traffic_df['CORR_ENDMP'].apply(parse_milepost)

    corridor_index = build_corridor_index(traffic_df)

    # match crashes to sections
    matched_sections = []
    crash_matches = []

    for idx, crash in crash_df.iterrows():
        match = match_crash_to_section(crash, corridor_index)
        if match:
            matched_sections.append(match)
            crash_matches.append((crash, match))

    # aggregate crashes per section
    output_df = pd.DataFrame(matched_sections)
    agg_cols = ['CORRIDOR', 'SEC_LNT_MI', 'TYC_AADT', 'MILES_DRIVEN', 'LOCATION', 'COUNTY', 'SITE_ID', 'CORR_MP', 'CORR_ENDMP', 'DEPT_ID']
    if not output_df.empty:
        output_df = output_df.groupby(agg_cols, as_index=False)['CRASHES'].sum()
        output_df = output_df.sort_values(['CORRIDOR', 'SITE_ID', 'CORR_MP'])

        # add CARS_PER_ACC and MILES_PER_ACC columns
        output_df['CARS_PER_ACC'] = output_df['TYC_AADT'] / output_df['CRASHES']
        output_df['MILES_PER_ACC'] = output_df['MILES_DRIVEN'] / output_df['CRASHES']

        # write merged_traffic_{year}.csv
        output_df.to_csv(output_csv, index=False)
        print(f"Output written to {output_csv} with {len(output_df)} rows.")

        # write sorted by CARS_PER_ACC (ascending, so lowest cars per accident at top)
        sort_car_csv = f"sort_car_merged_traffic_{year}.csv"
        output_df.sort_values('CARS_PER_ACC', ascending=True).to_csv(sort_car_csv, index=False)
        print(f"Output written to {sort_car_csv}")

        # write sorted by MILES_PER_ACC (ascending, so lowest miles per accident at top)
        sort_mile_csv = f"sort_mile_merged_traffic_{year}.csv"
        output_df.sort_values('MILES_PER_ACC', ascending=True).to_csv(sort_mile_csv, index=False)
        print(f"Output written to {sort_mile_csv}")

    else:
        print(f"No matches for year {year}, no output written.")

    # write per-DEPT_ID crash lists with full crash entry
    from collections import defaultdict
    dept_crash_dict = defaultdict(list)
    for crash, section in crash_matches:
        dept_id = section['DEPT_ID']
        dept_crash_dict[dept_id].append(crash)

    for dept_id, crash_rows in dept_crash_dict.items():
        route_file = os.path.join(routes_year_dir, f"{dept_id}-{year}.csv")
        pd.DataFrame(crash_rows).to_csv(route_file, index=False)

def create_average_outputs(years, routes_dir='routes'):
    """Create averaged outputs across all years"""
    
    # load and combine all traffic data
    all_traffic_data = []
    for year in years:
        traffic_csv = f'data/Traffic_Yearly_Counts_{year}/TYC_{year}.csv'
        if os.path.exists(traffic_csv):
            traffic_df = pd.read_csv(traffic_csv, dtype=str)
            traffic_df['CORR_ID'] = traffic_df['CORR_ID'].str.strip().str.upper()
            traffic_df['CORR_MP_FLOAT'] = traffic_df['CORR_MP'].apply(parse_milepost)
            traffic_df['CORR_ENDMP_FLOAT'] = traffic_df['CORR_ENDMP'].apply(parse_milepost)
            all_traffic_data.append(traffic_df)
    
    combined_traffic_df = pd.concat(all_traffic_data, ignore_index=True)
    
    # group by section identifiers and average TYC_AADT
    section_cols = ['DEPT_ID', 'SEC_LNT_MI', 'CORR_ID', 'CORR_MP', 'CORR_ENDMP', 'SITE_ID']
    avg_traffic_df = combined_traffic_df.groupby(section_cols, as_index=False).agg({
        'TYC_AADT': lambda x: pd.to_numeric(x, errors='coerce').mean()
    })
    
    # add back other needed columns
    for col in ['CORRIDOR', 'CNTY_NM']:  # TODO: add more columns as needed
        if col in combined_traffic_df.columns:
            first_vals = combined_traffic_df.groupby(section_cols, as_index=False)[col].first()
            avg_traffic_df = avg_traffic_df.merge(first_vals, on=section_cols, how='left')
    
    # Load crash data
    crash_df = pd.read_csv('data/2019-2023-crash-data.csv', dtype=str)
    crash_df['CORRIDOR'] = crash_df['CORRIDOR'].str.strip().str.upper()
    crash_df['REF_POINT_FLOAT'] = crash_df['REF_POINT'].apply(parse_milepost)
    
    # Build corridor index from averaged traffic data
    corridor_index = {}
    for _, row in avg_traffic_df.iterrows():
        corr_id = row['CORR_ID']
        section = {
            'DEPT_ID': row['DEPT_ID'],
            'SEC_LNT_MI': pd.to_numeric(row['SEC_LNT_MI'], errors='coerce'),
            'TYC_AADT': pd.to_numeric(row['TYC_AADT'], errors='coerce'),
            'LOCATION': row['SITE_ID'],
            'SITE_ID': row['SITE_ID'],
            'CORR_MP': row['CORR_MP'],
            'CORR_ENDMP': row['CORR_ENDMP'],
            'CORR_MP_FLOAT': parse_milepost(row['CORR_MP']),
            'CORR_ENDMP_FLOAT': parse_milepost(row['CORR_ENDMP']),
        }
        # Calculate MILES_DRIVEN using averaged TYC_AADT
        section['MILES_DRIVEN'] = section['SEC_LNT_MI'] * section['TYC_AADT']
        corridor_index.setdefault(corr_id, []).append(section)
    
    # Match all crashes to sections
    matched_sections = []
    for idx, crash in crash_df.iterrows():
        match = match_crash_to_section(crash, corridor_index)
        if match:
            matched_sections.append(match)
    
    # Aggregate CRASHES per section (sum across all years)
    output_df = pd.DataFrame(matched_sections)
    agg_cols = ['CORRIDOR', 'SEC_LNT_MI', 'TYC_AADT', 'MILES_DRIVEN', 'LOCATION', 'COUNTY', 'SITE_ID', 'CORR_MP', 'CORR_ENDMP', 'DEPT_ID']
    
    if not output_df.empty:
        output_df = output_df.groupby(agg_cols, as_index=False)['CRASHES'].sum()
        output_df = output_df.sort_values(['CORRIDOR', 'SITE_ID', 'CORR_MP'])

        # Add CARS_PER_ACC and MILES_PER_ACC columns
        output_df['CARS_PER_ACC'] = output_df['TYC_AADT'] / output_df['CRASHES']
        output_df['MILES_PER_ACC'] = output_df['MILES_DRIVEN'] / output_df['CRASHES']

        # Write averaged outputs
        output_df.to_csv('merged_traffic_average.csv', index=False)
        print(f"Average output written to merged_traffic_average.csv with {len(output_df)} rows.")

        # Write sorted by CARS_PER_ACC (ascending)
        output_df.sort_values('CARS_PER_ACC', ascending=True).to_csv('sort_car_merged_traffic_average.csv', index=False)
        print("Average output written to sort_car_merged_traffic_average.csv")

        # Write sorted by MILES_PER_ACC (ascending)
        output_df.sort_values('MILES_PER_ACC', ascending=True).to_csv('sort_mile_merged_traffic_average.csv', index=False)
        print("Average output written to sort_mile_merged_traffic_average.csv")

    else:
        print("No matches for average calculation, no output written.")

if __name__ == "__main__":
    years = [2019, 2020, 2021, 2022, 2023]
    
    # Process individual years
    for year in years:
        compile_corridor_miles_for_year(year)

    # Create average outputs
    create_average_outputs(years)

    # Create all-years route files (existing code)
    crash_df = pd.read_csv('data/2019-2023-crash-data.csv', dtype=str)
    crash_df['CORRIDOR'] = crash_df['CORRIDOR'].str.strip().str.upper()
    crash_df['REF_POINT_FLOAT'] = crash_df['REF_POINT'].apply(parse_milepost)
    
    # Load all traffic data to build complete corridor index
    all_traffic_data = []
    for year in years:
        traffic_csv = f'data/Traffic_Yearly_Counts_{year}/TYC_{year}.csv'
        if os.path.exists(traffic_csv):
            traffic_df = pd.read_csv(traffic_csv, dtype=str)
            traffic_df['CORR_ID'] = traffic_df['CORR_ID'].str.strip().str.upper()
            traffic_df['CORR_MP_FLOAT'] = traffic_df['CORR_MP'].apply(parse_milepost)
            traffic_df['CORR_ENDMP_FLOAT'] = traffic_df['CORR_ENDMP'].apply(parse_milepost)
            all_traffic_data.append(traffic_df)
    
    combined_traffic_df = pd.concat(all_traffic_data, ignore_index=True)
    corridor_index = build_corridor_index(combined_traffic_df)
    
    # Match all crashes to sections and group by DEPT_ID
    from collections import defaultdict
    all_dept_crash_dict = defaultdict(list)
    
    for idx, crash in crash_df.iterrows():
        match = match_crash_to_section(crash, corridor_index)
        if match:
            dept_id = match['DEPT_ID']
            all_dept_crash_dict[dept_id].append(crash)

    # Write /routes/all/{dept_id}-all.csv
    all_dir = os.path.join('routes', 'all')
    os.makedirs(all_dir, exist_ok=True)
    for dept_id, crash_rows in all_dept_crash_dict.items():
        all_df = pd.DataFrame(crash_rows)
        if 'CRASH_YEAR' in all_df.columns:
            all_df = all_df.sort_values('CRASH_YEAR')
        out_path = os.path.join(all_dir, f"{dept_id}-all.csv")
        all_df.to_csv(out_path, index=False)
        print(f"Wrote all-years file for {dept_id}: {out_path}")