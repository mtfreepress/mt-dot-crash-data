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
            # 'OBJECTID': row['OBJECTID'],
            'DEPT_ID': row['DEPT_ID'],
            'SEC_LNT_MI': pd.to_numeric(row['SEC_LNT_MI'], errors='coerce'),
            'TYC_AADT': pd.to_numeric(row['TYC_AADT'], errors='coerce'),
            'MILES_DRIVEN': pd.to_numeric(row['SEC_LNT_MI'], errors='coerce') * pd.to_numeric(row['TYC_AADT'], errors='coerce'),
            'LOCATION': row['SITE_ID'],
            # 'COUNTY': row['CNTY_NM'],  # <-- Fix here!
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
                    'CRASHES': 1
                }
    return None

def compile_corridor_miles_for_year(
    year,
    crash_csv='data/2019-2023-crash-data.csv',
    traffic_csv_template='data/Traffic_Yearly_Counts_{year}/TYC_{year}.csv',
    output_csv_template='merged_traffic_{year}.csv',
    routes_dir='routes'
):
    # Prepare file paths
    traffic_csv = traffic_csv_template.format(year=year)
    output_csv = output_csv_template.format(year=year)
    routes_year_dir = os.path.join(routes_dir, str(year))
    os.makedirs(routes_year_dir, exist_ok=True)

    # Load data
    crash_df = pd.read_csv(crash_csv, dtype=str)
    traffic_df = pd.read_csv(traffic_csv, dtype=str)

    crash_df['CORRIDOR'] = crash_df['CORRIDOR'].str.strip().str.upper()
    traffic_df['CORR_ID'] = traffic_df['CORR_ID'].str.strip().str.upper()

    # Only keep crashes for this year
    crash_df = crash_df[crash_df['CRASH_YEAR'] == str(year)].copy()
    crash_df['REF_POINT_FLOAT'] = crash_df['REF_POINT'].apply(parse_milepost)
    traffic_df['CORR_MP_FLOAT'] = traffic_df['CORR_MP'].apply(parse_milepost)
    traffic_df['CORR_ENDMP_FLOAT'] = traffic_df['CORR_ENDMP'].apply(parse_milepost)

    corridor_index = build_corridor_index(traffic_df)

    # Match crashes to sections
    matched_sections = []
    crash_matches = []  # (crash_row, section_dict)

    for idx, crash in crash_df.iterrows():
        match = match_crash_to_section(crash, corridor_index)
        if match:
            matched_sections.append(match)
            crash_matches.append((crash, match))

    # Aggregate CRASHES per section
    output_df = pd.DataFrame(matched_sections)
    agg_cols = ['CORRIDOR', 'SEC_LNT_MI', 'TYC_AADT', 'MILES_DRIVEN', 'LOCATION', 'COUNTY', 'SITE_ID', 'CORR_MP', 'CORR_ENDMP', 'DEPT_ID']
    if not output_df.empty:
        output_df = output_df.groupby(agg_cols, as_index=False)['CRASHES'].sum()
        output_df = output_df.sort_values(['CORRIDOR', 'SITE_ID', 'CORR_MP'])
        output_df.to_csv(output_csv, index=False)
        print(f"Output written to {output_csv} with {len(output_df)} rows.")
    else:
        print(f"No matches for year {year}, no output written.")

    # Write per-DEPT_ID crash lists with full crash entry
    from collections import defaultdict
    dept_crash_dict = defaultdict(list)
    for crash, section in crash_matches:
        dept_id = section['DEPT_ID']
        dept_crash_dict[dept_id].append(crash)

    for dept_id, crash_rows in dept_crash_dict.items():
        route_file = os.path.join(routes_year_dir, f"{dept_id}-{year}.csv")
        pd.DataFrame(crash_rows).to_csv(route_file, index=False)

if __name__ == "__main__":
    years = [2019, 2020, 2021, 2022, 2023]
    for year in years:
        compile_corridor_miles_for_year(year)