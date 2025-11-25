import os
import json
import math
import pandas as pd


def parse_milepost(mp_str):
    if pd.isna(mp_str):
        return None
    parts = str(mp_str).split('+')
    if len(parts) != 2:
        return None
    try:
        return float(parts[0].lstrip('0') or '0') + float(parts[1])
    except ValueError:
        return None


def load_base_segments_2023(base_csv='data/Traffic_Yearly_Counts_2023/TYC_2023.csv'):
    if not os.path.exists(base_csv):
        raise FileNotFoundError(base_csv)
    df = pd.read_csv(base_csv, dtype=str)
    df['CORR_ID'] = df['CORR_ID'].astype(str).str.strip().str.upper()
    df['DEPT_ID'] = df['DEPT_ID'].astype(str).str.strip().str.upper()
    df['SEGMENT_KEY'] = (df['CORR_ID'] + '_' + df['CORR_MP'] + '_' + df['CORR_ENDMP'] + '_' + df['DEPT_ID'])
    df['CORR_MP_FLOAT'] = df['CORR_MP'].apply(parse_milepost)
    df['CORR_ENDMP_FLOAT'] = df['CORR_ENDMP'].apply(parse_milepost)
    df['TYC_AADT'] = pd.to_numeric(df.get('TYC_AADT', ''), errors='coerce')
    df['YEARS_WITH_DATA'] = 1
    return df


def calculate_averaged_traffic(base_df, years=[2023, 2022, 2021, 2020, 2019]):
    for year in years[1:]:
        csv_path = f'data/Traffic_Yearly_Counts_{year}/TYC_{year}.csv'
        if not os.path.exists(csv_path):
            continue
        ydf = pd.read_csv(csv_path, dtype=str)
        ydf['CORR_ID'] = ydf['CORR_ID'].astype(str).str.strip().str.upper()
        ydf['DEPT_ID'] = ydf['DEPT_ID'].astype(str).str.strip().str.upper()
        ydf['SEGMENT_KEY'] = (ydf['CORR_ID'] + '_' + ydf['CORR_MP'] + '_' + ydf['CORR_ENDMP'] + '_' + ydf['DEPT_ID'])
        ydf['TYC_AADT'] = pd.to_numeric(ydf.get('TYC_AADT', ''), errors='coerce')

        # simple exact-key averaging
        idx_map = {r['SEGMENT_KEY']: (i, r) for i, r in ydf.iterrows()}
        for i, row in base_df.iterrows():
            key = row['SEGMENT_KEY']
            if key in idx_map:
                year_aadt = idx_map[key][1].get('TYC_AADT')
                if pd.notna(year_aadt) and pd.notna(base_df.at[i, 'TYC_AADT']):
                    current_aadt = base_df.at[i, 'TYC_AADT']
                    current_years = base_df.at[i, 'YEARS_WITH_DATA']
                    new_total = (current_aadt * current_years) + year_aadt
                    new_years = current_years + 1
                    base_df.at[i, 'TYC_AADT'] = new_total / new_years
                    base_df.at[i, 'YEARS_WITH_DATA'] = new_years
    return base_df


def build_corridor_index(segments_df):
    index = {}
    for _, r in segments_df.iterrows():
        corr = r['CORR_ID']
        entry = {
            'SEGMENT_KEY': r['SEGMENT_KEY'],
            'CORR_MP_FLOAT': r.get('CORR_MP_FLOAT'),
            'CORR_ENDMP_FLOAT': r.get('CORR_ENDMP_FLOAT'),
            'SEC_LNT_MI': pd.to_numeric(r.get('SEC_LNT_MI', None), errors='coerce'),
            'TYC_AADT': pd.to_numeric(r.get('TYC_AADT', None), errors='coerce'),
            'SITE_ID': r.get('SITE_ID'),
            'CORR_MP': r.get('CORR_MP'),
            'CORR_ENDMP': r.get('CORR_ENDMP'),
            'DEPT_ID': r.get('DEPT_ID')
        }
        index.setdefault(corr, []).append(entry)
    return index


def match_crash_to_section(crash_row, corridor_index):
    corridor = str(crash_row.get('CORRIDOR', '')).strip().upper()
    ref = parse_milepost(crash_row.get('REF_POINT'))
    if pd.isna(ref) or corridor not in corridor_index:
        return None
    for sec in corridor_index[corridor]:
        a = sec.get('CORR_MP_FLOAT')
        b = sec.get('CORR_ENDMP_FLOAT')
        if a is not None and b is not None and a <= ref <= b:
            return sec['SEGMENT_KEY']
    return None


def load_tyc_geojson_map(years, base_dir='data/Traffic_Yearly_Counts'):
    combined = {}
    for year in years:
        candidates = [
            os.path.join(base_dir, f'TYC_{year}.json'),
            os.path.join(base_dir + f'_{year}', f'TYC_{year}.json'),
            os.path.join(base_dir + f'_{year}', f'TYC_{year}.JSON'),
        ]
        for path in candidates:
            if os.path.exists(path):
                try:
                    with open(path, 'r') as fh:
                        js = json.load(fh)
                except Exception:
                    continue
                for feat in js.get('features', []):
                    p = feat.get('properties', {})
                    corr_id = str(p.get('CORR_ID', '')).strip().upper()
                    dept_id = str(p.get('DEPT_ID', '')).strip().upper()
                    corr_mp = str(p.get('CORR_MP', ''))
                    corr_endmp = str(p.get('CORR_ENDMP', ''))
                    key = f"{corr_id}_{corr_mp}_{corr_endmp}_{dept_id}"
                    if key not in combined:
                        combined[key] = feat
                break
    return combined


def point_on_linestring(geometry, prefer='midpoint'):
    if geometry is None:
        return None
    t = geometry.get('type')
    if t == 'LineString':
        coords = geometry.get('coordinates', [])
    elif t == 'MultiLineString':
        parts = geometry.get('coordinates', [])
        if not parts:
            return None
        coords = max(parts, key=lambda p: len(p))
    else:
        return None
    if not coords:
        return None
    if prefer == 'start':
        return coords[0]
    seg_lengths = []
    total = 0.0
    for i in range(1, len(coords)):
        x0, y0 = coords[i-1]
        x1, y1 = coords[i]
        d = math.hypot(x1 - x0, y1 - y0)
        seg_lengths.append(d)
        total += d
    if total == 0:
        return coords[0]
    half = total / 2.0
    cum = 0.0
    for i, d in enumerate(seg_lengths, start=1):
        prev = coords[i-1]
        cur = coords[i]
        if cum + d >= half:
            remain = half - cum
            t = remain / d if d != 0 else 0
            x = prev[0] + (cur[0] - prev[0]) * t
            y = prev[1] + (cur[1] - prev[1]) * t
            return [x, y]
        cum += d
    return coords[-1]


def main(crash_csv='raw-mdt-source-data/2019-2023-crash-data.csv', years=[2023, 2022, 2021, 2020, 2019], out_dir='output/merged_data'):
    os.makedirs(out_dir, exist_ok=True)
    base = load_base_segments_2023()
    averaged = calculate_averaged_traffic(base, years)
    corridor_index = build_corridor_index(averaged)

    crashes = pd.read_csv(crash_csv, dtype=str)
    crashes['CORRIDOR'] = crashes['CORRIDOR'].astype(str).str.strip().str.upper()

    matched = []
    for _, cr in crashes.iterrows():
        key = match_crash_to_section(cr, corridor_index)
        if key:
            matched.append(key)

    # count crashes per segment
    crash_counts = pd.Series(matched).value_counts().to_dict()

    # compute metrics
    df = averaged.copy()
    df['SEC_LNT_MI'] = pd.to_numeric(df.get('SEC_LNT_MI', None), errors='coerce')
    df['TYC_AADT'] = pd.to_numeric(df.get('TYC_AADT', None), errors='coerce')
    df['MILES_DRIVEN'] = df['SEC_LNT_MI'] * df['TYC_AADT']
    total_years = len(years)
    df['TOTAL_CRASHES'] = df['SEGMENT_KEY'].map(crash_counts).fillna(0).astype(int)
    df['AVG_CRASHES'] = df['TOTAL_CRASHES'] / total_years
    df['ANNUAL_VMT'] = df['MILES_DRIVEN'] * 365.25
    df['PER_100M_VMT'] = None
    mask = df['ANNUAL_VMT'].notna() & (df['ANNUAL_VMT'] > 0)
    df.loc[mask, 'PER_100M_VMT'] = (df.loc[mask, 'AVG_CRASHES'] / df.loc[mask, 'ANNUAL_VMT']) * 100_000_000

    # load geometries
    geo_map = load_tyc_geojson_map(years)

    # filter low-volume segments and departmental prefixes (exclude R, L, X, U)
    filtered = df.copy()
    filtered['TYC_AADT_NUM'] = pd.to_numeric(filtered.get('TYC_AADT', ''), errors='coerce')
    filtered = filtered[filtered['TYC_AADT_NUM'] >= 1]
    depts_exclude = ('R', 'L', 'X', 'U')
    dept_upper = filtered['DEPT_ID'].astype(str).str.strip().str.upper()
    # exclude prefixes R/L/X/U but explicitly keep U-5832
    exclude_mask = dept_upper.str.startswith(depts_exclude, na=False) & (dept_upper != 'U-5832')
    before_count = len(filtered)
    filtered = filtered[~exclude_mask]
    removed = before_count - len(filtered)
    if removed > 0:
        print(f"Filtered out {removed} segments because DEPT_ID starts with {', '.join(depts_exclude)} (kept U-5832)")

    points = []
    lines = []
    for _, row in filtered.iterrows():
        seg_key = row['SEGMENT_KEY']
        if seg_key not in geo_map:
            continue
        feat = geo_map[seg_key]
        geom = feat.get('geometry')
        pt = point_on_linestring(geom, prefer='start')
        if pt is None:
            continue
        try:
            pt = [round(float(pt[0]), 5), round(float(pt[1]), 5)]
        except Exception:
            pass
        props = {
            'SEGMENT_KEY': seg_key,
            'CORRIDOR': row.get('CORR_ID', ''),
            'DEPT_ID': row.get('DEPT_ID', ''),
            'TOTAL_CRASHES': int(row.get('TOTAL_CRASHES', 0)),
            'AVG_CRASHES': float(row.get('AVG_CRASHES', 0.0)),
            'PER_100M_VMT': float(row.get('PER_100M_VMT')) if pd.notna(row.get('PER_100M_VMT')) else '',
            'TYC_AADT': int(row['TYC_AADT']) if pd.notna(row.get('TYC_AADT')) and float(row['TYC_AADT']).is_integer() else (float(row['TYC_AADT']) if pd.notna(row.get('TYC_AADT')) else ''),
        }
        points.append({'type': 'Feature', 'geometry': {'type': 'Point', 'coordinates': pt}, 'properties': props})
        if geom is not None:
            lines.append({'type': 'Feature', 'geometry': geom, 'properties': props})

    points_gc = {'type': 'FeatureCollection', 'features': points}
    lines_gc = {'type': 'FeatureCollection', 'features': lines}

    with open(os.path.join(out_dir, 'merged_traffic_average_points.geojson'), 'w') as pf:
        json.dump(points_gc, pf)
    with open(os.path.join(out_dir, 'merged_traffic_lines.geojson'), 'w') as lf:
        json.dump(lines_gc, lf)

    print(f"Wrote {len(points)} points and {len(lines)} lines to {out_dir}")


if __name__ == '__main__':
    main()
