#!/usr/bin/env python3
import os
import csv
import math
import json
from collections import Counter

ROOT = os.path.dirname(__file__)
MANUAL_DIR = os.path.join(ROOT, "manual-segments")
# output analysis directory
TRIP_DIR = os.path.join(ROOT, "trip_analysis")
OUT_CAR = os.path.join(TRIP_DIR, "per_car.csv")
OUT_MILE = os.path.join(TRIP_DIR, "per_mile.csv")
# per-vmt CSV (crashes per 100M VMT)
OUT_VMT = os.path.join(TRIP_DIR, "per_vmt.csv")

def _to_float(x):
    try:
        return float(x)
    except Exception:
        return 0.0

def _to_int(x):
    try:
        return int(float(x))
    except Exception:
        return 0

def most_common_nonempty(values):
    vals = [v for v in values if v is not None and str(v).strip() != ""]
    if not vals:
        return ""
    return Counter(vals).most_common(1)[0][0]

def process_all_files():
    # process csv files and group by segment name
    if not os.path.isdir(MANUAL_DIR):
        print("manual-segments directory not found:", MANUAL_DIR)
        return []

    files = [os.path.join(MANUAL_DIR, fn) for fn in os.listdir(MANUAL_DIR) if fn.lower().endswith(".csv") and fn != "all_routes.csv"]
    
    # dictionary to group data by segment name
    segments = {}
    
    for file_path in files:
        # segment name from filename (remove .csv extension)
        segment_name = os.path.splitext(os.path.basename(file_path))[0]
        
        rows = []
        try:
            with open(file_path, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for r in reader:
                    rows.append(r)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            continue
            
        if not rows:
            continue

        # initialize segment data if not exists
        if segment_name not in segments:
            segments[segment_name] = {
                "rows": [],
                "sum_length": 0.0,
                "sum_miles_driven": 0.0,
                "sum_weighted_aadt": 0.0,
                "sum_crashes": 0,
                "total_length_for_weight": 0.0,
                "dept_vals": [],
                "route_name_vals": [],
                "system_vals": [],
                "signed_route_vals": []
            }

        # process each row and accumulate data for this segment
        for r in rows:
            sec = _to_float(r.get("SEC_LNT_MI", r.get("SEC_LNT", 0)))
            aadt = _to_float(r.get("TYC_AADT", r.get("AADT", 0)))
            # miles_driven = _to_float(r.get("MILES_DRIVEN", 0.0))
            # # if MILES_DRIVEN missing, compute from sec * aadt
            # if miles_driven == 0.0:
            miles_driven = sec * aadt

            crashes = _to_int(r.get("TOTAL_CRASHES", r.get("TOTAL_CRASH", 0)))

            # accumulate per-row totals for this segment
            segments[segment_name]["sum_length"] += sec
            segments[segment_name]["sum_miles_driven"] += miles_driven
            segments[segment_name]["sum_weighted_aadt"] += aadt * sec
            segments[segment_name]["total_length_for_weight"] += sec
            segments[segment_name]["sum_crashes"] += crashes

            segments[segment_name]["dept_vals"].append(r.get("DEPT_ID", ""))
            segments[segment_name]["route_name_vals"].append(r.get("ROUTE_NAME", ""))
            segments[segment_name]["system_vals"].append(r.get("SYSTEM", ""))
            segments[segment_name]["signed_route_vals"].append(r.get("SIGNED_ROUTE", ""))
            segments[segment_name]["rows"].append(r)

    # convert segment data to results
    results = []
    for segment_name, seg_data in segments.items():
        if seg_data["total_length_for_weight"] > 0:
            avg_aadt = seg_data["sum_weighted_aadt"] / seg_data["total_length_for_weight"]
        else:
            # fallback to simple mean
            aadt_vals = [_to_float(r.get("TYC_AADT", 0)) for r in seg_data["rows"]]
            avg_aadt = sum(aadt_vals) / len(aadt_vals) if aadt_vals else 0.0

        dept = most_common_nonempty(seg_data["dept_vals"])
        route_name = most_common_nonempty(seg_data["route_name_vals"])
        system = most_common_nonempty(seg_data["system_vals"])
        signed_route = most_common_nonempty(seg_data["signed_route_vals"])

        # adjust VMT to cover the crash accumulation period.
        DAYS_PER_YEAR = 365.25
        YEARS = 5

        # total_vmt is vehicle-miles traveled over the full crash period
        total_vmt = seg_data["sum_miles_driven"] * DAYS_PER_YEAR * YEARS if seg_data["sum_miles_driven"] > 0 else 0.0

        # don't divide by zero
        cars_per_acc = (avg_aadt / seg_data["sum_crashes"]) if seg_data["sum_crashes"] > 0 else math.inf
        miles_per_acc = (seg_data["sum_miles_driven"] / seg_data["sum_crashes"]) if seg_data["sum_crashes"] > 0 else math.inf
        crashes_per_100m_vmt = (seg_data["sum_crashes"] / total_vmt * 100_000_000) if total_vmt > 0 else math.inf

        results.append({
            "route": segment_name,
            "segment_name": segment_name,
            "SIGNED_ROUTE": signed_route,
            "crashes": seg_data["sum_crashes"],
            "length": seg_data["sum_length"],
            "DEPT_ID": dept,
            "TOTAL_CRASHES": seg_data["sum_crashes"],
            "AVG_TYC_AADT": avg_aadt,
            "MILES_DRIVEN": seg_data["sum_miles_driven"],
            "ROUTE_NAME": route_name,
            "SYSTEM": system,
            "CARS_PER_ACCIDENT": cars_per_acc,
            "MILES_PER_ACCIDENT": miles_per_acc,
            "CRASHES_PER_100M_VMT": crashes_per_100m_vmt,
        })

    return results

def write_csv(path, rows, columns):
    with open(path, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for r in rows:
            # format floats for readability
            out = r.copy()
            for k in ("length","AVG_TYC_AADT","MILES_DRIVEN","CARS_PER_ACCIDENT","MILES_PER_ACCIDENT","CRASHES_PER_100M_VMT"):
                if k in out:
                    v = out[k]
                    out[k] = "" if (v is None or (isinstance(v, float) and math.isinf(v))) else ("{:.6f}".format(v) if isinstance(v, float) else v)
            writer.writerow(out)

def main():
    if not os.path.isdir(MANUAL_DIR):
        print("manual-segments directory not found:", MANUAL_DIR)
        return

    # ensure output dirs exist
    os.makedirs(TRIP_DIR, exist_ok=True)
    per_car_dir = os.path.join(TRIP_DIR, "per_car")
    per_mile_dir = os.path.join(TRIP_DIR, "per_mile")
    per_vmt_dir = os.path.join(TRIP_DIR, "per_vmt")
    os.makedirs(per_car_dir, exist_ok=True)
    os.makedirs(per_mile_dir, exist_ok=True)
    os.makedirs(per_vmt_dir, exist_ok=True)
    results = process_all_files()

    # per_car sorted by CARS_PER_ACCIDENT (low -> high)
    per_car = sorted(results, key=lambda r: (math.inf if (r["CARS_PER_ACCIDENT"] is None) else r["CARS_PER_ACCIDENT"]))
    # per_mile sorted by MILES_PER_ACCIDENT (low -> high)
    per_mile = sorted(results, key=lambda r: (math.inf if (r["MILES_PER_ACCIDENT"] is None) else r["MILES_PER_ACCIDENT"]))
    # per_vmt sorted by CRASHES_PER_100M_VMT (high -> low)
    per_vmt = sorted(results, key=lambda r: (-(r["CRASHES_PER_100M_VMT"]) if (r["CRASHES_PER_100M_VMT"] is not None and not (isinstance(r["CRASHES_PER_100M_VMT"], float) and math.isinf(r["CRASHES_PER_100M_VMT"]))) else math.inf))
    cols = ["route","segment_name","SIGNED_ROUTE","crashes","length","DEPT_ID","TOTAL_CRASHES","AVG_TYC_AADT","MILES_DRIVEN","ROUTE_NAME","SYSTEM","CARS_PER_ACCIDENT","MILES_PER_ACCIDENT","CRASHES_PER_100M_VMT"]
    write_csv(OUT_CAR, per_car, cols)
    write_csv(OUT_MILE, per_mile, cols)
    write_csv(OUT_VMT, per_vmt, cols)
    print("Wrote", OUT_CAR, OUT_MILE, OUT_VMT)

    # helper to merge geojson files for top N routes into a single FeatureCollection
    def merge_geojson_for_top(rows, top_n, out_path):
        features = []
        count = 0
        for r in rows[:top_n]:
            segment_name = r.get("segment_name")
            if not segment_name:
                continue
            # try common extensions and case-insensitive prefix matches
            candidates = [segment_name + ".geojson", segment_name + ".json"]
            json_path = None
            for c in candidates:
                p = os.path.join(MANUAL_DIR, c)
                if os.path.isfile(p):
                    json_path = p
                    break
            # if exact names not found, try prefix match (case-insensitive) for files in MANUAL_DIR
            if json_path is None:
                lname = segment_name.lower()
                for fn in sorted(os.listdir(MANUAL_DIR)):
                    if not (fn.lower().endswith('.geojson') or fn.lower().endswith('.json')):
                        continue
                    base = os.path.splitext(fn)[0].lower()
                    # prefer exact base name match
                    if base == lname:
                        p = os.path.join(MANUAL_DIR, fn)
                        if os.path.isfile(p):
                            json_path = p
                            break
                # if no exact base match, allow common separator-prefixed matches
                if json_path is None:
                    for fn in sorted(os.listdir(MANUAL_DIR)):
                        if not (fn.lower().endswith('.geojson') or fn.lower().endswith('.json')):
                            continue
                        base = os.path.splitext(fn)[0].lower()
                        # allow matches like 'route_extra' or 'route-extra'
                        if base.startswith(lname + '_') or base.startswith(lname + '-'):
                            p = os.path.join(MANUAL_DIR, fn)
                            if os.path.isfile(p):
                                json_path = p
                                break
            if json_path is None:
                # no matching file found for this route
                continue
            try:
                with open(json_path, encoding='utf-8') as jf:
                    data = json.load(jf)
                
                # collect all geometries
                segment_geometries = []
                if isinstance(data, dict):
                    if data.get("type") == "FeatureCollection" and isinstance(data.get("features"), list):
                        for feat in data.get("features"):
                            if feat.get("geometry"):
                                segment_geometries.append(feat["geometry"])
                    elif data.get("type") == "Feature":
                        if data.get("geometry"):
                            segment_geometries.append(data["geometry"])
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get("geometry"):
                            segment_geometries.append(item["geometry"])
                
                # create a single feature
                if segment_geometries:
                    # Create unified geometry
                    if len(segment_geometries) == 1:
                        unified_geometry = segment_geometries[0]
                    else:
                        coordinates = []
                        for geom in segment_geometries:
                            if geom.get("type") == "LineString":
                                coordinates.append(geom["coordinates"])
                            elif geom.get("type") == "MultiLineString":
                                coordinates.extend(geom["coordinates"])
                        
                        unified_geometry = {
                            "type": "MultiLineString",
                            "coordinates": coordinates
                        }
                    
                    # use only the specified fields
                    unified_properties = {
                        "ROUTE": r.get("route"),
                        "SEGMENT_NAME": r.get("segment_name"),
                        "SIGNED_ROUTE": r.get("SIGNED_ROUTE"),
                        "CRASHES": r.get("crashes"),
                        "LENGTH": r.get("length"),
                        "DEPT_ID": r.get("DEPT_ID"),
                        "TOTAL_CRASHES": r.get("TOTAL_CRASHES"),
                        "AVG_TYC_AADT": r.get("AVG_TYC_AADT"),
                        "MILES_DRIVEN": r.get("MILES_DRIVEN"),
                        "ROUTE_NAME": r.get("ROUTE_NAME"),
                        "SYSTEM": r.get("SYSTEM"),
                        "CARS_PER_ACCIDENT": r.get("CARS_PER_ACCIDENT"),
                        "MILES_PER_ACCIDENT": r.get("MILES_PER_ACCIDENT"),
                        "CRASHES_PER_100M_VMT": None if (isinstance(r.get("CRASHES_PER_100M_VMT"), float) and math.isinf(r.get("CRASHES_PER_100M_VMT"))) else r.get("CRASHES_PER_100M_VMT")
                    }
                    
                    unified_feature = {
                        "type": "Feature",
                        "geometry": unified_geometry,
                        "properties": unified_properties
                    }
                    
                    features.append(unified_feature)
                
                count += 1
            except Exception as e:
                print(f"Error processing {json_path}: {e}")
                continue

        out = {"type": "FeatureCollection", "features": features}
        with open(out_path, 'w', encoding='utf-8') as out_f:
            json.dump(out, out_f, indent=2)

    # write merged JSONs for per_car, per_mile, per_vmt (top 10, top 25, and top 50)
    merge_geojson_for_top(per_car, 10, os.path.join(per_car_dir, "top_10.json"))
    merge_geojson_for_top(per_car, 25, os.path.join(per_car_dir, "top_25.json"))
    merge_geojson_for_top(per_car, 50, os.path.join(per_car_dir, "top_50.json"))
    merge_geojson_for_top(per_mile, 10, os.path.join(per_mile_dir, "top_10.json"))
    merge_geojson_for_top(per_mile, 25, os.path.join(per_mile_dir, "top_25.json"))
    merge_geojson_for_top(per_mile, 50, os.path.join(per_mile_dir, "top_50.json"))
    merge_geojson_for_top(per_vmt, 10, os.path.join(per_vmt_dir, "top_10.json"))
    merge_geojson_for_top(per_vmt, 25, os.path.join(per_vmt_dir, "top_25.json"))
    merge_geojson_for_top(per_vmt, 50, os.path.join(per_vmt_dir, "top_50.json"))

if __name__ == "__main__":
    main()