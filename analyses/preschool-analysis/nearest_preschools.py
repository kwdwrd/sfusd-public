#!/usr/bin/env python3
"""For each SFUSD school, find the 5 closest licensed preschools.

Schools come from data/school-mapping/merged/school-id-mapping.csv (lat/lon included).
Preschools come from data/preschools/data/ccld-sf.csv (addresses geocoded on first run,
cached in data/preschools/data/ccld-sf-geocoded.csv).

Output: analyses/preschool-analysis/nearest_preschools.csv
"""

import csv
import json
import math
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
SCHOOLS_CSV = ROOT / "data/school-mapping/merged/school-id-mapping.csv"
PRESCHOOLS_CSV = ROOT / "data/preschools/data/ccld-sf.csv"
GEOCACHE_JSON = ROOT / "data/preschools/data/ccld-sf-geocache.json"
OUT_CSV = ROOT / "analyses/preschool-analysis/nearest_preschools.csv"

N_CLOSEST = 10


# ---------------------------------------------------------------------------
# Haversine distance (returns km)
# ---------------------------------------------------------------------------
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ---------------------------------------------------------------------------
# Geocoding via Nominatim (with disk cache)
# ---------------------------------------------------------------------------
def load_geocache():
    if GEOCACHE_JSON.exists():
        with open(GEOCACHE_JSON, encoding="utf-8", errors="replace") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                print("Warning: geocache file is corrupt, starting fresh.")
    return {}


def save_geocache(cache):
    with open(GEOCACHE_JSON, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=True)


def geocode_address(address, geocoder, cache):
    if address in cache:
        return cache[address]

    try:
        loc = geocoder.geocode(address, timeout=10)
        if loc:
            result = (loc.latitude, loc.longitude)
        else:
            result = None
    except Exception as e:
        print(f"  Geocode error for '{address}': {e}")
        result = None

    cache[address] = result
    time.sleep(1.1)  # Nominatim rate limit: 1 req/sec
    return result


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
def load_schools():
    schools = []
    with open(SCHOOLS_CSV, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            lat = row.get("Latitude", "").strip()
            lon = row.get("Longitude", "").strip()
            name = row.get("school_name", "").strip()
            school_id = row.get("school_id", "").strip()
            street = row.get("Street", "").strip()
            if lat and lon and name:
                schools.append({
                    "school_id": school_id,
                    "school_name": name,
                    "street": street,
                    "lat": float(lat),
                    "lon": float(lon),
                })
    return schools


def load_preschools():
    preschools = []
    with open(PRESCHOOLS_CSV, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            name = row.get("facility_name", "").strip()
            fid = row.get("facility_id", "").strip()
            street = row.get("street_address", "").strip()
            zipcode = row.get("zip_code", "").strip()
            capacity = row.get("recent_capacity", "").strip()
            detail_url = row.get("detail_url", "").strip()
            if name and street:
                preschools.append({
                    "facility_id": fid,
                    "facility_name": name,
                    "street_address": street,
                    "zip_code": zipcode,
                    "capacity": capacity,
                    "detail_url": detail_url,
                    "full_address": f"{street}, San Francisco, CA {zipcode}",
                })
    return preschools


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    schools = load_schools()
    print(f"Loaded {len(schools)} schools with coordinates")

    preschools = load_preschools()
    print(f"Loaded {len(preschools)} preschools with addresses")

    # Geocode preschools
    cache = load_geocache()
    needs_geocoding = [p for p in preschools if p["full_address"] not in cache]
    if needs_geocoding:
        print(f"Geocoding {len(needs_geocoding)} preschool addresses via Nominatim...")
        from geopy.geocoders import Nominatim
        geocoder = Nominatim(user_agent="sfusd-preschool-analysis/1.0")
        for i, p in enumerate(needs_geocoding):
            print(f"  [{i+1}/{len(needs_geocoding)}] {p['full_address']}")
            geocode_address(p["full_address"], geocoder, cache)
        save_geocache(cache)
        print("Geocache saved.")
    else:
        print("All preschool addresses already cached.")

    # Attach coordinates to preschools
    geocoded = []
    skipped = 0
    for p in preschools:
        coords = cache.get(p["full_address"])
        if coords:
            geocoded.append({**p, "lat": coords[0], "lon": coords[1]})
        else:
            skipped += 1
    print(f"{len(geocoded)} preschools geocoded, {skipped} skipped (no result)")

    if not geocoded:
        print("No geocoded preschools — cannot compute distances.")
        return

    # For each school, find N closest preschools
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "school_id", "school_name", "school_street",
        "rank",
        "facility_id", "facility_name", "preschool_street", "preschool_zip",
        "capacity", "distance_km", "distance_miles", "detail_url",
    ]

    rows = []
    for school in schools:
        distances = []
        for p in geocoded:
            km = haversine_km(school["lat"], school["lon"], p["lat"], p["lon"])
            distances.append((km, p))
        distances.sort(key=lambda x: x[0])

        for rank, (km, p) in enumerate(distances[:N_CLOSEST], start=1):
            rows.append({
                "school_id": school["school_id"],
                "school_name": school["school_name"],
                "school_street": school["street"],
                "rank": rank,
                "facility_id": p["facility_id"],
                "facility_name": p["facility_name"],
                "preschool_street": p["street_address"],
                "preschool_zip": p["zip_code"],
                "capacity": p["capacity"],
                "distance_km": round(km, 4),
                "distance_miles": round(km * 0.621371, 4),
                "detail_url": p["detail_url"],
            })

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
