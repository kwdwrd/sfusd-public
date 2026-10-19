#!/usr/bin/env python3
"""
SFUSD Racial Concentration by Elementary Attendance Area — 2020 Census

Uses 2020 Decennial Census data (actual counts, not survey estimates):

  1. All residents by race — P2 (Hispanic or Latino, and Not Hispanic or Latino
     by Race), 2020 Redistricting Data (PL 94-171). Mutually exclusive by design.

  2. School-age children ages 5–9 by race — P12 race-specific Sex by Age tables,
     2020 Demographic and Housing Characteristics (DHC).
     Caveat: P12 race-specific tables (B–I) are not mutually exclusive for
     Hispanic people who also identify with a non-White race, matching the same
     limitation as the ACS B01001 tables in analyze_racial_concentration.py.

Note: The Decennial Census does not include school enrollment data. For
K–5 enrollment by race, use the companion ACS script:
  analyses/racial-concentration/analyze_racial_concentration.py

Method: identical area-weighted spatial join used in the ACS script, applied
to 2020 TIGER census tract boundaries (matched to the 2020 Census vintage).
"""

import geopandas as gpd
import pandas as pd
import requests
import sys

PL_BASE  = "https://api.census.gov/data/2020/dec/pl"   # redistricting data
DHC_BASE = "https://api.census.gov/data/2020/dec/dhc"  # demographic detail

# P2: Hispanic or Latino, and Not Hispanic or Latino by Race.
# Variables are mutually exclusive by design — equivalent to ACS B03002.
TOTAL_POP_VARS = {
    'P2_001N': 'total',
    'P2_002N': 'hispanic',     # Hispanic or Latino (any race)
    'P2_005N': 'white',        # Not Hispanic: White alone
    'P2_006N': 'black',        # Not Hispanic: Black or African American alone
    'P2_007N': 'aian',         # Not Hispanic: AIAN alone
    'P2_008N': 'asian',        # Not Hispanic: Asian alone
    'P2_009N': 'nhpi',         # Not Hispanic: NHPI alone
    'P2_010N': 'other',        # Not Hispanic: Some Other Race alone
    'P2_011N': 'two_or_more',  # Not Hispanic: Two or More Races
}

# Race groups — DHC P12 table suffixes (same Census Bureau convention as ACS)
RACE_GROUPS = {
    'white':       'H',   # White alone, not Hispanic or Latino
    'black':       'B',   # Black or African American alone
    'aian':        'C',   # American Indian and Alaska Native alone
    'asian':       'D',   # Asian alone
    'nhpi':        'E',   # Native Hawaiian and Other Pacific Islander alone
    'other':       'F',   # Some Other Race alone
    'two_or_more': 'G',   # Two or More Races
    'hispanic':    'I',   # Hispanic or Latino (any race)
}

RACE_LABELS = {
    'white':       'White (non-Hispanic)',
    'black':       'Black or African American',
    'aian':        'American Indian / Alaska Native',
    'asian':       'Asian',
    'nhpi':        'Native Hawaiian / Pacific Islander',
    'other':       'Some Other Race',
    'two_or_more': 'Two or More Races',
    'hispanic':    'Hispanic or Latino',
}

# DHC P12[suffix]: Sex by Age (49 variables, full single-year-of-age groups)
#   Male 5–9 = _004N  (position 4 of 49)
#   Female 5–9 = _028N (position 28 of 49)
def _build_age_vars():
    m = {}
    for race, s in RACE_GROUPS.items():
        m[f'P12{s}_004N'] = f'{race}_5_9_m'
        m[f'P12{s}_028N'] = f'{race}_5_9_f'
    return m

AGE_VARS = _build_age_vars()  # 16 variables — within 50-var API limit


# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_census(base_url, key, variables, label):
    """Fetch variables for all SF County (FIPS 075) tracts from a Census API."""
    params = {
        'get': 'NAME,' + ','.join(variables),
        'for': 'tract:*',
        'in':  'state:06 county:075',
        'key': key,
    }
    r = requests.get(base_url, params=params)
    if r.status_code != 200:
        print(f"   ✗ API Error ({label}): {r.status_code} — {r.text[:300]}")
        sys.exit(1)
    data = r.json()
    df = pd.DataFrame(data[1:], columns=data[0])
    for v in variables:
        df[v] = pd.to_numeric(df[v], errors='coerce')
    df['tract_id'] = df['state'] + df['county'] + df['tract']
    print(f"   ✓ {label}: {len(df)} tracts")
    return df


def show_table(results, title, denom_col, pct_fn, W=120):
    print("\n" + "="*W)
    print(title)
    print("="*W)
    groups = list(RACE_GROUPS.keys())
    pct_cols = [pct_fn(c) for c in groups]
    disp = results[[denom_col] + pct_cols].copy()
    disp.columns = ['Total'] + [f'% {RACE_LABELS[c]}' for c in groups]
    print(disp.to_string())


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    W = 120

    print("="*W)
    print("SFUSD RACIAL CONCENTRATION BY ELEMENTARY ATTENDANCE AREA — 2020 CENSUS")
    print("="*W)

    census_api_key = input(
        "\nEnter your Census API key "
        "(get one at https://api.census.gov/data/key_signup.html): "
    ).strip()
    if not census_api_key:
        print("ERROR: Census API key is required")
        sys.exit(1)

    # ── [1/6] SFUSD attendance areas ─────────────────────────────────────────
    print("\n[1/6] Loading SFUSD elementary attendance areas...")
    try:
        attendance_areas = gpd.read_file("https://data.sfgov.org/resource/e6tr-sxwg.geojson")
        attendance_areas = attendance_areas[attendance_areas['e_aa_name'].notna()].copy()
        attendance_areas = attendance_areas[['e_aa_name', 'e_aa_schno', 'geometry']]
        if attendance_areas.crs is None:
            attendance_areas.set_crs(epsg=4326, inplace=True)
        print(f"   ✓ Loaded {len(attendance_areas)} elementary attendance areas")
    except Exception as e:
        print(f"   ✗ Error loading attendance areas: {e}")
        sys.exit(1)

    # ── [2/6] 2020 Census tracts ──────────────────────────────────────────────
    print("\n[2/6] Loading San Francisco 2020 census tracts...")
    try:
        tracts = gpd.read_file(
            "https://www2.census.gov/geo/tiger/TIGER2020/TRACT/tl_2020_06_tract.zip"
        )
        sf_tracts = tracts[tracts['COUNTYFP'] == '075'].copy()
        sf_tracts['tract_id'] = sf_tracts['GEOID']
        sf_tracts = sf_tracts.to_crs(attendance_areas.crs)
        print(f"   ✓ Loaded {len(sf_tracts)} San Francisco census tracts")
    except Exception as e:
        print(f"   ✗ Error loading census tracts: {e}")
        sys.exit(1)

    # ── [3/6] Fetch Census data ───────────────────────────────────────────────
    print("\n[3/6] Fetching 2020 Census data...")
    try:
        df_total = fetch_census(PL_BASE, census_api_key, list(TOTAL_POP_VARS.keys()),
                                "P2   total population by race (redistricting data)")
        df_age   = fetch_census(DHC_BASE, census_api_key, list(AGE_VARS.keys()),
                                "P12  ages 5–9 by race (DHC)")
    except SystemExit:
        raise
    except Exception as e:
        print(f"   ✗ Error fetching Census data: {e}")
        sys.exit(1)

    df_total = df_total.rename(columns=TOTAL_POP_VARS)
    df_age   = df_age.rename(columns=AGE_VARS)

    for race in RACE_GROUPS:
        df_age[f'{race}_5_9'] = df_age[f'{race}_5_9_m'] + df_age[f'{race}_5_9_f']

    total_cols = list(TOTAL_POP_VARS.values())   # total, hispanic, white, …
    age_cols   = [f'{r}_5_9' for r in RACE_GROUPS]
    all_data_cols = total_cols + age_cols

    # ── [4/6] Join Census data to tracts ─────────────────────────────────────
    print("\n[4/6] Joining Census data to tracts...")
    sf_tracts = sf_tracts.merge(df_total[['tract_id'] + total_cols], on='tract_id', how='left')
    sf_tracts = sf_tracts.merge(df_age[['tract_id']   + age_cols],   on='tract_id', how='left')
    print(f"   ✓ Joined data to {len(sf_tracts)} tracts")

    # Pre-allocation totals
    pre_total     = sf_tracts['total'].sum()
    pre_total_5_9 = sum(sf_tracts[c].sum() for c in age_cols)
    print(f"\n   Pre-allocation SF totals (all {len(sf_tracts)} tracts):")
    print(f"     Total population (P2):          {pre_total:>10,.0f}")
    print(f"     Ages 5–9 population (P12):      {pre_total_5_9:>10,.0f}")

    # ── [5/6] Area-weighted spatial join ─────────────────────────────────────
    print("\n[5/6] Performing spatial join (area-weighted allocation)...")
    try:
        intersections = gpd.overlay(sf_tracts, attendance_areas, how='intersection')
        intersections['intersection_area'] = intersections.geometry.area
        tract_areas = sf_tracts.set_index('tract_id').geometry.area
        intersections['tract_area'] = intersections['tract_id'].map(tract_areas)
        intersections['proportion'] = intersections['intersection_area'] / intersections['tract_area']
        for col in all_data_cols:
            intersections[f'{col}_alloc'] = intersections[col] * intersections['proportion']
        print(f"   ✓ Created {len(intersections)} tract-attendance area intersections")

        prop_sums = intersections.groupby('tract_id')['proportion'].sum()
        over = prop_sums[prop_sums > 1.01]
        if len(over) > 0:
            print(f"   ⚠ WARNING: {len(over)} tracts have proportion sums > 1.01")
            print(f"     (max = {prop_sums.max():.3f}) — attendance area geometries may overlap")
        else:
            print(f"   ✓ Proportion check passed (max tract proportion sum = {prop_sums.max():.3f})")
    except Exception as e:
        print(f"   ✗ Error in spatial join: {e}")
        sys.exit(1)

    # ── [6/6] Aggregate and report ────────────────────────────────────────────
    print("\n[6/6] Aggregating by attendance area...")
    alloc_cols = [f'{c}_alloc' for c in all_data_cols]
    results = intersections.groupby('e_aa_name')[alloc_cols].sum().round(0)
    results.columns = all_data_cols

    groups = list(RACE_GROUPS.keys())

    # Post-allocation coverage check
    post_total     = results['total'].sum()
    post_total_5_9 = results[age_cols].sum().sum()
    print(f"\n   Post-allocation totals (sum across {len(results)} attendance areas):")
    print(f"     Total population:               {post_total:>10,.0f}  ({post_total/pre_total*100:.1f}% of SF total)")
    print(f"     Ages 5–9 population:            {post_total_5_9:>10,.0f}  ({post_total_5_9/pre_total_5_9*100:.1f}% of SF total)")

    # View 1: all residents (P2 — mutually exclusive)
    for col in groups:
        results[f'pct_{col}'] = (results[col] / results['total'] * 100).round(1)

    # View 2: school-age children ages 5–9 (P12)
    results['total_5_9'] = results[age_cols].sum(axis=1)
    for col in groups:
        results[f'pct_{col}_5_9'] = (
            results[f'{col}_5_9'] / results['total_5_9'] * 100
        ).round(1)

    results = results.sort_index()

    # ── Tables ────────────────────────────────────────────────────────────────
    show_table(
        results,
        "ALL RESIDENTS — racial/ethnic composition of all residents (P2, 2020 Census)",
        'total',
        lambda c: f'pct_{c}',
        W,
    )
    show_table(
        results,
        "SCHOOL-AGE CHILDREN ages 5–9 — racial/ethnic composition (P12 DHC, 2020 Census)\n"
        "  Note: non-White Hispanic residents may be double-counted across racial and Hispanic categories.",
        'total_5_9',
        lambda c: f'pct_{c}_5_9',
        W,
    )

    # ── Citywide summaries ────────────────────────────────────────────────────
    print("\n" + "="*W)
    print("CITYWIDE TOTALS (area-weighted sum across all attendance areas)")
    print("="*W)
    for view_label, denom, count_fn in [
        ("All residents",         'total',     lambda c: c),
        ("School-age (ages 5–9)", 'total_5_9', lambda c: f'{c}_5_9'),
    ]:
        total_n = results[denom].sum()
        print(f"\n  {view_label}: {total_n:,.0f}")
        for col in groups:
            n = results[count_fn(col)].sum()
            print(f"    {RACE_LABELS[col]:<45} {n:>8,.0f}  ({n/total_n*100:.1f}%)")

    # ── Most concentrated attendance areas ────────────────────────────────────
    print("\n" + "="*W)
    print("MOST CONCENTRATED ATTENDANCE AREAS BY RACIAL GROUP")
    print("="*W)
    for view_label, denom, pct_fn, count_fn in [
        ("All residents",         'total',     lambda c: f'pct_{c}',     lambda c: c),
        ("School-age (ages 5–9)", 'total_5_9', lambda c: f'pct_{c}_5_9', lambda c: f'{c}_5_9'),
    ]:
        print(f"\n  ── {view_label} ──")
        for col in groups:
            pct_col   = pct_fn(col)
            count_col = count_fn(col)
            top5 = results.nlargest(5, pct_col)[[denom, count_col, pct_col]].copy()
            top5.columns = ['Total', RACE_LABELS[col], f'% {RACE_LABELS[col]}']
            print(f"\n    {RACE_LABELS[col]}:")
            print(top5.to_string(index=True))

    # ── Save outputs ──────────────────────────────────────────────────────────
    output_file = 'sfusd_racial_concentration_census2020_by_attendance_area.csv'
    results.to_csv(output_file)
    print(f"\n✓ Results saved to: {output_file}")

    detailed_file = 'sfusd_racial_concentration_census2020_detailed_intersections.csv'
    intersections.drop(columns=['geometry']).to_csv(detailed_file, index=False)
    print(f"✓ Detailed intersections saved to: {detailed_file}")

    print("\n" + "="*W)
    print("Analysis complete!")
    print("="*W)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nAnalysis interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
