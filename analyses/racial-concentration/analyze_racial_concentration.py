#!/usr/bin/env python3
"""
SFUSD Racial Concentration by Elementary Attendance Area

Three views of racial composition are computed for each of the 58 SFUSD
elementary attendance areas using 2023 ACS 5-year estimates:

  1. All residents (B03002 — Hispanic or Latino Origin by Race)
  2. School-age children, ages 5–9 (B01001 race-specific Sex by Age tables)
  3. Children enrolled in grades K–5 (B14007 race-specific enrollment tables)

Note: ACS does not cross-tabulate race with public/private school type at the
census tract level. K–5 figures combine children in public and private schools.
For public/private enrollment by attendance area (without race breakdown), see
analyses/attendance-area-public-private/analyze_sfusd_enrollment.py.

Method: area-weighted spatial join between SF census tracts and SFUSD
elementary attendance areas (same approach as analyze_sfusd_enrollment.py).
"""

import geopandas as gpd
import pandas as pd
import requests
import sys

ACS_BASE = "https://api.census.gov/data/2023/acs/acs5"

# Racial/ethnic groups — mutually exclusive, aligned with B03002 categories.
# Each key is the column prefix; the value is the B01001/B14007 table suffix.
RACE_GROUPS = {
    'white':       'H',   # White alone, not Hispanic or Latino
    'black':       'B',   # Black or African American alone
    'aian':        'C',   # American Indian and Alaska Native alone
    'asian':       'D',   # Asian alone
    'nhpi':        'E',   # Native Hawaiian and Other Pacific Islander alone
    'other':       'F',   # Some other race alone
    'two_or_more': 'G',   # Two or more races
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

# ── ACS variable → column name mappings ──────────────────────────────────────

# B03002: total population by race (mutually exclusive Hispanic-origin scheme)
TOTAL_POP_VARS = {
    'B03002_001E': 'total',
    'B03002_003E': 'white',
    'B03002_004E': 'black',
    'B03002_005E': 'aian',
    'B03002_006E': 'asian',
    'B03002_007E': 'nhpi',
    'B03002_008E': 'other',
    'B03002_009E': 'two_or_more',
    'B03002_012E': 'hispanic',
}

def _build_age_vars():
    """B01001[suffix] — ages 5–9 by race: _004E = Male, _019E = Female."""
    m = {}
    for race, s in RACE_GROUPS.items():
        m[f'B01001{s}_004E'] = f'{race}_5_9_m'
        m[f'B01001{s}_019E'] = f'{race}_5_9_f'
    return m

def _build_enroll_vars():
    """B14007[suffix] — K–5 enrollment by race: _004E=K … _009E=Gr5."""
    grades = [('k', 4), ('1', 5), ('2', 6), ('3', 7), ('4', 8), ('5', 9)]
    m = {}
    for race, s in RACE_GROUPS.items():
        for label, num in grades:
            m[f'B14007{s}_{num:03d}E'] = f'{race}_gr{label}'
    return m

AGE_VARS    = _build_age_vars()     # 16 variables — within 50-var API limit
ENROLL_VARS = _build_enroll_vars()  # 48 variables — within 50-var API limit


# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_acs(key, variables, label):
    """Fetch ACS variables for all SF County (FIPS 075) tracts."""
    params = {
        'get': 'NAME,' + ','.join(variables),
        'for': 'tract:*',
        'in':  'state:06 county:075',
        'key': key,
    }
    r = requests.get(ACS_BASE, params=params)
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
    """Print a formatted table of attendance-area percentages for one view."""
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
    print("SFUSD RACIAL CONCENTRATION BY ELEMENTARY ATTENDANCE AREA")
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

    # ── [2/6] Census tracts ───────────────────────────────────────────────────
    print("\n[2/6] Loading San Francisco census tracts...")
    try:
        tracts = gpd.read_file(
            "https://www2.census.gov/geo/tiger/TIGER2023/TRACT/tl_2023_06_tract.zip"
        )
        sf_tracts = tracts[tracts['COUNTYFP'] == '075'].copy()
        sf_tracts['tract_id'] = sf_tracts['GEOID']
        sf_tracts = sf_tracts.to_crs(attendance_areas.crs)
        print(f"   ✓ Loaded {len(sf_tracts)} San Francisco census tracts")
    except Exception as e:
        print(f"   ✗ Error loading census tracts: {e}")
        sys.exit(1)

    # ── [3/6] Fetch ACS data (three tables) ──────────────────────────────────
    print("\n[3/6] Fetching ACS data from Census API (2023 ACS 5-year)...")
    try:
        df_total  = fetch_acs(census_api_key, list(TOTAL_POP_VARS.keys()),
                              "B03002  total population by race")
        df_age    = fetch_acs(census_api_key, list(AGE_VARS.keys()),
                              "B01001  school-age population ages 5–9 by race")
        df_enroll = fetch_acs(census_api_key, list(ENROLL_VARS.keys()),
                              "B14007  K–5 enrollment by race")
    except SystemExit:
        raise
    except Exception as e:
        print(f"   ✗ Error fetching ACS data: {e}")
        sys.exit(1)

    df_total  = df_total.rename(columns=TOTAL_POP_VARS)
    df_age    = df_age.rename(columns=AGE_VARS)
    df_enroll = df_enroll.rename(columns=ENROLL_VARS)

    # Combine male + female for ages 5–9
    for race in RACE_GROUPS:
        df_age[f'{race}_5_9'] = df_age[f'{race}_5_9_m'] + df_age[f'{race}_5_9_f']

    # Sum individual grades into K–5 total per race
    for race in RACE_GROUPS:
        df_enroll[f'{race}_k5'] = sum(
            df_enroll[f'{race}_gr{g}'] for g in ['k', '1', '2', '3', '4', '5']
        )

    total_cols  = list(TOTAL_POP_VARS.values())          # total, white, black, …
    age_cols    = [f'{r}_5_9' for r in RACE_GROUPS]
    enroll_cols = [f'{r}_k5'  for r in RACE_GROUPS]
    all_data_cols = total_cols + age_cols + enroll_cols

    # ── [4/6] Join ACS data to census tracts ─────────────────────────────────
    print("\n[4/6] Joining ACS data to census tracts...")
    sf_tracts = sf_tracts.merge(df_total[['tract_id']  + total_cols],  on='tract_id', how='left')
    sf_tracts = sf_tracts.merge(df_age[['tract_id']    + age_cols],    on='tract_id', how='left')
    sf_tracts = sf_tracts.merge(df_enroll[['tract_id'] + enroll_cols], on='tract_id', how='left')
    print(f"   ✓ Joined data to {len(sf_tracts)} tracts")

    # Pre-allocation totals — use these to validate the spatial join below.
    # After allocation, each measure should be ≤ the pre-allocation total (some
    # tract area falls outside all attendance areas, so coverage < 100%).
    pre_total     = sf_tracts['total'].sum()
    pre_total_5_9 = sum(sf_tracts[c].sum() for c in age_cols)
    pre_total_k5  = sum(sf_tracts[c].sum() for c in enroll_cols)
    print(f"\n   Pre-allocation SF totals (all {len(sf_tracts)} tracts):")
    print(f"     Total population (B03002):      {pre_total:>10,.0f}")
    print(f"     Ages 5–9 population (B01001):   {pre_total_5_9:>10,.0f}")
    print(f"     K–5 enrolled (B14007):          {pre_total_k5:>10,.0f}")

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

        # Validate: check that no tract's proportions sum to more than 1
        # (>1 would indicate overlapping attendance area geometries)
        prop_sums = intersections.groupby('tract_id')['proportion'].sum()
        over = prop_sums[prop_sums > 1.01]  # 1% tolerance for floating-point
        if len(over) > 0:
            print(f"   ⚠ WARNING: {len(over)} tracts have proportion sums > 1.01")
            print(f"     (max = {prop_sums.max():.3f}) — attendance area geometries may overlap,")
            print(f"     which would cause double-counting in the allocated totals.")
        else:
            print(f"   ✓ Proportion check passed (max tract proportion sum = {prop_sums.max():.3f})")
    except Exception as e:
        print(f"   ✗ Error in spatial join: {e}")
        sys.exit(1)

    # ── [6/6] Aggregate and report ────────────────────────────────────────────
    print("\n[6/6] Aggregating by attendance area...")
    alloc_cols = [f'{c}_alloc' for c in all_data_cols]
    results = intersections.groupby('e_aa_name')[alloc_cols].sum().round(0)
    results.columns = all_data_cols  # strip _alloc suffix

    # Post-allocation totals and coverage check
    post_total     = results['total'].sum()
    post_total_5_9 = results[age_cols].sum().sum()
    post_total_k5  = results[enroll_cols].sum().sum()
    print(f"\n   Post-allocation totals (sum across {len(results)} attendance areas):")
    print(f"     Total population:               {post_total:>10,.0f}  ({post_total/pre_total*100:.1f}% of pre-alloc SF total)")
    print(f"     Ages 5–9 population:            {post_total_5_9:>10,.0f}  ({post_total_5_9/pre_total_5_9*100:.1f}% of pre-alloc SF total)")
    print(f"     K–5 enrolled:                   {post_total_k5:>10,.0f}  ({post_total_k5/pre_total_k5*100:.1f}% of pre-alloc SF total)")
    print(f"\n   Note: coverage < 100% means some SF tract area falls outside all")
    print(f"   attendance areas (parks, water, commercial zones, etc.)")
    print(f"   Coverage > 100% would indicate overlapping attendance geometries.")

    groups = list(RACE_GROUPS.keys())

    # View 1: all residents (B03002)
    for col in groups:
        results[f'pct_{col}'] = (results[col] / results['total'] * 100).round(1)

    # View 2: school-age children ages 5–9 (B01001)
    results['total_5_9'] = results[age_cols].sum(axis=1)
    for col in groups:
        results[f'pct_{col}_5_9'] = (
            results[f'{col}_5_9'] / results['total_5_9'] * 100
        ).round(1)

    # View 3: K–5 enrolled children (B14007, public + private combined)
    results['total_k5'] = results[enroll_cols].sum(axis=1)
    for col in groups:
        results[f'pct_{col}_k5'] = (
            results[f'{col}_k5'] / results['total_k5'] * 100
        ).round(1)

    results = results.sort_index()

    # ── Tables ────────────────────────────────────────────────────────────────
    show_table(
        results,
        "ALL RESIDENTS — racial/ethnic composition of all residents (B03002)",
        'total',
        lambda c: f'pct_{c}',
        W,
    )
    show_table(
        results,
        "SCHOOL-AGE CHILDREN ages 5–9 — racial/ethnic composition (B01001)",
        'total_5_9',
        lambda c: f'pct_{c}_5_9',
        W,
    )
    show_table(
        results,
        "K–5 ENROLLED CHILDREN — racial/ethnic composition (B14007)\n"
        "  Public + private combined; ACS does not cross-tabulate race × school type at tract level.",
        'total_k5',
        lambda c: f'pct_{c}_k5',
        W,
    )

    # ── Citywide summaries ────────────────────────────────────────────────────
    print("\n" + "="*W)
    print("CITYWIDE TOTALS (area-weighted sum across all attendance areas)")
    print("="*W)
    for view_label, denom, count_fn in [
        ("All residents",         'total',     lambda c: c),
        ("School-age (ages 5–9)", 'total_5_9', lambda c: f'{c}_5_9'),
        ("K–5 enrolled",          'total_k5',  lambda c: f'{c}_k5'),
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
        ("K–5 enrolled",          'total_k5',  lambda c: f'pct_{c}_k5',  lambda c: f'{c}_k5'),
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
    output_file = 'sfusd_racial_concentration_by_attendance_area.csv'
    results.to_csv(output_file)
    print(f"\n✓ Results saved to: {output_file}")

    detailed_file = 'sfusd_racial_concentration_detailed_intersections.csv'
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
