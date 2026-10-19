#!/usr/bin/env python3
"""
Public vs. Private School Enrollment by Major US City

Ranks the largest US cities (Census places) by the share of school-age
children enrolled in public vs. private school, using recent ACS 5-year
estimates (table B14003). Reports elementary-age (5-9) and full K-12
(5-17) breakdowns separately, similar in spirit to Trulia's public vs.
private school research (https://www.trulia.com/research/private-vs-public-school/)
but at a national scale rather than SFUSD-specific attendance areas.
"""

import os
import sys

import pandas as pd
import requests

# All 50 states + DC (Census place-level detailed tables are queried per state)
STATE_FIPS = [
    '01', '02', '04', '05', '06', '08', '09', '10', '11', '12', '13', '15',
    '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27',
    '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39',
    '40', '41', '42', '44', '45', '46', '47', '48', '49', '50', '51', '53',
    '54', '55', '56',
]

# B14003: Sex by School Enrollment by Type of School by Age, population 3+
# Each entry: (public_var, private_var, not_enrolled_var) for one sex/age band
ENROLLMENT_VARS = {
    'male_5_9': ('B14003_005E', 'B14003_014E', 'B14003_023E'),
    'male_10_14': ('B14003_006E', 'B14003_015E', 'B14003_024E'),
    'male_15_17': ('B14003_007E', 'B14003_016E', 'B14003_025E'),
    'female_5_9': ('B14003_033E', 'B14003_042E', 'B14003_051E'),
    'female_10_14': ('B14003_034E', 'B14003_043E', 'B14003_052E'),
    'female_15_17': ('B14003_035E', 'B14003_044E', 'B14003_053E'),
}
ALL_VARS = sorted({v for triple in ENROLLMENT_VARS.values() for v in triple})


def get_api_key():
    key = os.environ.get('CENSUS_API_KEY', '').strip()
    if key:
        return key
    key = input(
        "\nEnter your Census API key (get one at "
        "https://api.census.gov/data/key_signup.html): "
    ).strip()
    if not key:
        print("ERROR: Census API key is required")
        sys.exit(1)
    return key


def census_get(year, get_vars, for_clause, in_clause, api_key):
    """Call the ACS 5-year API and return a DataFrame."""
    url = f"https://api.census.gov/data/{year}/acs/acs5"
    params = {
        'get': ','.join(get_vars),
        'for': for_clause,
        'in': in_clause,
        'key': api_key,
    }
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    rows = resp.json()
    return pd.DataFrame(rows[1:], columns=rows[0])


def fetch_top_places_by_population(year, top_n, api_key):
    """Fetch total population for every US place and return the top_n."""
    frames = []
    for i, state in enumerate(STATE_FIPS, 1):
        print(f"   [{i}/{len(STATE_FIPS)}] state {state}...", end='\r')
        df = census_get(
            year, ['NAME', 'B01003_001E'], 'place:*', f'state:{state}', api_key
        )
        df['state'] = state
        frames.append(df)
    print()
    places = pd.concat(frames, ignore_index=True)
    places['population'] = pd.to_numeric(places['B01003_001E'], errors='coerce')
    places['geoid'] = places['state'] + places['place']
    places = places.dropna(subset=['population'])
    places = places.sort_values('population', ascending=False).head(top_n)
    return places[['geoid', 'state', 'place', 'NAME', 'population']].reset_index(drop=True)


def fetch_enrollment_for_places(year, places, api_key):
    """Fetch B14003 enrollment variables for the given places, per state."""
    frames = []
    states_needed = places['state'].unique()
    for i, state in enumerate(states_needed, 1):
        print(f"   [{i}/{len(states_needed)}] state {state}...", end='\r')
        df = census_get(
            year, ['NAME'] + ALL_VARS, 'place:*', f'state:{state}', api_key
        )
        df['state'] = state
        frames.append(df)
    print()
    enrollment = pd.concat(frames, ignore_index=True)
    enrollment['geoid'] = enrollment['state'] + enrollment['place']
    for v in ALL_VARS:
        enrollment[v] = pd.to_numeric(enrollment[v], errors='coerce')
    return enrollment.merge(places[['geoid']], on='geoid', how='inner')


def summarize(enrollment):
    """Aggregate sex/age-band variables into elementary (5-9) and full K-12 (5-17) totals."""
    def total(bands):
        public = sum(enrollment[ENROLLMENT_VARS[b][0]] for b in bands)
        private = sum(enrollment[ENROLLMENT_VARS[b][1]] for b in bands)
        not_enrolled = sum(enrollment[ENROLLMENT_VARS[b][2]] for b in bands)
        return public, private, not_enrolled

    elem_bands = ['male_5_9', 'female_5_9']
    all_bands = list(ENROLLMENT_VARS.keys())

    out = enrollment[['geoid', 'NAME']].copy()
    out['elem_public'], out['elem_private'], out['elem_not_enrolled'] = total(elem_bands)
    out['k12_public'], out['k12_private'], out['k12_not_enrolled'] = total(all_bands)

    out['elem_total'] = out['elem_public'] + out['elem_private'] + out['elem_not_enrolled']
    out['k12_total'] = out['k12_public'] + out['k12_private'] + out['k12_not_enrolled']

    out['pct_public_elem'] = (out['elem_public'] / out['elem_total'] * 100).round(1)
    out['pct_private_elem'] = (out['elem_private'] / out['elem_total'] * 100).round(1)
    out['pct_public_k12'] = (out['k12_public'] / out['k12_total'] * 100).round(1)
    out['pct_private_k12'] = (out['k12_private'] / out['k12_total'] * 100).round(1)

    return out


def main():
    print("=" * 80)
    print("PUBLIC VS. PRIVATE SCHOOL ENROLLMENT BY MAJOR US CITY")
    print("=" * 80)

    year = int(os.environ.get('ACS_YEAR', '2023'))
    top_n = int(os.environ.get('TOP_N_CITIES', '100'))
    api_key = get_api_key()

    print(f"\n[1/4] Finding the {top_n} largest US cities (ACS {year} 5-year, all states)...")
    places = fetch_top_places_by_population(year, top_n, api_key)
    print(f"   ✓ Identified top {len(places)} places by population")

    print(f"\n[2/4] Fetching school enrollment by type (B14003) for those cities...")
    enrollment = fetch_enrollment_for_places(year, places, api_key)
    print(f"   ✓ Retrieved enrollment data for {len(enrollment)} places")

    print("\n[3/4] Aggregating elementary (5-9) and full K-12 (5-17) public/private shares...")
    summary = summarize(enrollment)
    results = places.merge(summary, on=['geoid', 'NAME'], how='left')
    results = results.sort_values('population', ascending=False).reset_index(drop=True)
    results.insert(0, 'rank', results.index + 1)
    print(f"   ✓ Summarized {len(results)} cities")

    print("\n[4/4] Saving results...")
    cols = [
        'rank', 'NAME', 'population',
        'elem_total', 'elem_public', 'elem_private', 'pct_public_elem', 'pct_private_elem',
        'k12_total', 'k12_public', 'k12_private', 'pct_public_k12', 'pct_private_k12',
    ]
    output = results[cols].rename(columns={'NAME': 'city'})
    output_file = 'national_public_private_enrollment.csv'
    output.to_csv(output_file, index=False)
    print(f"   ✓ Results saved to: {output_file}")

    print("\n" + "=" * 100)
    print(f"TOP 10 HIGHEST % PRIVATE (K-12, ages 5-17)")
    print("=" * 100)
    print(output.nlargest(10, 'pct_private_k12')[['city', 'population', 'pct_private_k12']].to_string(index=False))

    print(f"\nTOP 10 HIGHEST % PRIVATE (elementary, ages 5-9)")
    print("=" * 100)
    print(output.nlargest(10, 'pct_private_elem')[['city', 'population', 'pct_private_elem']].to_string(index=False))

    print(f"\nTOP 10 HIGHEST % PUBLIC (K-12, ages 5-17)")
    print("=" * 100)
    print(output.nlargest(10, 'pct_public_k12')[['city', 'population', 'pct_public_k12']].to_string(index=False))

    print("\nAnalysis complete!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nAnalysis interrupted by user")
        sys.exit(0)
    except requests.HTTPError as e:
        print(f"\n\nCensus API error: {e}")
        sys.exit(1)
