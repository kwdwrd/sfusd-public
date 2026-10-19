#!/usr/bin/env python3
"""
ACS Data Smoothing Analysis — Racial Composition in SF Census Tracts

For each SF census tract, fetches:
  - ACS 5-year racial composition (B03002) for years 2019–2024
  - ACS 5-year ages 5-9 by race (B01001 race-specific tables) for years 2019–2024
  - 2020 Decennial Census racial composition (P2)
  - 2020 Decennial Census ages 5-9 by race (P12 DHC)

Outputs
-------
acs_tract_racial_composition.csv
    Long-format table: tract_id | year | source | group | race | count | total | pct
    group = 'total_pop' or 'ages_5_9'

plots/histogram_<group>_<race>.png  (one per racial group × population group)
    KDE distribution of racial % across tracts, ACS years as overlapping
    series, Census 2020 as a dashed line. Illustrates the smoothing effect
    of 5-year ACS averaging vs. a point-in-time Census count.

plots/scatterplot_<group>_<race>.png  (one per racial group × population group)
    ACS % (y) vs 2020 Census % (x), one point per tract. Each ACS year is
    a separate series. The y=x diagonal represents perfect agreement.

Note on tract boundaries
------------------------
ACS 2019–2021 use 2010 Census tract definitions.
ACS 2022–2024 and the 2020 Decennial Census use 2020 tract definitions.
Tracts from 2022–2024 can be matched exactly to Census 2020 tract IDs.
Tracts from 2019–2021 are plotted with hollow markers in the scatterplot
to flag the boundary mismatch.

Note on ages 5-9 racial data
-----------------------------
ACS B01001 and Census P12 race-specific tables are NOT mutually exclusive:
Hispanic respondents who also identify with a non-White race appear in both
the race-specific table (B01001B–G / P12B–G) and the Hispanic table
(B01001I / P12I). Within-group comparisons across years remain valid;
citywide totals will be modestly overstated for overlapping groups.
The total denominator uses the base B01001/P12 table (not double-counted).
"""

import os
import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from scipy.stats import gaussian_kde

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
PLOTS_DIR  = os.path.join(OUTPUT_DIR, 'plots')

# ── Variable definitions ──────────────────────────────────────────────────────

# ACS B03002: Hispanic or Latino Origin by Race (mutually exclusive)
ACS_VARS = {
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

# 2020 Census P2: Hispanic or Latino, and Not Hispanic or Latino by Race
CENSUS_VARS = {
    'P2_001N': 'total',
    'P2_002N': 'hispanic',
    'P2_005N': 'white',
    'P2_006N': 'black',
    'P2_007N': 'aian',
    'P2_008N': 'asian',
    'P2_009N': 'nhpi',
    'P2_010N': 'other',
    'P2_011N': 'two_or_more',
}

# Race suffixes for race-specific iteration tables (B01001X, P12X)
RACE_SUFFIXES = {
    'white':       'H',   # White alone, not Hispanic or Latino
    'black':       'B',   # Black or African American alone
    'aian':        'C',   # American Indian and Alaska Native alone
    'asian':       'D',   # Asian alone
    'nhpi':        'E',   # Native Hawaiian and Other Pacific Islander alone
    'other':       'F',   # Some Other Race alone
    'two_or_more': 'G',   # Two or More Races
    'hispanic':    'I',   # Hispanic or Latino (any race)
}

# ACS B01001 ages 5-9 by race.
# Base B01001 (49-var structure): male 5-9 = _004E, female 5-9 = _028E.
# Race-specific B01001X (31-var condensed structure): male = _004E, female = _019E.
# Base table provides the clean total denominator (no double-counting).
def _build_acs_age_vars():
    m = {
        'B01001_004E': 'total_m',   # base table, male 5-9 (all races)
        'B01001_028E': 'total_f',   # base table, female 5-9 (all races)
    }
    for race, s in RACE_SUFFIXES.items():
        m[f'B01001{s}_004E'] = f'{race}_m'   # race-specific, male 5-9
        m[f'B01001{s}_019E'] = f'{race}_f'   # race-specific, female 5-9
    return m

ACS_AGE_VARS = _build_acs_age_vars()   # 18 variables — within 50-var API limit

# Census 2020 P12 DHC ages 5-9 by race.
# Both base P12 and race-specific P12X tables share the 49-var structure:
#   male 5-9 = _004N, female 5-9 = _028N.
def _build_census_age_vars():
    m = {
        'P12_004N': 'total_m',   # base table, male 5-9 (all races)
        'P12_028N': 'total_f',   # base table, female 5-9 (all races)
    }
    for race, s in RACE_SUFFIXES.items():
        m[f'P12{s}_004N'] = f'{race}_m'
        m[f'P12{s}_028N'] = f'{race}_f'
    return m

CENSUS_AGE_VARS = _build_census_age_vars()  # 18 variables — within 50-var API limit

RACE_GROUPS = ['white', 'black', 'aian', 'asian', 'nhpi', 'other', 'two_or_more', 'hispanic']
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

GROUP_LABELS = {
    'total_pop': 'All Residents',
    'ages_5_9':  'Children Ages 5–9',
}

ACS_YEARS = [2019, 2020, 2021, 2022, 2023, 2024]

# ACS switched from 2010 to 2020 tract definitions starting with the 2022 vintage.
# Only 2022+ tracts share IDs with the 2020 Census.
BOUNDARY_CHANGE_YEAR = 2022

# ── Colours ───────────────────────────────────────────────────────────────────

_cmap = plt.get_cmap('plasma')
ACS_COLORS = {yr: _cmap(i / (len(ACS_YEARS) - 1)) for i, yr in enumerate(ACS_YEARS)}
CENSUS_COLOR = '#2ca02c'   # green, distinct from the plasma ramp


# ── Data helpers ──────────────────────────────────────────────────────────────

def fetch_sf_tracts(url, api_key, variables):
    """Return a DataFrame of Census API variables for all SF county tracts."""
    params = {
        'get': 'NAME,' + ','.join(variables),
        'for': 'tract:*',
        'in':  'state:06 county:075',
        'key': api_key,
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    df = pd.DataFrame(data[1:], columns=data[0])
    for v in variables:
        df[v] = pd.to_numeric(df[v], errors='coerce')
    df['tract_id'] = df['state'] + df['county'] + df['tract']
    return df


def to_long(df, var_map, year, source, group):
    """
    Pivot a wide Census API DataFrame into long format.
    Returns: tract_id | year | source | group | race | count | total | pct
    """
    df = df.rename(columns=var_map).copy()
    df = df[df['total'].notna() & (df['total'] > 0)]

    long = df.melt(
        id_vars=['tract_id', 'total'],
        value_vars=RACE_GROUPS,
        var_name='race',
        value_name='count',
    )
    long['year']   = year
    long['source'] = source
    long['group']  = group
    long['pct']    = (long['count'] / long['total'] * 100).round(4)
    return long[['tract_id', 'year', 'source', 'group', 'race', 'count', 'total', 'pct']]


def to_long_age(df, var_map, year, source):
    """
    Process ages 5-9 race data: combine male + female counts, compute pct
    against the total ages 5-9 from the base table (clean, not double-counted).
    Returns: tract_id | year | source | group | race | count | total | pct
    """
    df = df.rename(columns=var_map).copy()
    df['total'] = df['total_m'] + df['total_f']
    df = df[df['total'].notna() & (df['total'] > 0)]

    for race in RACE_GROUPS:
        df[race] = df[f'{race}_m'] + df[f'{race}_f']

    long = df.melt(
        id_vars=['tract_id', 'total'],
        value_vars=RACE_GROUPS,
        var_name='race',
        value_name='count',
    )
    long['year']   = year
    long['source'] = source
    long['group']  = 'ages_5_9'
    long['pct']    = (long['count'] / long['total'] * 100).round(4)
    return long[['tract_id', 'year', 'source', 'group', 'race', 'count', 'total', 'pct']]


# ── Plot helpers ──────────────────────────────────────────────────────────────

_KDE_GRID = np.linspace(0, 100, 500)


def _bounded_kde(vals):
    """
    Boundary-reflected KDE on [0, 100], normalized to integrate to 1.

    Reflects data at both boundaries (x → -x at 0, x → 200-x at 100)
    before fitting the KDE. This folds tail mass back into [0, 100] at the
    correct locations — right-side mass returns near 100, left-side near 0 —
    rather than redistributing it uniformly via global renormalization.
    The final trapz normalization corrects for the 3× data augmentation.
    """
    x         = vals.values
    augmented = np.concatenate([x, -x, 200 - x])
    # gaussian_kde kernel bandwidth = bw_method * std(dataset).
    # Reflection inflates std(augmented) >> std(x), so we rescale the factor
    # to keep the effective kernel bandwidth anchored to the original data.
    bw        = 0.4 * np.std(x, ddof=1) / np.std(augmented, ddof=1)
    kde       = gaussian_kde(augmented, bw_method=bw)
    density   = kde(_KDE_GRID)
    density   = np.clip(density, 0, None)
    area      = np.trapezoid(density, _KDE_GRID)
    if area > 0:
        density /= area
    return density


_BIN_EDGES   = np.arange(0, 105, 5)          # 5-percentage-point bins
_BIN_CENTERS = (_BIN_EDGES[:-1] + _BIN_EDGES[1:]) / 2


def plot_histogram(data, race, group):
    """
    Two-panel figure per race × group:
      Top:    Boundary-reflected KDE curves (density, integrates to 1).
      Bottom: Grouped bar chart of raw tract counts per 5-ppt bin.
              Bars for each series are adjacent within each bin.
              Census 2020 bars are hollow (outline only) to match the
              dashed-line convention in the KDE panel.
    """
    sub         = data[data['group'] == group]
    group_label = GROUP_LABELS[group]

    # Collect all plottable series in display order
    all_series = []
    for year in ACS_YEARS:
        vals = (
            sub[(sub['source'] == 'acs') & (sub['year'] == year) & (sub['race'] == race)]
            ['pct'].dropna()
        )
        if len(vals) >= 5:
            all_series.append((f'ACS {year} (5-yr)', ACS_COLORS[year], False, vals))

    census_vals = (
        sub[(sub['source'] == 'census2020') & (sub['race'] == race)]
        ['pct'].dropna()
    )
    if len(census_vals) >= 5:
        all_series.append(('Census 2020', CENSUS_COLOR, True, census_vals))

    fig, (ax_kde, ax_bar) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

    # ── KDE panel ─────────────────────────────────────────────────────────────
    for label, color, is_census, vals in all_series:
        ax_kde.plot(
            _KDE_GRID, _bounded_kde(vals),
            color=color,
            linewidth=2.5 if is_census else 1.8,
            linestyle='--' if is_census else '-',
            label=label,
        )
    ax_kde.set_ylabel('Density', fontsize=11)
    ax_kde.legend(fontsize=9)
    ax_kde.grid(axis='y', alpha=0.3)

    # ── Bar chart panel ────────────────────────────────────────────────────────
    n_series  = len(all_series)
    bar_width = 4.5 / n_series   # bins are 5 ppt wide; leave a small gap

    for i, (label, color, is_census, vals) in enumerate(all_series):
        counts, _ = np.histogram(vals, bins=_BIN_EDGES)
        x_pos     = _BIN_CENTERS + (i - (n_series - 1) / 2) * bar_width
        if is_census:
            ax_bar.bar(x_pos, counts, width=bar_width,
                       facecolor='none', edgecolor=color, linewidth=1.2,
                       label=label)
        else:
            ax_bar.bar(x_pos, counts, width=bar_width,
                       color=color, alpha=0.8, label=label)

    ax_bar.set_xlim(0, 100)
    ax_bar.set_xlabel(f'% {RACE_LABELS[race]} in census tract', fontsize=11)
    ax_bar.set_ylabel('Tract count', fontsize=11)
    ax_bar.legend(fontsize=9)
    ax_bar.grid(axis='y', alpha=0.3)

    fig.suptitle(
        f'{RACE_LABELS[race]} — {group_label}\n'
        f'Distribution across SF census tracts · ACS 5-year (2019–2024) vs 2020 Census',
        fontsize=12,
    )

    filename = f'histogram_{group}_{race}.png'
    fig.tight_layout()
    fig.savefig(os.path.join(PLOTS_DIR, filename), dpi=150)
    plt.close(fig)
    print(f"      {filename}")


_SIZE_SCALE = 2.5   # marker area = SIZE_SCALE² × census_total  (points²)


def plot_scatterplot(data, race, group):
    """
    ACS % (y-axis) vs 2020 Census % (x-axis), one point per tract.
    ACS years 2022–2024 (matching 2020 tract boundaries): filled markers.
    ACS years 2019–2021 (2010 tract boundaries, approximate): hollow markers.
    The y=x diagonal indicates perfect agreement with the Census.
    Point size is proportional to √(Census 2020 tract total) so that scatter
    near small-population tracts is visually distinguished from scatter near
    large ones — testing whether ACS/Census discrepancy shrinks with n.
    """
    sub         = data[data['group'] == group]
    group_label = GROUP_LABELS[group]

    # Include 'total' so it can drive point sizing
    census_sub = (
        sub[(sub['source'] == 'census2020') & (sub['race'] == race)]
        [['tract_id', 'pct', 'total']]
        .rename(columns={'pct': 'census_pct', 'total': 'census_total'})
    )

    fig, ax = plt.subplots(figsize=(8, 6))

    # Reference diagonal
    ax.plot([0, 100], [0, 100], color='black', linewidth=0.8,
            linestyle=':', alpha=0.5, zorder=1)

    for year in ACS_YEARS:
        acs_sub = (
            sub[(sub['source'] == 'acs') & (sub['year'] == year) & (sub['race'] == race)]
            [['tract_id', 'pct']]
            .rename(columns={'pct': 'acs_pct'})
        )
        merged = acs_sub.merge(census_sub, on='tract_id').dropna()
        if merged.empty:
            continue

        sizes   = np.sqrt(np.clip(merged['census_total'], 1, None)) * _SIZE_SCALE
        matched = year >= BOUNDARY_CHANGE_YEAR
        label   = f'ACS {year}' + ('' if matched else ' †')
        ax.scatter(
            merged['census_pct'],
            merged['acs_pct'],
            s=sizes,
            color=ACS_COLORS[year],
            facecolors=ACS_COLORS[year] if matched else 'none',
            edgecolors=ACS_COLORS[year],
            alpha=0.65,
            linewidths=0.8,
            label=label,
            zorder=2,
        )

    # ── Year / boundary legend ────────────────────────────────────────────────
    year_legend = ax.legend(
        fontsize=8, markerscale=1.2, loc='upper left',
        title='† 2010 tract boundaries\n  (approximate match)', title_fontsize=7,
    )
    ax.add_artist(year_legend)

    # ── Size legend (census tract total) ─────────────────────────────────────
    ref_ns    = [20, 100, 250] if group == 'ages_5_9' else [500, 2000, 5000]
    size_hdls = [
        ax.scatter([], [], s=np.sqrt(n) * _SIZE_SCALE, color='gray',
                   alpha=0.6, label=str(n))
        for n in ref_ns
    ]
    size_title = 'Census ages 5–9\n(tract count)' if group == 'ages_5_9' \
                 else 'Census population\n(tract total)'
    ax.legend(handles=size_hdls, fontsize=8, loc='lower right',
              title=size_title, title_fontsize=7)

    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    ax.set_xlabel(f'2020 Census: % {RACE_LABELS[race]}', fontsize=11)
    ax.set_ylabel(f'ACS 5-year: % {RACE_LABELS[race]}', fontsize=11)
    ax.set_title(
        f'{RACE_LABELS[race]} — {group_label}\n'
        f'ACS estimates vs 2020 Census · SF census tracts · size ∝ √(census total)',
        fontsize=11,
    )
    ax.grid(alpha=0.3)

    filename = f'scatterplot_{group}_{race}.png'
    fig.tight_layout()
    fig.savefig(os.path.join(PLOTS_DIR, filename), dpi=150)
    plt.close(fig)
    print(f"      {filename}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("="*70)
    print("ACS VARIANCE ANALYSIS — RACIAL COMPOSITION IN SF CENSUS TRACTS")
    print("="*70)

    api_key = input("\nEnter your Census API key: ").strip()
    if not api_key:
        print("ERROR: Census API key is required")
        sys.exit(1)

    os.makedirs(PLOTS_DIR, exist_ok=True)

    frames = []

    # ── [1/5] Fetch ACS 5-year total population data ──────────────────────────
    print("\n[1/5] Fetching ACS 5-year total population data (B03002)...")
    for year in ACS_YEARS:
        url = f"https://api.census.gov/data/{year}/acs/acs5"
        try:
            df   = fetch_sf_tracts(url, api_key, list(ACS_VARS.keys()))
            long = to_long(df, ACS_VARS, year, 'acs', 'total_pop')
            frames.append(long)
            boundary = '2020' if year >= BOUNDARY_CHANGE_YEAR else '2010'
            print(f"   ✓ ACS {year}  ({len(df)} tracts, {boundary} boundaries)")
        except Exception as e:
            print(f"   ✗ ACS {year}: {e}")

    # ── [2/5] Fetch ACS 5-year ages 5-9 data ─────────────────────────────────
    print("\n[2/5] Fetching ACS 5-year ages 5-9 data (B01001 race-specific)...")
    for year in ACS_YEARS:
        url = f"https://api.census.gov/data/{year}/acs/acs5"
        try:
            df   = fetch_sf_tracts(url, api_key, list(ACS_AGE_VARS.keys()))
            long = to_long_age(df, ACS_AGE_VARS, year, 'acs')
            frames.append(long)
            boundary = '2020' if year >= BOUNDARY_CHANGE_YEAR else '2010'
            print(f"   ✓ ACS {year}  ({len(df)} tracts, {boundary} boundaries)")
        except Exception as e:
            print(f"   ✗ ACS {year}: {e}")

    # ── [3/5] Fetch 2020 Census total population data ─────────────────────────
    print("\n[3/5] Fetching 2020 Census total population data (P2)...")
    try:
        df_c   = fetch_sf_tracts(
            "https://api.census.gov/data/2020/dec/pl",
            api_key,
            list(CENSUS_VARS.keys()),
        )
        long_c = to_long(df_c, CENSUS_VARS, 2020, 'census2020', 'total_pop')
        frames.append(long_c)
        print(f"   ✓ Census 2020 P2  ({len(df_c)} tracts)")
    except Exception as e:
        print(f"   ✗ Census 2020 P2: {e}")

    # ── [4/5] Fetch 2020 Census ages 5-9 data ────────────────────────────────
    print("\n[4/5] Fetching 2020 Census ages 5-9 data (P12 DHC)...")
    try:
        df_ca  = fetch_sf_tracts(
            "https://api.census.gov/data/2020/dec/dhc",
            api_key,
            list(CENSUS_AGE_VARS.keys()),
        )
        long_ca = to_long_age(df_ca, CENSUS_AGE_VARS, 2020, 'census2020')
        frames.append(long_ca)
        print(f"   ✓ Census 2020 P12  ({len(df_ca)} tracts)")
    except Exception as e:
        print(f"   ✗ Census 2020 P12: {e}")

    if not frames:
        print("No data fetched. Exiting.")
        sys.exit(1)

    data = pd.concat(frames, ignore_index=True)

    # Summary
    n_tracts = data['tract_id'].nunique()
    n_rows   = len(data)
    print(f"\n   Combined: {n_rows:,} rows  ·  {n_tracts} unique tract IDs")
    print(f"   Years:    {sorted(data[data['source']=='acs']['year'].unique())}")

    # ── Save long-format CSV ──────────────────────────────────────────────────
    csv_path = os.path.join(OUTPUT_DIR, 'acs_tract_racial_composition.csv')
    data.to_csv(csv_path, index=False)
    print(f"\n✓ {os.path.basename(csv_path)}  ({n_rows:,} rows)")

    # ── [5/5] Generate plots ──────────────────────────────────────────────────
    print(f"\n[5/5] Generating plots → {PLOTS_DIR}")
    for group in ['total_pop', 'ages_5_9']:
        print(f"\n   {GROUP_LABELS[group]}:")
        for race in RACE_GROUPS:
            print(f"      {RACE_LABELS[race]}:")
            plot_histogram(data, race, group)
            plot_scatterplot(data, race, group)

    print("\n" + "="*70)
    print("Done. Output written to:", OUTPUT_DIR)
    print(f"Plots saved to: {PLOTS_DIR}")
    print("="*70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(0)
    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(1)
