#!/usr/bin/env python3
"""
The introduction's enrollment-revenue counterfactual, as a traceable calculation.

Claim: if a mechanism change moved San Francisco's private-schooling rate from
its current level to the national average, SFUSD's LCFF revenue would rise by
$X per year. Every input is either read from the CDE enrollment file the rest of
the pipeline already uses, computed from data/processed/calpads.csv, or a
documented external constant below.

LCFF (CDE 2024-25 Principal Apportionment funding rates,
https://www.cde.ca.gov/fg/aa/pa/pa2425rates.asp):

    LCFF = base * ADA
         + SUPPLEMENTAL * base * n_unduplicated
         + CONCENTRATION * base * max(0, n_unduplicated - THRESHOLD * ADA)

The concentration term is why the marginal student is worth less than SFUSD's
average: a student arriving from private school is unlikely to be an
unduplicated pupil, so they raise ADA without raising n_unduplicated, which
dilutes concentration funding for every student already enrolled. SFUSD is above
the threshold today (UPP ~60%), and a large enough influx pushes it below, at
which point the concentration grant disappears entirely.

Two modelling choices are deliberately exposed as constants rather than buried,
because they move the answer and are the authors' call:
  - BASE_GRANT_MODE: 'blended' weights the grade-span rates by SFUSD's own grade
    distribution (the default, since the counterfactual is district-wide);
    'tk3' uses the TK-3 rate alone.
  - INCLUDE_CHARTERS: False (the default) counts only non-charter SFUSD, since
    LCFF for charter students flows to the charter rather than the district.
    True treats all SFUSD-area public enrollment as the district's.

Run from within this directory: `python3 lcff_counterfactual.py`.
Writes output/lcff_counterfactual.csv.
"""

import csv
import sys

import pandas as pd

sys.path.insert(0, 'src')

ENROLLMENT_FILE = '../../data/enrollment/raw/cdenroll2324-v2.txt'
CALPADS_FILE = '../../data/processed/calpads.csv'
OUT = 'output/lcff_counterfactual.csv'

# --- LCFF structure, CDE 2024-25 Principal Apportionment funding rates -------
BASE_GRANT = {            # per ADA, after grade-span adjustment
    'tk3': 11_068.0,      # $10,025 + 10.4% adjustment
    '4_6': 10_177.0,
    '7_8': 10_478.0,
    '9_12': 12_460.0,     # $12,144 + 2.6% adjustment
}
GRADE_SPAN = {
    'tk3': ['GR_TK', 'GR_KN', 'GR_01', 'GR_02', 'GR_03'],
    '4_6': ['GR_04', 'GR_05', 'GR_06'],
    '7_8': ['GR_07', 'GR_08'],
    '9_12': ['GR_09', 'GR_10', 'GR_11', 'GR_12'],
}
SUPPLEMENTAL_RATE = 0.20
CONCENTRATION_RATE = 0.65
CONCENTRATION_THRESHOLD = 0.55

# --- SFUSD actuals, 2024-25 1st Interim -------------------------------------
# SFUSD Board of Education, 2024-25 First Interim Report: LCFF of $634.4M on
# funded ADA of 45,682, i.e. $13,887 per ADA.
SFUSD_LCFF_ACTUAL = 634_400_000.0
SFUSD_FUNDED_ADA = 45_682.0

# --- external constants ------------------------------------------------------
# San Francisco private-school share of K-12. KQED (2024), citing state data,
# reports 30% for 2023-24; an independent count puts it at 31%
# (23,964 private against 52,715 public).
SF_PRIVATE_SHARE = 0.30
# National private-school share of K-12 enrollment, about 9% (NCES, fall 2021
# and fall 2019). NB: the draft previously said 15%, which is not the NCES figure.
NATIONAL_PRIVATE_SHARE = 0.09

# --- modelling choices (see docstring) --------------------------------------
BASE_GRANT_MODE = 'blended'   # 'blended' | 'tk3'
INCLUDE_CHARTERS = False
# Entrants from private school are assumed not to be unduplicated pupils: they
# are on average higher-income, and FRPM/EL/foster status is what drives UPP.
ENTRANTS_ARE_UNDUPLICATED = False


def sfusd_enrollment_by_grade(charter=None):
    """SFUSD district enrollment by grade from the CDE file (ReportingCategory
    'TA' = total all). Returns (total, {grade_span: count}). `charter` defaults
    to the INCLUDE_CHARTERS setting; pass 'ALL' or 'N' to override."""
    if charter is None:
        charter = 'ALL' if INCLUDE_CHARTERS else 'N'
    with open(ENROLLMENT_FILE, encoding='utf-8', errors='replace') as fh:
        for row in csv.DictReader(fh, delimiter='\t'):
            if ((row.get('AggregateLevel') or '').strip() == 'D'
                    and 'San Francisco Unified' in (row.get('DistrictName') or '')
                    and (row.get('ReportingCategory') or '').strip() == 'TA'
                    and row['Charter'] == charter):
                by_span = {span: sum(float(row[c]) for c in cols)
                           for span, cols in GRADE_SPAN.items()}
                return float(row['TOTAL_ENR']), by_span
    raise RuntimeError('SFUSD district total row not found in ' + ENROLLMENT_FILE)


def unduplicated_share():
    """SFUSD unduplicated pupil percentage, most recent year in calpads.csv."""
    df = pd.read_csv(CALPADS_FILE)
    df = df[df.metric.isin(['enrollment', 'unduplicated_count'])]
    year = df.year.max()
    totals = df[df.year == year].groupby('metric').value.sum()
    return totals['unduplicated_count'] / totals['enrollment'], int(year)


def base_grant(by_span, total):
    if BASE_GRANT_MODE == 'tk3':
        return BASE_GRANT['tk3']
    return sum(BASE_GRANT[span] * n for span, n in by_span.items()) / total


def lcff(ada, n_unduplicated, base):
    return (base * ada
            + SUPPLEMENTAL_RATE * base * n_unduplicated
            + CONCENTRATION_RATE * base * max(0.0, n_unduplicated - CONCENTRATION_THRESHOLD * ada))


def main():
    enrollment, by_span = sfusd_enrollment_by_grade()
    upp, upp_year = unduplicated_share()
    base = base_grant(by_span, enrollment)
    # Funded ADA runs below enrollment; apply SFUSD's own ratio to new students.
    ada_ratio = SFUSD_FUNDED_ADA / enrollment
    n_unduplicated = upp * SFUSD_FUNDED_ADA

    # Total SF K-12, from ALL public enrollment (charters included, since the
    # private share is a share of every student in the city) and the private
    # share. Students gained then split between the district and charters in
    # today's proportions, so only the district's share earns LCFF here.
    public_all, _ = sfusd_enrollment_by_grade(charter='ALL')
    district_share_of_public = enrollment / public_all
    sf_total = public_all / (1.0 - SF_PRIVATE_SHARE)
    private_now = sf_total - public_all
    private_national = NATIONAL_PRIVATE_SHARE * sf_total
    public_gained = private_now - private_national
    students_gained = public_gained * district_share_of_public
    ada_gained = students_gained * ada_ratio

    new_ada = SFUSD_FUNDED_ADA + ada_gained
    new_unduplicated = n_unduplicated + (upp * ada_gained if ENTRANTS_ARE_UNDUPLICATED else 0.0)

    lcff_now = lcff(SFUSD_FUNDED_ADA, n_unduplicated, base)
    lcff_new = lcff(new_ada, new_unduplicated, base)
    gain = lcff_new - lcff_now

    # Marginal rate for one extra non-unduplicated student, while UPP stays
    # above the threshold: base * (1 - CONCENTRATION_RATE * THRESHOLD).
    marginal_rate = base * (1.0 - CONCENTRATION_RATE * CONCENTRATION_THRESHOLD)

    rows = {
        'sfusd_enrollment': enrollment,
        'sfusd_funded_ada': SFUSD_FUNDED_ADA,
        'ada_per_enrolled': ada_ratio,
        'base_grant_blended': base,
        'base_grant_mode': BASE_GRANT_MODE,
        'include_charters': INCLUDE_CHARTERS,
        'upp': upp,
        'upp_year': upp_year,
        'upp_threshold': CONCENTRATION_THRESHOLD,
        'sf_public_enrollment_all': public_all,
        'district_share_of_public': district_share_of_public,
        'sf_total_students': sf_total,
        'sf_private_share': SF_PRIVATE_SHARE,
        'national_private_share': NATIONAL_PRIVATE_SHARE,
        'private_students_now': private_now,
        'private_students_national_rate': private_national,
        'public_students_gained': public_gained,
        'students_gained': students_gained,
        'ada_gained': ada_gained,
        'upp_after': new_unduplicated / new_ada,
        'lcff_modelled_now': lcff_now,
        'lcff_modelled_new': lcff_new,
        'lcff_gain': gain,
        'lcff_gain_millions': gain / 1e6,
        'marginal_rate_below_threshold': marginal_rate,
        'lcff_actual_reported': SFUSD_LCFF_ACTUAL,
        'lcff_actual_per_ada': SFUSD_LCFF_ACTUAL / SFUSD_FUNDED_ADA,
        'lcff_modelled_per_ada': lcff_now / SFUSD_FUNDED_ADA,
        'naive_gain_at_average_rate': ada_gained * SFUSD_LCFF_ACTUAL / SFUSD_FUNDED_ADA,
    }
    pd.Series(rows).to_frame('value').rename_axis('key').to_csv(OUT)

    print(f'{"key":<34} value')
    for k, v in rows.items():
        print(f'{k:<34} {v:,.4f}' if isinstance(v, float) else f'{k:<34} {v}')
    print(f'\n✓ {OUT}')
    print(f'\nHeadline: moving the private share from {SF_PRIVATE_SHARE:.0%} to '
          f'{NATIONAL_PRIVATE_SHARE:.0%} brings {students_gained:,.0f} students '
          f'({ada_gained:,.0f} ADA) and ${gain/1e6:,.0f}M of LCFF.')
    print(f'Modelled LCFF per ADA today is ${lcff_now/SFUSD_FUNDED_ADA:,.0f} against '
          f'${SFUSD_LCFF_ACTUAL/SFUSD_FUNDED_ADA:,.0f} actually reported '
          f'({100*(lcff_now/SFUSD_LCFF_ACTUAL - 1):+.1f}% error), which is the '
          f'check on the formula above.')


if __name__ == '__main__':
    main()
