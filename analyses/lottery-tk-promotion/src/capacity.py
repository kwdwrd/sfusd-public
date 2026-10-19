"""
Kindergarten seat-capacity proxy, built from CDE school-level enrollment
files, and admission-probability weighting for the distaste calculation.

Why this exists: lottery applications reveal how much families value
*applying* to a school, not their odds of *attending* it. Valuing lottery
participation therefore needs each school's admission probability.

This module estimates a per-school admission probability P_k = min(1,
capacity_k / demand_k), where capacity_k is a proxy for available K seats
(actual K enrollment from CDE, since occupied seats are the closest
available proxy for capacity) and demand_k is the school's average total
lottery request volume (across all rank positions, all years). It's then
used to compute a family's expected payoff from lottery participation under
a *sequential* (rank-order) assignment model: a family ranks schools by
preference (theta, descending) and attends the highest-ranked school that
admits them, so

    E[U] = sum_k theta_k * P_k * prod_{j ranked above k} (1 - P_j)

matching the waterfall structure "P(admit_1) theta_1 + P(admit_2)P(reject_1)
theta_2 + ..." -- rejections are treated as independent across schools for
tractability, since we don't have individual-level match outcomes to
estimate their true (likely positively correlated) joint distribution.
"""

import re

import numpy as np
import pandas as pd

CDE_ENROLLMENT_PATHS = [
    ('../../data/enrollment/raw/cdenroll2324-v2.txt', 'utf-8'),
    ('../../data/enrollment/raw/cdenroll2425.txt', 'latin1'),
]
SFUSD_DISTRICT_NAME = 'San Francisco Unified'

# Lottery school_name -> CDE SchoolName, for the cases automated normalization
# can't bridge (spelled-out vs. abbreviated middle names, "Alternative" vs.
# the lottery system's own suffix, SF vs. San Francisco, etc). Verified by
# hand against the CDE file's SFUSD school list.
MANUAL_NAME_OVERRIDES = {
    'Clarendon ES': 'Clarendon Alternative Elementary',
    'Cobb (Dr William L) ES': 'Cobb (William L.) Elementary',
    'Carver (Dr George W) ES': 'Carver (George Washington) Elementary',
    'Milk (Harvey) Civil Right ES': 'Milk (Harvey) Civil Rights Elementary',
    'SF Community K-8': 'San Francisco Community Alternative',
    'Spring Valley Science ES': 'Spring Valley Elementary',
    'Buena Vista Horace Mann K-8': 'Buena Vista/ Horace Mann K-8',
    'Lawton K-8': 'Lawton Alternative',
    'Drew (Dr Charles) College Preparatory Academy ES': 'Drew (Charles) College Preparatory Academy',
    'Carmichael (Bessie) K-8': "Carmichael (Bessie)/FEC",
    'Lakeshore ES': 'Lakeshore Alternative Elementary',
    'Wo (Yick) ES': 'Yick Wo Elementary',
    'SF Public Montessori ES': 'San Francisco Public Montessori',
}

# Standalone TK program sites (school_name suffix "Tk") are either co-located
# without their own CDS code, or reported as TK/pre-K-only "Children
# Centers" with no separate kindergarten enrollment (GR_KN=0) -- neither
# gives a usable K-capacity figure, and naive name matching risks wrongly
# assigning them a same-named ES building's capacity (e.g. "Alvarado Tk" is
# a distinct site from "Alvarado ES", not the same building). These are
# excluded from capacity matching entirely; see get_admission_probabilities
# for the fallback applied to them.
_TK_SITE_SUFFIX = re.compile(r'\s+Tk$')


def _normalize(name):
    n = name.upper().strip()
    n = n.replace('.', '').replace(',', '').replace('/', ' ')
    n = re.sub(r'^SF\b', 'SAN FRANCISCO', n)
    for suf in ['ELEMENTARY SCHOOL', 'MIDDLE SCHOOL', 'HIGH SCHOOL', 'CHILDREN CENTER',
                'CHILDREN CENTERS', 'ELEMENTARY', 'MIDDLE', 'ES', 'MS', 'HS', 'K-8', 'EES']:
        n = re.sub(r'\s+' + re.escape(suf) + r'$', '', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def load_cde_kindergarten_enrollment():
    """Average GR_KN (kindergarten enrollment) per SFUSD school across the
    two most recent CDE school-level enrollment files, as a capacity proxy."""
    frames = []
    for path, encoding in CDE_ENROLLMENT_PATHS:
        df = pd.read_csv(path, sep='\t', dtype=str, encoding=encoding)
        sf = df[
            (df.DistrictName == SFUSD_DISTRICT_NAME)
            & (df.AggregateLevel == 'S')
            & (df.ReportingCategory == 'TA')
        ].copy()
        sf['GR_KN'] = pd.to_numeric(sf.GR_KN, errors='coerce').fillna(0)
        frames.append(sf[['SchoolName', 'GR_KN']])
    combined = pd.concat(frames, ignore_index=True)
    return combined.groupby('SchoolName', as_index=False)['GR_KN'].mean().rename(
        columns={'GR_KN': 'capacity'}
    )


def match_capacity_to_schools(school_names, cde_enrollment=None):
    """
    Match lottery school_name values to a CDE kindergarten-capacity figure.

    Returns a DataFrame with columns [school_name, capacity, matched] --
    matched=False for standalone TK program sites (excluded by design, see
    module docstring) and any school this couldn't confidently match.
    """
    if cde_enrollment is None:
        cde_enrollment = load_cde_kindergarten_enrollment()
    cde_enrollment = cde_enrollment.assign(norm=lambda _df: _df.SchoolName.apply(_normalize))

    rows = []
    for name in school_names:
        if _TK_SITE_SUFFIX.search(name):
            rows.append((name, np.nan, False))
            continue
        target = MANUAL_NAME_OVERRIDES.get(name, name)
        norm = _normalize(target)
        match = cde_enrollment[cde_enrollment.norm == norm]
        if len(match):
            rows.append((name, match.capacity.iloc[0], True))
        else:
            rows.append((name, np.nan, False))
    return pd.DataFrame(rows, columns=['school_name', 'capacity', 'matched'])


def get_admission_probabilities(lottery_df, school_theta):
    """
    P_k = min(1, capacity_k / demand_k) for every school in school_theta.

    demand_k is the school's average total (all-rank) lottery request volume
    across application years (num_requests_total, i.e. including families who
    would have applied absent the TK opt-out -- see src/data.py).

    Schools with no matched capacity (standalone TK sites, or any school CDE
    matching missed) fall back to P_k=1: these are, by construction, the
    lowest-theta / most under-demanded schools in the panel, so assuming they
    have enough seats to admit anyone who ranks them is the least distortive
    default available without real capacity data for them.
    """
    demand = lottery_df.groupby('school_name', as_index=False)['num_requests_total'].mean().rename(
        columns={'num_requests_total': 'demand'}
    )
    capacity = match_capacity_to_schools(school_theta.school_name.unique())

    admission = school_theta[['school_name', 'theta']].merge(demand, on='school_name', how='left').merge(
        capacity, on='school_name', how='left'
    )
    admission['admission_prob'] = np.where(
        admission.matched.fillna(False),
        np.minimum(1.0, admission.capacity / admission.demand),
        1.0,
    )
    return admission


def sequential_expected_value(admission_df):
    """
    E[U] under sequential (rank-by-preference, take first admit) assignment
    over the full choice set in admission_df, ordered by theta descending:

        E[U] = sum_k theta_k * P_k * prod_{j ranked above k} (1 - P_j)

    Also returns the residual probability of being admitted nowhere in the
    choice set (1 - sum of the per-school admission weights), which is the
    weight implicitly left on a common "admitted nowhere in the choice set"
    outcome; that residual is assigned zero utility here, i.e. this is a
    conservative (lower) bound on E[U] to the extent it understates whatever
    fallback those families actually get.
    """
    ordered = admission_df.sort_values('theta', ascending=False).reset_index(drop=True)
    survival = np.concatenate([[1.0], np.cumprod(1 - ordered.admission_prob.values)[:-1]])
    weight = ordered.admission_prob.values * survival
    expected_value = float(np.sum(weight * ordered.theta.values))
    residual_prob = float(1 - weight.sum())
    return expected_value, residual_prob
