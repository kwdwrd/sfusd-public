"""
Robustness of the distaste estimates to how (and over which years) school
quality is measured, and year-to-year stability of school rankings.

estimate_spec re-runs the full chain -- admission probabilities, both
expected values, forgone option value, certainty premium, free slope -- for
any school_theta, so the main specification and every robustness row are
computed identically. The specs:

  - pooled fixed effects over the main pre-treatment window (main spec)
  - pooled fixed effects over the full 15-year panel, and over the window
    extended through the 25-26 treatment year
  - single-year quality theta_kt (src/quality_model.py's fit_yearly_quality)
    for each year in the main window, with admission probabilities from that
    year's demand alone
"""

import numpy as np
import pandas as pd

from capacity import get_admission_probabilities, sequential_expected_value
from distaste import compute_distaste, summarize_distaste
from promotion_rate import load_promotion_rates, certainty_premium, pooled_certainty_premium
from simulation import capacity_constrained_expected_value


def estimate_spec(label, lottery_df, est_df, school_theta, promotion_df, school_mapping_df, treatment_ay):
    """
    lottery_df: full panel (the capacity simulation needs the treatment-year
    cohort); est_df: the years whose demand defines admission probabilities;
    school_theta: one row per school with columns school_name, theta.
    """
    admission = get_admission_probabilities(est_df, school_theta)
    expected_value, _ = sequential_expected_value(admission)
    simulation, _ = capacity_constrained_expected_value(lottery_df, school_theta, admission, treatment_ay, n_sims=1)
    expected_value_capacity = simulation['expected_value']

    distaste_df = compute_distaste(
        school_theta, admission, promotion_df, school_mapping_df, expected_value, expected_value_capacity
    ).dropna(subset=['theta'])
    summary = summarize_distaste(distaste_df)

    rates = certainty_premium(load_promotion_rates(promotion_df, school_theta, admission), expected_value)
    rates = rates.dropna(subset=['theta'])
    restricted, free = pooled_certainty_premium(rates, expected_value)
    c = restricted.params[0]

    return {
        'spec': label,
        'n_schools': len(school_theta),
        'n_eligible_with_theta': len(rates),
        'expected_value': expected_value,
        'expected_value_capacity': expected_value_capacity,
        'schools_above_eu': int((school_theta.theta > expected_value).sum()),
        'schools_above_eu_capacity': int((school_theta.theta > expected_value_capacity).sum()),
        'forgone_value': summary.distaste_weighted_mean_family_weighted,
        'forgone_value_capacity': summary.distaste_capacity_mean_family_weighted,
        'certainty_premium': c,
        'certainty_premium_capacity': c - (expected_value - expected_value_capacity),
        'certainty_premium_se': restricted.bse[0],
        'free_slope_beta': free.params[1],
        'free_slope_beta_se': free.bse[1],
        'corr_rate_theta': rates.promotion_rate.corr(rates.theta),
    }


def yearly_rank_correlations(yearly_theta):
    """Spearman rank correlation of theta_kt between every pair of years,
    over the schools present in both."""
    wide = yearly_theta.pivot_table(index='school_name', columns='ay', values='theta')
    return wide.corr(method='spearman', min_periods=10), wide.notna().sum()


def rank_correlation_with(yearly_theta, school_theta):
    """Spearman rank correlation of each year's theta_kt with a pooled
    school_theta (e.g. the main specification)."""
    merged = yearly_theta.merge(school_theta[['school_name', 'theta']], on='school_name', suffixes=('', '_pooled'))
    return merged.groupby('ay').apply(
        lambda g: pd.Series({
            'spearman': g.theta.corr(g.theta_pooled, method='spearman'),
            'n_schools': len(g),
        }),
        include_groups=False,
    ).reset_index()
