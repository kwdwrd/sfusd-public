"""
Estimating families' distaste for SFUSD's kindergarten lottery.

Intuition (see main.tex intro): a family offered automatic promotion into
their TK-linked school k trades a *certain* payoff (theta_k) for the
*uncertain* payoff of staying in the lottery. That uncertain payoff is the
sequential, capacity-aware expected value EU from src/capacity.py:
families rank schools by theta and attend the first that admits them. The
gap EU - theta_k is the option value a family forgoes by accepting
promotion -- the lottery distaste of a family exactly indifferent between
the two. src/promotion_rate.py builds on this using observed take-up.
"""

import numpy as np
import pandas as pd


def compute_distaste(school_theta, admission_df, promotion_df, school_mapping_df, expected_value,
                     expected_value_capacity):
    """
    Join school-level theta estimates and admission probabilities to the
    promotion roster (via the school_id -> school_name mapping, since the
    roster only carries IDs) and compute, per promotion-eligible program:
      - theta: the school's estimated (certain) payoff
      - admission_prob: the school's own lottery admission probability
      - expected_value_weighted: EU, the same for every school
      - distaste_weighted = EU - theta
      - expected_value_capacity / distaste_capacity: the same, using the
        capacity-constrained simulation's EU (src/simulation.py)
    Two buildings run two TK programs each (DeAvila's Chinese-immersion
    strands, Huerta's Spanish strands). As in src/data.py, sum their roster
    rows to the building first, so this stays one row per school and both
    strands share one theta and one admission probability (see main.tex,
    "Schools versus programs"). Merging program by program double-counts them.
    """
    promotion_by_school = promotion_df.groupby('school_id', as_index=False)[
        ['promotion_eligible', 'promotion_accepted']
    ].sum(min_count=1)
    eligible = promotion_by_school[promotion_by_school.promotion_eligible.notna()].merge(
        school_mapping_df, on='school_id', how='left'
    )
    return eligible.merge(school_theta, on='school_name', how='left').merge(
        admission_df[['school_name', 'admission_prob']], on='school_name', how='left'
    ).assign(
        expected_value_weighted=expected_value,
        distaste_weighted=lambda _df: expected_value - _df.theta,
        expected_value_capacity=expected_value_capacity,
        distaste_capacity=lambda _df: expected_value_capacity - _df.theta,
    )


def gap_as_school_pair(school_theta, gap, anchor='top'):
    """
    Translate an abstract theta-unit gap into a concrete pair of real
    schools whose theta's differ by (approximately) that amount, so results
    can be read as "about as big as the quality gap between X and Y"
    instead of a bare log-utility number.

    anchor='top' anchors the comparison at the highest-theta school in
    school_theta and finds the school `gap` below it; anchor='bottom'
    anchors at the lowest-theta school and finds the school `gap` above it.
    Returns (anchor_school_name, anchor_theta, match_school_name, match_theta,
    actual_gap) -- actual_gap will differ slightly from the requested gap
    since it snaps to the nearest real school rather than interpolating.
    """
    ordered = school_theta.sort_values('theta', ascending=False).reset_index(drop=True)
    if anchor == 'top':
        anchor_row = ordered.iloc[0]
        target = anchor_row.theta - gap
    else:
        anchor_row = ordered.iloc[-1]
        target = anchor_row.theta + gap
    match_row = ordered.iloc[(ordered.theta - target).abs().idxmin()]
    actual_gap = abs(anchor_row.theta - match_row.theta)
    return anchor_row.school_name, anchor_row.theta, match_row.school_name, match_row.theta, actual_gap


def summarize_distaste(distaste_df, weight_col='promotion_accepted'):
    """
    Family-weighted average distaste: weight each school's distaste by how
    many families actually accepted automatic promotion there, giving the
    average distaste experienced by families who opted out of the lottery.
    """
    weights = distaste_df[weight_col].fillna(0)
    return pd.Series({
        'distaste_weighted_mean_by_school': distaste_df.distaste_weighted.mean(),
        'distaste_weighted_mean_family_weighted': np.average(distaste_df.distaste_weighted, weights=weights),
        'distaste_capacity_mean_by_school': distaste_df.distaste_capacity.mean(),
        'distaste_capacity_mean_family_weighted': np.average(distaste_df.distaste_capacity, weights=weights),
        'n_schools': len(distaste_df),
        'n_families_weighted': weights.sum(),
    })
