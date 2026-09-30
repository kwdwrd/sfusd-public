"""
Estimating families' distaste for SFUSD's kindergarten lottery.

Intuition (see main.tex intro): a family offered automatic promotion into
their TK-linked school k trades a *certain* payoff (theta_k) for the
*uncertain* payoff they'd get by staying in the lottery. Under the logit
demand model in src/quality_model.py, the expected payoff from facing a
choice set S under idiosyncratic Type-I extreme-value shocks is the
"inclusive value" (log-sum-exp of the set's theta's); the certain payoff of
being assigned to k for sure is just theta_k. Their difference is the
option value the family gives up by accepting promotion -- i.e. their
distaste for (or measured cost of forgoing) lottery participation.

Two choice-set definitions are computed, since neither is fully satisfying
on its own:

  - citywide inclusive value: log-sum-exp of theta_k across every K school
    in the panel. This is the more informative number (roughly, "how much
    better is the best school you could plausibly have gotten into"), but
    assumes every family could realistically compete for every school
    citywide -- with no geographic/commute restriction on the choice set,
    this likely *overstates* the true option value for most families.
  - binary "k vs. everything else", matching the exact two-alternative
    identification in main.tex's Model section (theta_k vs. theta_0t, the
    fitted outside-option level for the relevant application year). This is
    the option value implied by the paper's own binary logit, but is
    close to degenerate: since any single school is a small share of
    citywide demand, theta_0t sits very near its normalized zero point for
    every year, so this measure collapses to approximately -theta_k and
    adds little beyond the raw quality estimate.

Both are reported so the citywide number's magnitude can be read in light
of its more conservative binary counterpart.
"""

import numpy as np
import pandas as pd


def citywide_inclusive_value(school_theta):
    """log-sum-exp of every school's theta_k -- the expected payoff of a
    choice set spanning every K school in the panel."""
    return np.log(np.sum(np.exp(school_theta.theta)))


def binary_outside_option(ay_effects, ay, reference_ay_theta0=0.0):
    """
    Recover theta_0t (absolute outside-option level for application year ay)
    from the treatment-coded year fixed effect b_t estimated in
    quality_model.fit_quality_model.

    The regression normalizes theta_0 = 0 in the dropped reference year, so
    (see src/quality_model.py docstring for the full derivation):
        theta_0,t = reference_ay_theta0 - b_t
    """
    return reference_ay_theta0 - ay_effects[f'T.{ay}']


def compute_distaste(school_theta, ay_effects, promotion_df, school_mapping_df, treatment_ay):
    """
    Join school-level theta estimates to the promotion roster (via the
    school_id -> school_name mapping, since the roster only carries IDs)
    and compute, per promotion-eligible school:
      - theta: the school's estimated (certain) payoff
      - iv_citywide: citywide inclusive value (same for every school)
      - distaste_citywide = iv_citywide - theta
      - theta_0_binary: the binary outside-option level for treatment_ay
      - distaste_binary = ln(1 + exp(theta_0_binary - theta))
    """
    iv_citywide = citywide_inclusive_value(school_theta)
    theta_0_binary = binary_outside_option(ay_effects, treatment_ay)

    eligible = promotion_df[promotion_df.promotion_eligible.notna()].merge(
        school_mapping_df, on='school_id', how='left'
    )
    merged = eligible.merge(school_theta, on='school_name', how='left')

    merged = merged.assign(
        iv_citywide=iv_citywide,
        distaste_citywide=lambda _df: iv_citywide - _df.theta,
        theta_0_binary=theta_0_binary,
        distaste_binary=lambda _df: np.log1p(np.exp(theta_0_binary - _df.theta)),
    )
    return merged


def compute_weighted_distaste(distaste_df, admission_df, expected_value):
    """
    Probability-weighted distaste: expected_value should be the sequential,
    capacity-aware expected payoff of lottery participation from
    src/capacity.py's sequential_expected_value (NOT the naive citywide
    inclusive value above, which assumes certain attendance of whichever
    school is chosen). distaste_weighted = expected_value - theta_k.

    Takes the *already-built* distaste_df (from compute_distaste) rather
    than re-deriving the promotion roster join, since some schools have
    more than one row in the roster (distinct TK program codes within the
    same building, e.g. two immersion tracks) -- joining admission_df in by
    school_name onto the existing one-row-per-program table avoids
    re-fanning those rows out via a second independent join on school_id.

    Also carries each row's own admission_prob through, since "how likely
    would this family have been to actually get k via the lottery" is
    useful context for interpreting distaste_weighted.
    """
    merged = distaste_df.merge(
        admission_df[['school_name', 'admission_prob']], on='school_name', how='left'
    )
    merged = merged.assign(
        expected_value_weighted=expected_value,
        distaste_weighted=lambda _df: expected_value - _df.theta,
    )
    return merged


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
    out = {}
    cols = ['distaste_citywide', 'distaste_binary']
    if 'distaste_weighted' in distaste_df.columns:
        cols.append('distaste_weighted')
    for col in cols:
        out[f'{col}_mean_unweighted'] = distaste_df[col].mean()
        out[f'{col}_mean_family_weighted'] = np.average(distaste_df[col], weights=weights)
    out['n_schools'] = len(distaste_df)
    out['n_families_weighted'] = weights.sum()
    return pd.Series(out)
