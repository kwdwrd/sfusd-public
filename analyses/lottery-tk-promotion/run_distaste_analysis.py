#!/usr/bin/env python3
"""
Estimate families' distaste for SFUSD's kindergarten lottery, using the
2025-26 TK-to-K automatic promotion opt-out as a natural experiment.

Pipeline:
  1. Load kindergarten lottery request volume + the TK promotion roster (src/data.py)
  2. Estimate school "quality" (demand) via school/year fixed effects on log
     request share (src/quality_model.py)
  3. Estimate each school's admission probability from CDE kindergarten
     enrollment (capacity) vs. lottery request volume (demand) (src/capacity.py)
  4. Compute each promotion-eligible school's distaste measure three ways:
     the gap between the certain payoff of accepting automatic promotion and
     the expected payoff of staying in the lottery, naively (citywide,
     binary) and probability-weighted (src/distaste.py)
  5. Save tables to output/ and figures to figures/

Run from within this directory: `python3 run_distaste_analysis.py`
"""

import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, 'src')
from data import load_kindergarten_requests, load_promotion, load_school_mapping, TREATMENT_YEAR
from quality_model import fit_quality_model
from distaste import compute_distaste, compute_weighted_distaste, summarize_distaste, gap_as_school_pair
from capacity import get_admission_probabilities, sequential_expected_value


def make_quality_figure(school_theta, distaste_df, path):
    """Reproduce (and properly save) the quality-by-promotion-status scatter
    referenced in main.tex, plus a panel comparing the naive (certain-
    attendance) citywide distaste measure to the probability-weighted one."""
    plot_df = school_theta.merge(
        distaste_df[['school_name', 'distaste_citywide']], on='school_name', how='left'
    ).sort_values('theta', ascending=False).reset_index(drop=True)
    is_treatment = plot_df.distaste_citywide.notna()

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    ax = axes[0]
    ax.scatter(range(len(plot_df)), plot_df.theta, color='white')
    for treatment, label, color in [(True, 'Promotion-eligible', 'tab:green'),
                                     (False, 'Ineligible', 'tab:blue')]:
        mask = is_treatment == treatment
        ax.scatter(np.where(mask)[0], plot_df.loc[mask, 'theta'], label=label, color=color, s=18)
    ax.set_xlabel('School (ranked by estimated quality)')
    ax.set_ylabel(r'Estimated school quality $\theta_k$')
    ax.set_xticks([])
    ax.legend(loc='best')
    ax.set_title('School quality vs. TK-promotion eligibility')

    ax = axes[1]
    ax.axhline(0, color='gray', linewidth=0.8, linestyle=':')
    ax.scatter(distaste_df.theta, distaste_df.distaste_citywide, color='lightgray', s=20,
               label='Naive (certain-attendance) citywide')
    ax.scatter(distaste_df.theta, distaste_df.distaste_weighted, color='tab:green', s=24,
               label='Probability-weighted (sequential)')
    ax.set_xlabel(r'School quality $\theta_k$ (promotion-eligible schools)')
    ax.set_ylabel('Estimated distaste for lottery participation')
    ax.set_title('Accounting for admission odds shrinks (and can flip the\nsign of) measured distaste')
    ax.legend(loc='best')

    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close(fig)


def main():
    print('=' * 80)
    print('ESTIMATING FAMILIES’ DISTASTE FOR THE SFUSD KINDERGARTEN LOTTERY')
    print('=' * 80)

    print('\n[1/5] Loading lottery requests + TK promotion roster...')
    lottery_df = load_kindergarten_requests()
    promotion_df = load_promotion()
    school_mapping_df = load_school_mapping()
    print(f'   ✓ {lottery_df.school_id.nunique()} schools, '
          f'{lottery_df.ay.nunique()} application years, '
          f'{promotion_df.promotion_eligible.notna().sum()} schools on the promotion roster')

    print('\n[2/5] Estimating school quality (theta_k) via school/year fixed effects...')
    model, school_theta, ay_effects = fit_quality_model(lottery_df, requests_col='num_requests_total')
    print(f'   ✓ Fit on {int(model.nobs)} school-year observations, R²={model.rsquared:.3f}')

    print('\n[3/5] Estimating admission probabilities from CDE kindergarten-capacity data...')
    admission = get_admission_probabilities(lottery_df, school_theta)
    n_matched = admission.matched.fillna(False).sum()
    print(f'   ✓ Matched {n_matched}/{len(admission)} schools to a CDE capacity figure '
          f'({len(admission) - n_matched} standalone TK-only sites fall back to admission_prob=1, '
          f'see src/capacity.py)')
    expected_value, residual_prob = sequential_expected_value(admission)
    print(f'   ✓ Sequential (rank-order) expected payoff of lottery participation: '
          f'{expected_value:.3f} (vs. best real school theta={school_theta.theta.max():.3f}; '
          f'residual probability of admission nowhere in the full 86-school list: {residual_prob:.4f})')

    print('\n[4/5] Computing lottery-participation distaste for promotion-eligible schools...')
    distaste_df = compute_distaste(
        school_theta, ay_effects, promotion_df, school_mapping_df, treatment_ay=TREATMENT_YEAR
    )
    unmatched = distaste_df[distaste_df.theta.isna()]
    if len(unmatched):
        print(f'   ! {len(unmatched)} promotion-eligible schools had no quality estimate '
              f'(no lottery K requests in the panel) and are excluded: '
              f'{unmatched.school_name.tolist()}')
    distaste_df = distaste_df.dropna(subset=['theta'])
    distaste_df = compute_weighted_distaste(distaste_df, admission, expected_value)
    summary = summarize_distaste(distaste_df)
    print(f'   ✓ Computed distaste for {int(summary.n_schools)} schools')

    # Translate each school's abstract distaste into a concrete real-school
    # comparison: "a gap this size is about as big as [top school] to [X]".
    top_school_name = school_theta.sort_values('theta', ascending=False).iloc[0].school_name
    distaste_df = distaste_df.assign(
        distaste_citywide_comparable_to=lambda _df: _df.distaste_citywide.apply(
            lambda gap: f"{top_school_name} → {gap_as_school_pair(school_theta, gap, 'top')[2]}"
        ),
        distaste_binary_comparable_to=lambda _df: _df.distaste_binary.apply(
            lambda gap: f"{top_school_name} → {gap_as_school_pair(school_theta, gap, 'top')[2]}"
        ),
    )

    print('\n[5/5] Saving outputs...')
    school_theta.sort_values('theta', ascending=False).to_csv(
        'output/school_quality_estimates.csv', index=False
    )
    admission.sort_values('theta', ascending=False).to_csv(
        'output/school_admission_probabilities.csv', index=False
    )
    distaste_df[[
        'school_id', 'school_name', 'promotion_eligible', 'promotion_accepted',
        'theta', 'ci_lower', 'ci_upper', 'admission_prob',
        'expected_value_weighted', 'distaste_weighted',
        'iv_citywide', 'distaste_citywide', 'distaste_citywide_comparable_to',
        'theta_0_binary', 'distaste_binary', 'distaste_binary_comparable_to',
    ]].sort_values('distaste_weighted', ascending=False).to_csv(
        'output/lottery_distaste_estimates.csv', index=False
    )
    summary.to_csv('output/lottery_distaste_summary.csv', header=['value'])
    make_quality_figure(school_theta, distaste_df, 'figures/quality_by_promotion_status.png')
    print('   ✓ output/school_quality_estimates.csv')
    print('   ✓ output/school_admission_probabilities.csv')
    print('   ✓ output/lottery_distaste_estimates.csv')
    print('   ✓ output/lottery_distaste_summary.csv')
    print('   ✓ figures/quality_by_promotion_status.png')

    print('\n' + '=' * 100)
    print('SUMMARY')
    print('=' * 100)
    print(summary.to_string())
    print('\nHEADLINE (probability-weighted): distaste_weighted accounts for actual admission odds via '
          'the sequential rank-order model requested -- P(admit_1)*theta_1 + P(admit_2)*P(reject_1)*theta_2 '
          '+ ... -- using CDE kindergarten-enrollment-based capacity estimates. The other two measures '
          '(citywide, binary) both implicitly assumed certain attendance of whichever school a family '
          'prefers and are shown only as naive upper-bound comparisons; see main.tex for the full '
          'discussion of why they overstate true option value.')

    print('\nCONCRETE TRANSLATION: the family-weighted mean PROBABILITY-WEIGHTED distaste of '
          f'{summary.distaste_weighted_mean_family_weighted:.2f} is comparable in size to the theta-gap '
          'between:')
    top_name, top_theta, match_name, match_theta, actual_gap = gap_as_school_pair(
        school_theta, summary.distaste_weighted_mean_family_weighted, 'top'
    )
    print(f'  {top_name} (theta={top_theta:.2f}, #1 of {len(school_theta)} citywide) and '
          f'{match_name} (theta={match_theta:.2f}).')
    print(f'\n  For reference, the naive (unweighted) citywide and binary measures give family-weighted '
          f'means of {summary.distaste_citywide_mean_family_weighted:.2f} and '
          f'{summary.distaste_binary_mean_family_weighted:.2f} respectively -- '
          f'roughly {summary.distaste_citywide_mean_family_weighted / summary.distaste_weighted_mean_family_weighted:.1f}x '
          f'and {summary.distaste_binary_mean_family_weighted / summary.distaste_weighted_mean_family_weighted:.1f}x '
          f'the probability-weighted figure -- illustrating how much the certain-attendance assumption '
          f'was inflating the naive numbers.')

    n_negative = (distaste_df.distaste_weighted < 0).sum()
    print(f'\n{n_negative} of {len(distaste_df)} promotion-eligible schools have NEGATIVE '
          f'probability-weighted distaste (accepting promotion has higher expected value than staying in '
          f'the lottery, once admission odds are accounted for) -- these are the highest-theta '
          f'TK-linked schools, where the certain seat is good enough that gambling on something better '
          f'isn\'t worth the (low-probability) upside. This partially rationalizes the ~25% of eligible '
          f'families who entered the lottery anyway: our model says they should NOT have, unless zoned '
          f'to one of the {n_negative} schools below.')
    print(
        distaste_df.sort_values('distaste_weighted')
        [['school_name', 'promotion_accepted', 'theta', 'admission_prob', 'distaste_weighted']]
        .head(n_negative if n_negative else 5).to_string(index=False)
    )

    print('\nTop 10 highest measured distaste (probability-weighted):')
    print(
        distaste_df.sort_values('distaste_weighted', ascending=False)
        [['school_name', 'promotion_accepted', 'theta', 'admission_prob', 'distaste_weighted']]
        .head(10).to_string(index=False)
    )


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\n\nAnalysis interrupted by user')
        sys.exit(0)
