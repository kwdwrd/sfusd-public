#!/usr/bin/env python3
"""
Estimate families' distaste for SFUSD's kindergarten lottery, using the
2025-26 TK-to-K automatic promotion opt-out as a natural experiment.

Pipeline:
  1. Load kindergarten lottery request volume + the TK promotion roster (src/data.py)
  2. Estimate school "quality" (demand) via school/year fixed effects on log
     odds of ranking each school, over the pre-treatment window
     ESTIMATION_YEARS (src/data.py, src/quality_model.py)
  3. Estimate each school's admission probability from CDE kindergarten
     enrollment (capacity) vs. lottery request volume (demand), and the
     expected value of lottery participation two ways: the baseline formula
     with observed admission probabilities (src/capacity.py), and a
     capacity-constrained simulation of the full cohort (src/simulation.py)
  4. Compute each promotion-eligible school's distaste measure: the gap
     between the probability-weighted expected payoff of staying in the
     lottery and the certain payoff of accepting promotion (src/distaste.py)
  5. Relate promotion rates to school quality and estimate the rate-aware
     certainty premium (src/promotion_rate.py)
  6. Effect of promotion on applications per school: summary statistics,
     difference-in-differences, event study, and the mechanical model's
     prediction (src/application_effects.py)
  7. Robustness: re-run everything with quality pooled over other windows
     and measured year by year, and rank-correlate school quality across all
     years (src/robustness.py)
  8. Risk aversion: treat each eligible family's promotion decision as a
     sure-thing-vs-lottery choice and estimate CARA risk aversion
     (src/risk_aversion.py; validated by validate_risk_aversion.py)
  9. Save tables to output/ and figures to figures/, and write every number
     main.tex reports to output/numbers.tex and output/tables/ (make_numbers.py)

Run from within this directory: `python3 run_distaste_analysis.py`
"""

import os
import subprocess
import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, 'src')
from data import load_kindergarten_requests, load_promotion, load_school_mapping, TREATMENT_YEAR, ESTIMATION_YEARS
from quality_model import fit_quality_model, fit_yearly_quality
from distaste import compute_distaste, summarize_distaste, gap_as_school_pair
from capacity import get_admission_probabilities, sequential_expected_value
from promotion_rate import load_promotion_rates, certainty_premium, pooled_certainty_premium
from simulation import capacity_constrained_expected_value
from robustness import estimate_spec, yearly_rank_correlations, rank_correlation_with
from risk_aversion import baseline_lottery, certainty_equivalent, risk_aversion_analysis
import make_numbers
import plot_first_rankings
import lcff_counterfactual
from application_effects import (
    OUTCOMES, balanced_panel, summary_stats, did_regressions, event_study, model_comparison,
)


# Categorical pair validated for color-vision deficiency (see dataviz palette);
# the two groups also differ in marker shape.
ELIGIBLE_COLOR = '#eb6834'
INELIGIBLE_COLOR = '#2a78d6'
INK = '#2a78d6'
BASELINE_COLOR = '#444444'
CAPACITY_COLOR = '#eb6834'


def make_quality_figure(school_theta, distaste_df, expected_value, expected_value_capacity, path):
    """Quality-by-promotion-status scatter referenced in main.tex, plus the
    probability-weighted distaste of each promotion-eligible school. The
    expected utility of lottery participation (baseline and capacity-
    constrained) is marked on the quality axis."""
    eligible_names = set(distaste_df.school_name)
    plot_df = school_theta.sort_values('theta', ascending=False).reset_index(drop=True)
    is_treatment = plot_df.school_name.isin(eligible_names)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    ax = axes[0]
    for treatment, label, color, marker in [(False, 'Ineligible', INELIGIBLE_COLOR, 'o'),
                                             (True, 'Promotion-eligible', ELIGIBLE_COLOR, '^')]:
        mask = is_treatment == treatment
        ax.scatter(np.where(mask)[0], plot_df.loc[mask, 'theta'], label=label, color=color,
                   marker=marker, s=30, edgecolor='white', linewidth=0.8)
    for value, linestyle, label in [
        (expected_value, '--', 'EU, observed admission probabilities'),
        (expected_value_capacity, ':', 'EU, capacity-constrained'),
    ]:
        ax.axhline(value, color=BASELINE_COLOR, linestyle=linestyle, linewidth=1.5)
        ax.annotate(rf'{label} $= {value:.2f}$', (len(plot_df) - 1, value), xytext=(0, 5),
                    textcoords='offset points', ha='right', va='bottom', fontsize=9, color=BASELINE_COLOR)
    ax.set_xlabel('School (ranked by estimated quality)')
    ax.set_ylabel(r'Estimated school quality $\theta_k$')
    ax.set_xticks([])
    ax.legend(loc='lower left', frameon=False)
    ax.set_title('School quality vs. TK-promotion eligibility')

    ax = axes[1]
    ax.axhline(0, color='gray', linewidth=0.8, linestyle=':')
    ax.scatter(distaste_df.theta, distaste_df.distaste_weighted, facecolor='none', edgecolor=BASELINE_COLOR,
               marker='o', s=36, label='Observed admission probabilities')
    ax.scatter(distaste_df.theta, distaste_df.distaste_capacity, color=CAPACITY_COLOR, marker='^', s=36,
               edgecolor='white', linewidth=0.8, label='Capacity-constrained')
    ax.set_xlabel(r'School quality $\theta_k$ (promotion-eligible schools)')
    ax.set_ylabel(r'Forgone option value $\mathrm{EU} - \theta_k$')
    ax.legend(loc='upper right', frameon=False)
    ax.set_title('Forgone option value by school')

    for ax in axes:
        ax.grid(alpha=0.25)
        for side in ('top', 'right'):
            ax.spines[side].set_visible(False)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close(fig)


def make_promotion_rate_figure(rates, expected_value, restricted, free, restricted_capacity, path):
    """Promotion rate vs. demand-based quality (theta_k) and vs. SBAC math,
    with the model-implied acceptance curve (beta=1) against the free fit.
    With beta=1 only c - EU is identified, so the baseline and capacity-
    constrained fits trace the same curve; both c's go in its label."""
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), sharey=True)
    size = 4 * rates.promotion_eligible

    ax = axes[0]
    ax.scatter(rates.theta, rates.promotion_rate, s=size, color=INK, alpha=0.75, edgecolor='white', linewidth=1)
    grid = np.linspace(rates.theta.min() - 0.2, rates.theta.max() + 0.2, 100)
    c = restricted.params[0]
    a, b = free.params
    ax.plot(grid, 1 / (1 + np.exp(-(c + grid - expected_value))), color='#444444', linestyle='--', linewidth=2,
            label=rf'Model, $\beta=1$: $c={c:.2f}$ (observed probabilities), '
                  rf'${restricted_capacity.params[0]:.2f}$ (capacity)')
    ax.plot(grid, 1 / (1 + np.exp(-(a + b * (grid - expected_value)))), color='#444444', linewidth=2,
            label=rf'Free slope: $\hat\beta={b:.2f}$ ({free.bse[1]:.2f})')
    ax.set_xlabel(r'Demand-based school quality $\theta_k$')
    ax.set_ylabel('TK$\\to$K promotion rate (accepted / eligible)')
    ax.set_ylim(0, 1.05)
    ax.legend(loc='lower right', frameon=False)
    ax.set_title(r'Promotion rate vs. $\theta_k$')

    ax = axes[1]
    ax.scatter(rates.sbac_math, rates.promotion_rate, s=size, color=INK, alpha=0.75, edgecolor='white', linewidth=1)
    ax.set_xlabel('SBAC 2023-24 math, % met or exceeded')
    ax.set_title('Promotion rate vs. test scores')
    for ax in axes:
        ax.grid(alpha=0.25)
        for side in ('top', 'right'):
            ax.spines[side].set_visible(False)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close(fig)


def make_rank_correlation_figure(rank_corr, with_main, path):
    """Left: Spearman rank correlation of single-year school quality between
    every pair of application years. Right: each year's correlation with the
    main specification's pooled quality, main window highlighted."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5), gridspec_kw={'width_ratios': [1.25, 1]})
    years = list(rank_corr.columns)
    n = len(years)
    lower = np.tril(np.ones((n, n), dtype=bool))

    ax = axes[0]
    masked = np.ma.masked_where(~lower, rank_corr.values)
    cmap = plt.get_cmap('Blues').copy()
    cmap.set_bad('white')
    image = ax.imshow(masked, cmap=cmap, vmin=0.8, vmax=1.0)
    for i in range(n):
        for j in range(i + 1):
            value = rank_corr.values[i, j]
            ax.text(j, i, f'{value:.2f}'[1:] if value < 1 else '1', ha='center', va='center', fontsize=7,
                    color='white' if value > 0.93 else '#222222')
    ax.set_xticks(range(n), years, rotation=60, fontsize=8)
    ax.set_yticks(range(n), years, fontsize=8)
    for side in ('top', 'right'):
        ax.spines[side].set_visible(False)
    fig.colorbar(image, ax=ax, fraction=0.04, pad=0.02, label='Spearman rank correlation')
    ax.set_title('Rank correlation of school quality between years')

    ax = axes[1]
    in_window = with_main.ay.isin(ESTIMATION_YEARS)
    ax.plot(with_main.ay, with_main.spearman, color='#bbbbbb', linewidth=2, zorder=1)
    ax.scatter(with_main.ay[~in_window], with_main.spearman[~in_window], color=INK, s=40, zorder=2,
               label='Other years')
    ax.scatter(with_main.ay[in_window], with_main.spearman[in_window], color=CAPACITY_COLOR, marker='^', s=60,
               zorder=2, label='Main estimation window')
    ax.set_ylim(0.85, 1.0)
    ax.set_ylabel('Spearman correlation with main-specification quality')
    ax.tick_params(axis='x', rotation=60, labelsize=8)
    ax.legend(loc='lower right', frameon=False)
    ax.grid(alpha=0.25)
    for side in ('top', 'right'):
        ax.spines[side].set_visible(False)
    ax.set_title('Single-year quality vs. pooled main specification')

    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close(fig)


def make_risk_aversion_figure(lotteries, comparisons, model_a_designs, path):
    """Left: certainty equivalent of each lottery definition as a function of
    CARA risk aversion r, with the CE implied by take-up and the resulting
    estimates. Right: Model A's implied CDF -- the share of each school's
    families choosing the lottery against that school's indifference r*."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    styles = {'baseline': (BASELINE_COLOR, '--', 'o', 'Observed admission probabilities'),
              'capacity': (CAPACITY_COLOR, '-', '^', 'Capacity-constrained')}

    ax = axes[0]
    grid = np.linspace(-2, 12, 300)
    for name, (outcomes, probs) in lotteries.items():
        color, linestyle, marker, label = styles[name]
        ax.plot(grid, [certainty_equivalent(outcomes, probs, r) for r in grid], color=color, linestyle=linestyle,
                linewidth=2, label=label)
        row = comparisons[comparisons.lottery == name].iloc[0]
        ax.scatter([row.r_theta], [row.certainty_equivalent], color=color, marker=marker, s=70, zorder=3,
                   edgecolor='white', linewidth=1)
        offset, align = ((-8, -16), 'right') if name == 'baseline' else ((8, 6), 'left')
        ax.annotate(rf'$\hat r = {row.r_theta:.1f}$', (row.r_theta, row.certainty_equivalent), xytext=offset,
                    textcoords='offset points', ha=align, fontsize=9, color='#444444')
    target = comparisons.certainty_equivalent.iloc[0]
    ax.axhline(target, color='#888888', linestyle=':', linewidth=1.2)
    ax.annotate(f'CE implied by 73% take-up = {target:.2f}', (grid[0], target), xytext=(0, 5),
                textcoords='offset points', ha='left', fontsize=9, color='#444444')
    ax.set_xlabel(r'CARA risk aversion $r$ (per unit of $\theta$)')
    ax.set_ylabel(r'Certainty equivalent of the lottery ($\theta$ units)')
    ax.legend(loc='upper right', frameon=False)
    ax.set_title('Representative agent: risk aversion that rationalizes take-up')

    ax = axes[1]
    for name in lotteries:
        color, _, marker, label = styles[name]
        d = model_a_designs[(model_a_designs.lottery == name) & np.isfinite(model_a_designs.r_star)
                            & (model_a_designs.r_star.abs() < 15)]
        ax.scatter(d.r_star, d.n_lottery / d.n_total, s=4 * d.n_total, color=color, marker=marker, alpha=0.7,
                   edgecolor='white', linewidth=0.8, label=label)
    ax.set_ylim(0, 1)
    ax.set_xlabel(r'School indifference point $r^*_k$: $\mathrm{CE}_L(r^*_k) = \theta_k$')
    ax.set_ylabel('Share of eligible families choosing the lottery')
    ax.legend(loc='upper left', frameon=False)
    ax.set_title('Heterogeneous agents: implied CDF of $r$ should rise; it is flat')

    for ax in axes:
        ax.grid(alpha=0.25)
        for side in ('top', 'right'):
            ax.spines[side].set_visible(False)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close(fig)


def make_application_effects_figure(panel, events, path):
    """Left: per-school mean applications by group, indexed to 2024-25, for
    first-choice and all ranked requests. Right: event-study coefficients
    (promotion-eligible minus other schools, log points, 2024-25 = 0)."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    years = sorted(panel.ay.unique())

    ax = axes[0]
    for col, linestyle in (('num_first_choice', '-'), ('num_requests', '--')):
        means = panel.groupby(['treated', 'ay'])[col].mean().unstack()
        indexed = means.div(means['24-25'], axis=0)
        for treated, color, marker, group in ((True, ELIGIBLE_COLOR, '^', 'promotion-eligible'),
                                              (False, INELIGIBLE_COLOR, 'o', 'other schools')):
            ax.plot(years, indexed.loc[treated, years], color=color, linestyle=linestyle, marker=marker,
                    markersize=6, linewidth=2, label=f'{OUTCOMES[col]}, {group}')
    ax.axvline(3.5, color='#888888', linestyle=':', linewidth=1)
    ax.annotate('TK promotion', (3.55, ax.get_ylim()[1]), xytext=(2, -12), textcoords='offset points',
                fontsize=9, color='#444444')
    ax.set_ylabel('Mean per school, indexed to 2024-25 = 1')
    ax.legend(loc='lower left', frameon=False, fontsize=8)
    ax.set_title('Applications per school, 2021-22 to 2026-27')

    ax = axes[1]
    ax.axhline(0, color='#888888', linewidth=0.8)
    for name, color, marker, shift in (('First-choice requests', ELIGIBLE_COLOR, '^', 0.12),
                                       ('All ranked requests', BASELINE_COLOR, 'o', -0.12)):
        e = events[events.outcome == name].reset_index(drop=True)
        x = np.arange(len(e)) + shift
        ax.errorbar(x, e.coef, yerr=[e.coef - e.ci_lower, e.ci_upper - e.coef], fmt=marker, color=color,
                    markersize=6, capsize=2, linewidth=1.2, label=name)
    ax.set_xticks(np.arange(len(e)), e.ay, rotation=60, fontsize=8)
    ax.axvline(list(e.ay).index('24-25') + 0.5, color='#888888', linestyle=':', linewidth=1)
    ax.set_ylabel('Promotion-eligible minus other schools (log points)')
    ax.legend(loc='upper left', frameon=False)
    ax.set_title('Event study (2024-25 = 0), 95% CIs clustered by school')

    for ax in axes:
        ax.grid(alpha=0.25)
        for side in ('top', 'right'):
            ax.spines[side].set_visible(False)
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close(fig)


# make_numbers.main() reads output/risk_aversion_validation.csv, which only
# validate_risk_aversion.py writes, so the pipeline has to produce it first or
# the run dies at the risk_aversion step on a clean checkout. The validator
# takes ~50s, so re-run it only when it is missing or older than the code it
# checks.
VALIDATION_CSV = 'output/risk_aversion_validation.csv'
VALIDATION_INPUTS = ('validate_risk_aversion.py', 'src/risk_aversion.py')


def ensure_risk_aversion_validation():
    if os.path.exists(VALIDATION_CSV) and all(
        os.path.getmtime(VALIDATION_CSV) >= os.path.getmtime(dep)
        for dep in VALIDATION_INPUTS
    ):
        print(f'   - {VALIDATION_CSV} is up to date; skipping re-validation')
        return
    print('   ... validate_risk_aversion.py (~50s; writes '
          f'{VALIDATION_CSV})')
    subprocess.run([sys.executable, 'validate_risk_aversion.py'], check=True)


def main():
    print('=' * 80)
    print('ESTIMATING FAMILIES’ DISTASTE FOR THE SFUSD KINDERGARTEN LOTTERY')
    print('=' * 80)

    print('\n[1/9] Loading lottery requests + TK promotion roster...')
    lottery_df = load_kindergarten_requests()
    promotion_df = load_promotion()
    school_mapping_df = load_school_mapping()
    print(f'   ✓ {lottery_df.school_id.nunique()} schools, '
          f'{lottery_df.ay.nunique()} application years, '
          f'{promotion_df.promotion_eligible.notna().sum()} schools on the promotion roster')

    print('\n[2/9] Estimating school quality (theta_k) via school/year fixed effects...')
    estimation_df = lottery_df[lottery_df.ay.isin(ESTIMATION_YEARS)]
    model, school_theta, _ = fit_quality_model(estimation_df, requests_col='num_requests_total')
    print(f'   ✓ Fit on {int(model.nobs)} school-year observations ({ESTIMATION_YEARS[0]} to '
          f'{ESTIMATION_YEARS[-1]}), {len(school_theta)} schools, R²={model.rsquared:.3f}')

    print('\n[3/9] Estimating admission probabilities from CDE kindergarten-capacity data...')
    admission = get_admission_probabilities(estimation_df, school_theta)
    n_matched = admission.matched.fillna(False).sum()
    print(f'   ✓ Matched {n_matched}/{len(admission)} schools to a CDE capacity figure '
          f'({len(admission) - n_matched} standalone TK-only sites fall back to admission_prob=1, '
          f'see src/capacity.py)')
    expected_value, residual_prob = sequential_expected_value(admission)
    print(f'   ✓ Sequential (rank-order) expected payoff of lottery participation: '
          f'{expected_value:.3f} (vs. best real school theta={school_theta.theta.max():.3f}; '
          f'residual probability of admission nowhere on the full {len(admission)}-school list: {residual_prob:.4f})')
    simulation, _ = capacity_constrained_expected_value(lottery_df, school_theta, admission, TREATMENT_YEAR)
    expected_value_capacity = simulation['expected_value']
    print(f'   ✓ Capacity-constrained simulation ({simulation["ay"]}, {simulation["n_families"]} families, '
          f'{simulation["total_seats"]} seats): EU = {expected_value_capacity:.3f} '
          f'(sd across draws {simulation["expected_value_sd"]:.3f}); '
          f'{simulation["share_placed_from_list"]:.1%} placed from their list, '
          f'{simulation["share_administrative"]:.1%} administratively, '
          f'{simulation["share_no_seat"]:.1%} with no seat left')

    print('\n[4/9] Computing lottery-participation distaste for promotion-eligible schools...')
    distaste_df = compute_distaste(
        school_theta, admission, promotion_df, school_mapping_df, expected_value, expected_value_capacity
    )
    unmatched = distaste_df[distaste_df.theta.isna()]
    if len(unmatched):
        print(f'   ! {len(unmatched)} promotion-eligible schools had no quality estimate '
              f'(no lottery K requests in the panel) and are excluded: '
              f'{unmatched.school_name.tolist()}')
    distaste_df = distaste_df.dropna(subset=['theta'])
    summary = summarize_distaste(distaste_df)
    print(f'   ✓ Computed distaste for {int(summary.n_schools)} schools')

    print('\n[5/9] Promotion rates vs. school quality, and rate-aware distaste...')
    rates = certainty_premium(load_promotion_rates(promotion_df, school_theta, admission), expected_value)
    restricted, free = pooled_certainty_premium(rates, expected_value)
    rates = certainty_premium(rates, expected_value_capacity, suffix='_capacity')
    restricted_capacity, free_capacity = pooled_certainty_premium(rates, expected_value_capacity)
    rate_corr = rates[['promotion_rate', 'theta', 'sbac_ela', 'sbac_math', 'unduplicated_share', 'admission_prob']].corr()
    print(f'   ✓ {len(rates)} schools; pooled rate {rates.promotion_accepted.sum() / rates.promotion_eligible.sum():.3f}')
    print('   correlation of promotion rate with:')
    print(rate_corr.promotion_rate.drop('promotion_rate').round(3).to_string())
    print(f'   pooled certainty premium (beta=1): c = {restricted.params[0]:.3f} (se {restricted.bse[0]:.3f})')
    print(f'   free slope on theta_k - EU: beta = {free.params[1]:.3f} (se {free.bse[1]:.3f})')
    print(f'   capacity-constrained: c = {restricted_capacity.params[0]:.3f} '
          f'(se {restricted_capacity.bse[0]:.3f})')
    rates.sort_values('promotion_rate').to_csv('output/promotion_rates.csv', index=False)
    make_promotion_rate_figure(rates, expected_value, restricted, free, restricted_capacity, 'figures/promotion_rate_vs_quality.png')

    print('\n[6/9] Effect of promotion on applications per school...')
    did_panel = balanced_panel(lottery_df)
    n_applicants = int(lottery_df[lottery_df.ay == TREATMENT_YEAR].ay_num_requests.iloc[0])
    n_promoted = int(promotion_df.promotion_accepted.sum())
    application_summary = summary_stats(did_panel)
    did = did_regressions(did_panel, n_applicants, n_promoted)
    events, _ = event_study(lottery_df, did_panel.school_name.unique())
    model_vs_did = model_comparison(did_panel, did, n_applicants, n_promoted)
    print(f'   ✓ Balanced panel: {did_panel.school_name.nunique()} schools '
          f'({did_panel[did_panel.treated].school_name.nunique()} promotion-eligible), {did_panel.ay.nunique()} cycles; '
          f'{n_applicants} lottery applicants + {n_promoted} promoted in {TREATMENT_YEAR}')
    print(did[did.method == 'OLS'][['outcome', 'spec', 'term', 'coef', 'se']].round(3).to_string(index=False))
    print(model_vs_did[['outcome', 'spec', 'predicted_did_q1', 'estimated_did', 'implied_q']].round(3).to_string(index=False))
    application_summary.to_csv('output/application_effects_summary.csv', index=False)
    did.to_csv('output/application_effects_did.csv', index=False)
    events.to_csv('output/application_effects_event_study.csv', index=False)
    model_vs_did.to_csv('output/application_effects_model.csv', index=False)
    make_application_effects_figure(did_panel, events, 'figures/application_effects.png')

    print('\n[7/9] Robustness to how school quality is measured...')
    yearly_theta = fit_yearly_quality(lottery_df)
    specs = [('Main: pooled, ' + f'{ESTIMATION_YEARS[0]} to {ESTIMATION_YEARS[-1]}', estimation_df, school_theta)]
    for label, years in [('Pooled, full panel', sorted(lottery_df.ay.unique())),
                         (f'Pooled, {ESTIMATION_YEARS[0]} to {TREATMENT_YEAR}', ESTIMATION_YEARS + [TREATMENT_YEAR])]:
        window_df = lottery_df[lottery_df.ay.isin(years)]
        specs.append((label, window_df, fit_quality_model(window_df)[1]))
    for ay in ESTIMATION_YEARS:
        specs.append((f'Single year, {ay}', lottery_df[lottery_df.ay == ay],
                      yearly_theta[yearly_theta.ay == ay][['school_name', 'theta']]))
    # §quality-over-time compares window theta against the full panel's, so the
    # full-panel per-school estimates have to leave the pipeline to be citable.
    full_panel_theta = next(t for label, _, t in specs if label == 'Pooled, full panel')
    full_panel_theta.to_csv('output/school_quality_full_panel.csv', index=False)
    print('   ✓ output/school_quality_full_panel.csv')

    robustness = pd.DataFrame([
        estimate_spec(label, lottery_df, est_df, theta_df, promotion_df, school_mapping_df, TREATMENT_YEAR)
        for label, est_df, theta_df in specs
    ])
    print(robustness[['spec', 'forgone_value', 'forgone_value_capacity', 'certainty_premium',
                      'certainty_premium_capacity', 'free_slope_beta']].round(2).to_string(index=False))
    rank_corr, _ = yearly_rank_correlations(yearly_theta)
    with_main = rank_correlation_with(yearly_theta, school_theta)
    adjacent = np.diag(rank_corr.values, k=-1)
    print(f'   ✓ Year-to-year rank correlation of school quality: adjacent years {adjacent.min():.2f} to '
          f'{adjacent.max():.2f}; {rank_corr.columns[0]} vs. {rank_corr.columns[-1]}: {rank_corr.values[-1, 0]:.2f}')
    robustness.to_csv('output/robustness_quality_measurement.csv', index=False)
    rank_corr.to_csv('output/quality_rank_correlation_by_year.csv')
    with_main.to_csv('output/quality_rank_correlation_with_main.csv', index=False)
    yearly_theta.to_csv('output/school_quality_by_year.csv', index=False)
    make_rank_correlation_figure(rank_corr, with_main, 'figures/quality_rank_correlation.png')

    print('\n[8/9] Risk aversion from the promotion decision...')
    lotteries = {
        'baseline': baseline_lottery(admission),
        'capacity': (simulation['lottery_outcomes'], simulation['lottery_probs'] / simulation['lottery_probs'].sum()),
    }
    risk_estimates, risk_comparisons, model_a_designs = risk_aversion_analysis(rates, lotteries)
    for row in risk_estimates.itertuples():
        status = (f'r = {row.r:.3f} (se {row.r_se_robust:.3f})' if row.identified and row.r is not None
                  else f'not identified: {row.reason}')
        print(f'   {row.lottery:8s} {row.model:38s} {status}')
    print(risk_comparisons[['lottery', 'r_theta', 'certainty_equivalent', 'risk_premium', 'risk_premium_per_sd',
                            'premium_share_of_max', 'holt_laury_equivalent_crra']].round(3).to_string(index=False))
    risk_estimates.to_csv('output/risk_aversion_estimates.csv', index=False)
    risk_comparisons.to_csv('output/risk_aversion_canonical_comparison.csv', index=False)
    model_a_designs.to_csv('output/risk_aversion_model_a_designs.csv', index=False)
    make_risk_aversion_figure(lotteries, risk_comparisons, model_a_designs, 'figures/risk_aversion.png')

    print('\n[9/9] Saving outputs...')
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
        'expected_value_capacity', 'distaste_capacity',
    ]].sort_values('distaste_weighted', ascending=False).to_csv(
        'output/lottery_distaste_estimates.csv', index=False
    )
    summary = pd.concat([summary, pd.Series({
        'certainty_premium_pooled': restricted.params[0],
        'certainty_premium_pooled_se': restricted.bse[0],
        'certainty_premium_mean_family_weighted': np.average(rates.certainty_premium, weights=rates.promotion_accepted),
        'free_slope_beta': free.params[1],
        'free_slope_beta_se': free.bse[1],
        'promotion_rate_pooled': rates.promotion_accepted.sum() / rates.promotion_eligible.sum(),
        'expected_value_weighted': expected_value,
        'expected_value_capacity': expected_value_capacity,
        'certainty_premium_capacity_pooled': restricted_capacity.params[0],
        'certainty_premium_capacity_pooled_se': restricted_capacity.bse[0],
        'certainty_premium_capacity_mean_family_weighted': np.average(
            rates.certainty_premium_capacity, weights=rates.promotion_accepted
        ),
        'simulation_share_placed_from_list': simulation['share_placed_from_list'],
        'simulation_share_administrative': simulation['share_administrative'],
        'simulation_share_no_seat': simulation['share_no_seat'],
        'simulation_total_seats': simulation['total_seats'],
        'simulation_target_list_length': simulation['target_list_length'],
        'simulation_n_families': simulation['n_families'],
    })])
    summary.to_csv('output/lottery_distaste_summary.csv', header=['value'])
    make_quality_figure(school_theta, distaste_df, expected_value, expected_value_capacity, 'figures/quality_by_promotion_status.png')
    print('   ✓ output/school_quality_estimates.csv')
    print('   ✓ output/school_admission_probabilities.csv')
    print('   ✓ output/lottery_distaste_estimates.csv')
    print('   ✓ output/lottery_distaste_summary.csv')
    print('   ✓ output/promotion_rates.csv')
    print('   ✓ figures/quality_by_promotion_status.png')
    print('   ✓ figures/promotion_rate_vs_quality.png')
    print('   ✓ output/robustness_quality_measurement.csv, output/quality_rank_correlation_*.csv, '
          'output/school_quality_by_year.csv')
    print('   ✓ figures/quality_rank_correlation.png')
    print('   ✓ output/risk_aversion_*.csv, figures/risk_aversion.png')
    print('   ✓ output/application_effects_*.csv, figures/application_effects.png')
    ensure_risk_aversion_validation()
    plot_first_rankings.main()
    lcff_counterfactual.main()
    make_numbers.main()

    print('\n' + '=' * 100)
    print('SUMMARY')
    print('=' * 100)
    print(summary.to_string())
    print('\nCONCRETE TRANSLATION: the family-weighted mean PROBABILITY-WEIGHTED distaste of '
          f'{summary.distaste_weighted_mean_family_weighted:.2f} is comparable in size to the theta-gap '
          'between:')
    top_name, top_theta, match_name, match_theta, actual_gap = gap_as_school_pair(
        school_theta, summary.distaste_weighted_mean_family_weighted, 'top'
    )
    print(f'  {top_name} (theta={top_theta:.2f}, #1 of {len(school_theta)} citywide) and '
          f'{match_name} (theta={match_theta:.2f}).')

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
