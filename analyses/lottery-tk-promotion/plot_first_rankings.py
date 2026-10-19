#!/usr/bin/env python3
"""
First rankings by school group, 2021-22 to 2026-27, before and after TK
automatic promotion. Three series:

  1. Promotion-eligible schools, observed first-choice requests
  2. Promotion-eligible schools plus the families who accepted promotion --
     the count if every promoted family had applied and ranked its own TK
     school first. Shown for 2025-26 only (anchored at 2024-25), since the
     roster has no 2026-27 promotion counts.
  3. Other schools, observed first-choice requests

Left panel: counts. Right panel: share of all first rankings (every
applicant submits exactly one first choice, so shares are unaffected by
changes in cohort size). In the share panel the hypothetical adds the
promoted families to the denominator for both groups: series 2's share adds
them to the numerator too, and a matching dashed series for other schools
keeps their count unchanged (under the hypothetical, every promoted family's
first choice is its own school), so the two hypothetical shares sum to one.
The count panel needs no other-schools counterpart, since that count is
unchanged.

Uses the balanced panel from src/application_effects.py (schools observed in
every cycle). Run from within this directory: `python3 plot_first_rankings.py`.
Writes figures/first_rankings.png and output/first_rankings_by_year.csv.
"""

import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, 'src')
from data import load_kindergarten_requests, load_promotion, TREATMENT_YEAR
from application_effects import balanced_panel, DID_YEARS

ELIGIBLE_COLOR = '#eb6834'
OTHER_COLOR = '#2a78d6'
ANCHOR_YEAR = '24-25'


def first_rankings_by_year():
    lottery_df = load_kindergarten_requests()
    panel = balanced_panel(lottery_df)
    n_promoted = load_promotion().promotion_accepted.sum()

    counts = panel.groupby(['ay', 'treated']).num_first_choice.sum().unstack()
    df = pd.DataFrame({
        'eligible': counts[True],
        'other': counts[False],
        'all_first_rankings': lottery_df.groupby('ay').num_first_choice.sum(),
    }).loc[DID_YEARS]
    df['eligible_plus_promoted'] = np.where(df.index == TREATMENT_YEAR, df.eligible + n_promoted, np.nan)
    df.loc[ANCHOR_YEAR, 'eligible_plus_promoted'] = df.loc[ANCHOR_YEAR, 'eligible']
    df['eligible_share'] = df.eligible / df.all_first_rankings
    df['other_share'] = df.other / df.all_first_rankings
    promoted_in_year = np.where(df.index == TREATMENT_YEAR, n_promoted, 0)
    df['eligible_plus_promoted_share'] = df.eligible_plus_promoted / (df.all_first_rankings + promoted_in_year)
    df['other_with_promoted_share'] = np.where(
        df.eligible_plus_promoted.notna(), df.other / (df.all_first_rankings + promoted_in_year), np.nan
    )
    return df, int(n_promoted)


def plot(df, n_promoted, path):
    years = list(df.index)
    x = np.arange(len(years))
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    panels = [
        (axes[0], '', 'First rankings', 'First rankings by school group', '{:,.0f}'),
        (axes[1], '_share', 'Share of all first rankings', 'Share of all first rankings', '{:.1%}'),
    ]
    for ax, suffix, ylabel, title, fmt in panels:
        observed = df[f'eligible{suffix}']
        but_for = df[f'eligible_plus_promoted{suffix}']
        other = df[f'other{suffix}']
        ax.plot(x, observed, color=ELIGIBLE_COLOR, marker='^', markersize=8, linewidth=2,
                label='Promotion-eligible schools, observed')
        treated = years.index(TREATMENT_YEAR)
        ax.plot(x, but_for, color=ELIGIBLE_COLOR, marker='^', markersize=8, linewidth=2, linestyle='--',
                markerfacecolor='white', markevery=[treated], zorder=1,
                label=f'Promotion-eligible + {n_promoted} promoted families')
        ax.plot(x, other, color=OTHER_COLOR, marker='o', markersize=7, linewidth=2, label='Other schools, observed')
        labels = [(observed.iloc[treated], (-10, -4), 'right'), (but_for.iloc[treated], (-10, 2), 'right')]
        if suffix == '_share':
            other_but_for = df.other_with_promoted_share
            ax.plot(x, other_but_for, color=OTHER_COLOR, marker='o', markersize=7, linewidth=2, linestyle='--',
                    markerfacecolor='white', markevery=[treated], zorder=1,
                    label=f'Other schools, share with {n_promoted} promoted families in denominator')
            labels.append((other_but_for.iloc[treated], (10, -4), 'left'))

        ax.axvline(treated - 0.5, color='#888888', linestyle=':', linewidth=1)
        for value, offset, align in labels:
            ax.annotate(fmt.format(value), (treated, value), xytext=offset, textcoords='offset points',
                        ha=align, va='top' if offset[1] < 0 else 'bottom', fontsize=9, color='#444444')
        ax.margins(y=0.08)

        ax.set_xticks(x, years)
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.grid(alpha=0.25)
        for side in ('top', 'right'):
            ax.spines[side].set_visible(False)

    axes[0].annotate('TK promotion', (years.index(TREATMENT_YEAR) - 0.45, axes[0].get_ylim()[1]),
                     xytext=(0, -12), textcoords='offset points', fontsize=9, color='#444444')
    axes[1].yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(0.02))
    axes[1].yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0, decimals=0))
    handles, legend_labels = axes[1].get_legend_handles_labels()
    order = [0, 1, 2, 3]
    fig.legend([handles[i] for i in order], [legend_labels[i] for i in order], loc='lower center', ncol=2,
               frameon=False, fontsize=9, bbox_to_anchor=(0.5, -0.10))

    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close(fig)


def main():
    df, n_promoted = first_rankings_by_year()
    df.to_csv('output/first_rankings_by_year.csv')
    plot(df, n_promoted, 'figures/first_rankings.png')
    print(df.round(3).to_string())
    print('\n✓ figures/first_rankings.png')
    print('✓ output/first_rankings_by_year.csv')


if __name__ == '__main__':
    main()
