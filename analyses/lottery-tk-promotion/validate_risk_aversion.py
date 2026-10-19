#!/usr/bin/env python3
"""
Simulation-based validation of src/risk_aversion.py (spec section 7):

  1. Model A recovery (CRRA): rho_i ~ N(0.5, 0.3^2), N=2000, 200 seeds,
     point-estimate bias and 95% CI coverage for m and sigma
  2. Model B recovery (CARA) for each noise specification (ce, fechner,
     contextual) with known (r, mu)
  3. Cross-fit: Model A data estimated with Model B (ce)
  4. Single-design guard for both models
  5. Dominance rows flagged and excluded
  6. Numerical edge cases: rho near 1, zero outcomes under CRRA, large stakes

Run from within this directory: `python3 validate_risk_aversion.py`.
Exits non-zero if any check fails; writes output/risk_aversion_validation.csv.
"""

import sys

import numpy as np
import pandas as pd

sys.path.insert(0, 'src')
from risk_aversion import (
    certainty_equivalent, indifference_point, fit_model_a, fit_model_b, utility, inverse_utility, _choice_prob,
)

results = []


def check(name, passed, detail):
    results.append({'check': name, 'passed': bool(passed), 'detail': detail})
    print(f"  [{'PASS' if passed else 'FAIL'}] {name}: {detail}")


def grouped(s_values, chose_lottery, outcomes, probs):
    df = pd.DataFrame({'s': s_values, 'y': chose_lottery})
    designs = df.groupby('s', as_index=False).agg(n_lottery=('y', 'sum'), n_total=('y', 'size'))
    designs['outcomes'] = [np.asarray(outcomes)] * len(designs)
    designs['probs'] = [np.asarray(probs)] * len(designs)
    return designs


# ------------------------------------------------------------------ 1. Model A
print('\n[1] Model A recovery (CRRA, rho_i ~ N(0.5, 0.3^2), N=2000, 200 seeds)')
OUT_A, PROB_A = np.array([1.0, 4.0]), np.array([0.5, 0.5])
S_GRID = np.linspace(1.6, 3.2, 17)
r_star_grid = {s: indifference_point(OUT_A, PROB_A, s, 'crra') for s in S_GRID}
estimates = []
for seed in range(200):
    rng = np.random.default_rng(seed)
    rho = rng.normal(0.5, 0.3, 2000)
    s = rng.choice(S_GRID, 2000)
    y = (rho < np.array([r_star_grid[v] for v in s])).astype(int)
    fit = fit_model_a(grouped(s, y, OUT_A, PROB_A), family='crra')
    estimates.append((fit['m'], fit['m_se'], fit['sigma'], fit['sigma_se']))
est = np.array(estimates)
cover_m = np.mean(np.abs(est[:, 0] - 0.5) < 1.96 * est[:, 1])
cover_s = np.mean(np.abs(est[:, 2] - 0.3) < 1.96 * est[:, 3])
check('Model A mean m', abs(est[:, 0].mean() - 0.5) < 0.01, f'mean m_hat = {est[:, 0].mean():.4f} (true 0.5)')
check('Model A mean sigma', abs(est[:, 2].mean() - 0.3) < 0.01, f'mean sigma_hat = {est[:, 2].mean():.4f} (true 0.3)')
check('Model A coverage m', 0.92 <= cover_m <= 0.98, f'95% CI coverage {cover_m:.3f}')
check('Model A coverage sigma', 0.92 <= cover_s <= 0.98, f'95% CI coverage {cover_s:.3f}')

# ------------------------------------------------------------------ 2. Model B
print('\n[2] Model B recovery (CARA, N=2000, 20 seeds per specification)')
OUT_B, PROB_B = np.array([-4.0, -2.5, -1.0]), np.array([0.25, 0.5, 0.25])
S_GRID_B = np.linspace(-3.6, -1.4, 12)
for spec, r_true, mu_true in [('ce', 1.0, 0.5), ('fechner', 1.0, 2.0), ('contextual', 1.0, 0.2)]:
    fits = []
    for seed in range(20):
        rng = np.random.default_rng(1000 + seed)
        s = rng.choice(S_GRID_B, 2000)
        p = np.array([_choice_prob(spec, OUT_B, PROB_B, v, r_true, mu_true, 'cara') for v in s])
        y = (rng.uniform(size=2000) < p).astype(int)
        fits.append(fit_model_b(grouped(s, y, OUT_B, PROB_B), spec=spec))
    ok = [f for f in fits if f['identified']]
    r_hat = np.array([f['r'] for f in ok])
    mu_hat = np.array([f['mu'] for f in ok])
    check(f'Model B ({spec}) identified', len(ok) == len(fits), f'{len(ok)}/{len(fits)} fits identified')
    if ok:
        check(f'Model B ({spec}) r', abs(np.median(r_hat) - r_true) < 0.15 * r_true,
              f'median r_hat = {np.median(r_hat):.3f} (true {r_true}); IQR {np.percentile(r_hat, 25):.3f}-'
              f'{np.percentile(r_hat, 75):.3f}')
        check(f'Model B ({spec}) mu', abs(np.median(mu_hat) - mu_true) < 0.15 * mu_true,
              f'median mu_hat = {np.median(mu_hat):.3f} (true {mu_true})')
    ses = np.array([f['r_se'] for f in ok])
    cover = np.mean(np.abs(r_hat - r_true) < 1.96 * ses)
    check(f'Model B ({spec}) coverage r', cover >= 0.85, f'95% CI coverage {cover:.2f} over {len(ok)} seeds')

# ------------------------------------------------------------------ 3. cross-fit
print('\n[3] Cross-fit: Model A data (CARA, r_i ~ N(1.0, 0.5^2)) estimated with Model B (ce)')
rng = np.random.default_rng(7)
r_i = rng.normal(1.0, 0.5, 4000)
s = rng.choice(S_GRID_B, 4000)
r_star = np.array([indifference_point(OUT_B, PROB_B, v, 'cara') for v in s])
y = (r_i < r_star).astype(int)
cross = fit_model_b(grouped(s, y, OUT_B, PROB_B), spec='ce')
check('Cross-fit runs', cross['identified'] and np.isfinite(cross['r']),
      f"Model B r_hat = {cross['r']:.3f} vs. true mean/median 1.0 (mu_hat = {cross['mu']:.3f})")

# ------------------------------------------------------------------ 4. single design
print('\n[4] Single-design guards')
rng = np.random.default_rng(11)
s_single = certainty_equivalent(OUT_B, PROB_B, 1.0)  # r* = 1, the median of r_i: about half take the lottery
s1 = np.full(1000, s_single)
r_star1 = indifference_point(OUT_B, PROB_B, s_single, 'cara')
y1 = (rng.normal(1.0, 0.5, 1000) < r_star1).astype(int)
single = grouped(s1, y1, OUT_B, PROB_B)
a1 = fit_model_a(single)
check('Model A single design -> quantile only', not a1['identified'] and 'quantile' in a1, a1.get('report', ''))
b1 = fit_model_b(single, spec='ce')
check('Model B single design -> not identified', not b1['identified'], b1['reason'])
b1_fixed = fit_model_b(single, spec='ce', fixed_mu=0.5)
check('Model B single design, mu fixed -> r estimable', b1_fixed['identified'] and abs(b1_fixed['r'] - 1.0) < 0.2,
      f"r_hat = {b1_fixed['r']:.3f} (r* = 1; identified only because mu is fixed)")

# ------------------------------------------------------------------ 5. dominance
print('\n[5] Dominance rows')
check('s <= min(L) -> +inf', indifference_point(OUT_B, PROB_B, -4.0) == np.inf, 'lottery weakly dominates')
check('s >= max(L) -> -inf', indifference_point(OUT_B, PROB_B, -1.0) == -np.inf, 'sure amount dominates')
dom = grouped(np.r_[np.full(100, -4.5), rng.choice(S_GRID_B, 900)], rng.integers(0, 2, 1000), OUT_B, PROB_B)
a_dom = fit_model_a(dom)
check('Model A drops dominance rows', a_dom['n_dropped_dominance'] == 100, f"dropped {a_dom['n_dropped_dominance']}")
check('Risk-neutral point', abs(indifference_point(OUT_B, PROB_B, float(np.sum(OUT_B * PROB_B)))) < 1e-8,
      's = E[L] gives r* = 0')

# ------------------------------------------------------------------ 6. edge cases
print('\n[6] Numerical edge cases')
x = np.array([0.5, 2.0])
near = [certainty_equivalent(x, [0.5, 0.5], 1 + d, 'crra') for d in (-1e-4, -1e-7, 0.0, 1e-7, 1e-4)]
check('CRRA continuous at rho = 1', np.ptp(near) < 1e-4, f'CE over rho in 1 +/- 1e-4: {np.ptp(near):.2e} spread')
check('CRRA u/u^-1 round trip near 1', abs(inverse_utility(utility(2.0, 1 + 1e-7, 'crra'), 1 + 1e-7, 'crra') - 2.0) < 1e-5,
      'u^-1(u(2)) = 2')
try:
    certainty_equivalent([0.0, 1.0], [0.5, 0.5], 2.0, 'crra')
    check('CRRA rejects zero outcomes', False, 'no error raised')
except ValueError as err:
    check('CRRA rejects zero outcomes', True, str(err))
big = certainty_equivalent([1e3, 5e3], [0.5, 0.5], 5.0, 'cara')
check('CARA large stakes, large r', np.isfinite(big) and abs(big - (1e3 + np.log(2) / 5)) < 1e-6,
      f'CE = {big:.4f}')
huge = certainty_equivalent([1e6, 1e8], [0.5, 0.5], 3.0, 'crra')
check('CRRA large stakes', np.isfinite(huge) and 1e6 < huge < 2e6, f'CE = {huge:.1f}')
check('CE decreasing in r', np.all(np.diff([certainty_equivalent(OUT_B, PROB_B, r) for r in np.linspace(-5, 5, 41)]) < 0),
      'strictly decreasing on [-5, 5]')

pd.DataFrame(results).to_csv('output/risk_aversion_validation.csv', index=False)
n_fail = sum(not r['passed'] for r in results)
print(f'\n{len(results) - n_fail}/{len(results)} checks passed')
sys.exit(1 if n_fail else 0)
