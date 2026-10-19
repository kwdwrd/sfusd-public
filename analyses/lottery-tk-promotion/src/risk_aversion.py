"""
Estimating risk aversion from binary sure-thing-vs-lottery choices.

Application (see main.tex, "Risk aversion"): each of the 820 TK families
eligible for automatic promotion makes one binary choice between

  - a sure outcome s_i = theta_k: kindergarten at its own TK school k, and
  - a lottery L: the distribution of theta of the school the kindergarten
    lottery would assign it (from src/capacity.py's observed admission
    probabilities, or src/simulation.py's capacity-constrained simulation).

Without individual data every family at school k faces the same pair, so
the data are 40 "designs" (one per building) with grouped counts. The
lottery is common to all designs; only s varies. The certainty premium c of
src/promotion_rate.py is the special case of Model B (CE noise, mu = 1) with
risk neutrality plus an additive distaste; here the same take-up is instead
attributed to curvature.

Utility families:
  - CARA (used for the application): u(x; r) = (1 - exp(-r x)) / r. theta is
    identified only up to location (each specification normalizes the
    outside option differently), and CARA is invariant to adding a
    constant to every outcome, so r is well defined in theta units. theta
    also takes negative values, which CRRA cannot accept.
  - CRRA: u(x; rho) = x^(1 - rho) / (1 - rho), x > 0 only (optionally on
    background wealth w + x). Implemented for completeness and for the
    validation suite (validate_risk_aversion.py).

Models:
  - Model A (random parameter): r_i ~ N(m, sigma^2), deterministic choice;
    family i takes the lottery iff r_i < r*_d, where r*_d solves
    CE_L(r) = s_d. P(lottery) = Phi((r*_d - m) / sigma): a probit on r*.
  - Model B (representative agent): one r shared by all choices, with noise
    in utility (Fechner), in range-normalized utility (contextual; Wilcox
    2011), or in certainty-equivalent space.
Both refuse to report point estimates the data cannot identify
(single design, non-positive probit slope, boundary or ridge in Model B).
"""

import numpy as np
import pandas as pd
from scipy import optimize, special, stats

R_TOL = 1e-8
RHO_LOG_TOL = 1e-6


class NotIdentified(Exception):
    pass


# ---------------------------------------------------------------- utilities

def _check_crra_support(x):
    if np.any(np.asarray(x) <= 0):
        raise ValueError('CRRA requires strictly positive outcomes; pass background wealth or use CARA')


def utility(x, r, family='cara'):
    x = np.asarray(x, dtype=float)
    if family == 'cara':
        return x if abs(r) < R_TOL else -np.expm1(-r * x) / r
    _check_crra_support(x)
    log_x = np.log(x)
    a = 1 - r
    if abs(a) < RHO_LOG_TOL:
        # series in (1 - rho) around log(x), avoiding cancellation near rho = 1
        return log_x + a * log_x ** 2 / 2
    return np.exp(a * log_x) / a


def inverse_utility(v, r, family='cara'):
    v = np.asarray(v, dtype=float)
    if family == 'cara':
        return v if abs(r) < R_TOL else -np.log1p(-r * v) / r
    a = 1 - r
    if abs(a) < RHO_LOG_TOL:
        return np.exp(v)
    return np.exp(np.log(a * v) / a)


def expected_utility(outcomes, probs, r, family='cara'):
    return float(np.sum(np.asarray(probs) * utility(outcomes, r, family)))


def certainty_equivalent(outcomes, probs, r, family='cara'):
    """CE of a lottery, computed in log-sum-exp form so large |r| or large
    stakes don't overflow. Strictly decreasing in r."""
    outcomes = np.asarray(outcomes, dtype=float)
    probs = np.asarray(probs, dtype=float)
    if family == 'cara':
        if abs(r) < R_TOL:
            return float(np.sum(probs * outcomes))
        return float(-special.logsumexp(-r * outcomes, b=probs) / r)
    _check_crra_support(outcomes)
    a = 1 - r
    if abs(a) < RHO_LOG_TOL:
        return float(np.exp(np.sum(probs * np.log(outcomes))))
    return float(np.exp(special.logsumexp(a * np.log(outcomes), b=probs) / a))


def indifference_point(outcomes, probs, s, family='cara', bracket=(-10.0, 20.0), max_widen=8):
    """
    r* solving CE_L(r) = s. Returns +inf when s <= min(L) (the lottery
    weakly dominates) and -inf when s >= max(L) (the sure thing dominates);
    either way the choice carries no information about r.
    """
    outcomes = np.asarray(outcomes, dtype=float)
    if s <= outcomes.min():
        return np.inf
    if s >= outcomes.max():
        return -np.inf
    g = lambda r: certainty_equivalent(outcomes, probs, r, family) - s
    lo, hi = bracket
    for _ in range(max_widen):
        if g(lo) > 0 > g(hi):
            return optimize.brentq(g, lo, hi, xtol=1e-12)
        lo, hi = lo * 2, hi * 2
    raise RuntimeError(f'could not bracket r* for s={s}')


# ------------------------------------------------------------------ Model A

def fit_model_a(designs, outcomes=None, probs=None, family='cara'):
    """
    designs: DataFrame with columns s, n_lottery (chose the lottery),
    n_total, and either a shared lottery (outcomes, probs) or per-row
    columns 'outcomes' / 'probs'. Fits the probit P(lottery) = Phi(b0 + b_r r*)
    on individual-level rows (one per person) and maps it to
    m = -b0 / b_r, sigma = 1 / b_r with delta-method standard errors
    (robust sandwich covariance; one choice per person, so no clustering).
    """
    import statsmodels.api as sm
    designs = designs.copy()
    designs['r_star'] = [
        indifference_point(row.get('outcomes', outcomes), row.get('probs', probs), row['s'], family)
        for _, row in designs.iterrows()
    ]
    dominated = ~np.isfinite(designs.r_star)
    result = {
        'n': int(designs.n_total.sum()),
        'n_dropped_dominance': int(designs.n_total[dominated].sum()),
        'n_designs': int((~dominated).sum()),
        'designs': designs,
    }
    usable = designs[~dominated]
    if usable.r_star.nunique() == 1:
        share = usable.n_lottery.sum() / usable.n_total.sum()
        result.update(identified=False, reason='single design', quantile=share, r_star=usable.r_star.iloc[0],
                      report=f'{share:.0%} of people have r below r* = {usable.r_star.iloc[0]:.3f}; '
                             'm and sigma are not identified')
        return result

    y = np.concatenate([np.r_[np.ones(int(d.n_lottery)), np.zeros(int(d.n_total - d.n_lottery))]
                        for d in usable.itertuples()])
    r_star = np.repeat(usable.r_star.values, usable.n_total.astype(int).values)
    fit = sm.Probit(y, sm.add_constant(r_star)).fit(disp=False, cov_type='HC0')
    b0, b_r = fit.params
    cov = fit.cov_params()
    result.update(probit=fit, b0=b0, b_r=b_r, b_r_se=fit.bse[1], loglik=fit.llf)
    if b_r <= 0 or b_r / fit.bse[1] < 1.96:
        result.update(identified=False,
                      reason=f'probit slope on r* is {b_r:.3f} (se {fit.bse[1]:.3f}): choice is not increasing '
                             'in r*, so m and sigma are not identified')
        return result
    m, sigma = -b0 / b_r, 1 / b_r
    grad_m = np.array([-1 / b_r, b0 / b_r ** 2])
    grad_sigma = np.array([0.0, -1 / b_r ** 2])
    result.update(identified=True, m=m, sigma=sigma,
                  m_se=float(np.sqrt(grad_m @ cov @ grad_m)), sigma_se=float(np.sqrt(grad_sigma @ cov @ grad_sigma)))
    return result


# ------------------------------------------------------------------ Model B

def _choice_prob(spec, outcomes, probs, s, r, mu, family):
    if spec == 'ce':
        index = (certainty_equivalent(outcomes, probs, r, family) - s) / mu
    else:
        diff = expected_utility(outcomes, probs, r, family) - utility(s, r, family)
        if spec == 'contextual':
            support = np.r_[outcomes, s]
            diff = diff / (utility(support.max(), r, family) - utility(support.min(), r, family))
        index = diff / mu
    return special.expit(index)


def _design_logliks(params, designs, spec, family, fixed_mu):
    r = params[0]
    mu = fixed_mu if fixed_mu is not None else np.exp(params[1])
    p = np.array([_choice_prob(spec, d.outcomes, d.probs, d.s, r, mu, family) for d in designs.itertuples()])
    p = np.clip(p, 1e-12, 1 - 1e-12)
    return np.log(p), np.log1p(-p)


def _negloglik(params, designs, spec, family, fixed_mu):
    log_p, log_q = _design_logliks(params, designs, spec, family, fixed_mu)
    return -float(np.sum(designs.n_lottery * log_p + (designs.n_total - designs.n_lottery) * log_q))


def _numerical_hessian(f, x, h=1e-4):
    k = len(x)
    hess = np.zeros((k, k))
    for i in range(k):
        for j in range(k):
            ei, ej = np.eye(k)[i] * h, np.eye(k)[j] * h
            hess[i, j] = (f(x + ei + ej) - f(x + ei - ej) - f(x - ei + ej) + f(x - ei - ej)) / (4 * h * h)
    return hess


def fit_model_b(designs, spec='ce', family='cara', fixed_mu=None, r_bounds=(-30.0, 30.0), log_mu_bounds=(-6.0, 8.0)):
    """
    Representative-agent MLE over (r, log mu), or r alone when fixed_mu is
    given. designs: DataFrame with s, n_lottery, n_total, outcomes, probs
    (one row per unique design). spec: 'fechner', 'contextual', or 'ce'.
    Returns estimates with inverse-Hessian and sandwich standard errors, or
    identified=False with a reason when the likelihood is flat/ridged or the
    optimum sits on a bound.
    """
    designs = designs.reset_index(drop=True)
    n_designs = len(designs.drop_duplicates(subset=['s']))
    base = {'spec': spec, 'family': family, 'fixed_mu': fixed_mu, 'n': int(designs.n_total.sum()),
            'n_designs': n_designs}
    if n_designs == 1 and fixed_mu is None:
        return {**base, 'identified': False,
                'reason': 'single design: (r, mu) lie on a ridge; fix mu exogenously to recover r'}

    f = lambda x: _negloglik(x, designs, spec, family, fixed_mu)
    bounds = [r_bounds] + ([] if fixed_mu is not None else [log_mu_bounds])
    starts = [[r0] + ([] if fixed_mu is not None else [lm0])
              for r0 in (-2.0, -0.5, 0.0, 0.5, 2.0, 5.0) for lm0 in (-1.0, 0.0, 1.0, 3.0)]
    starts = [list(x) for x in {tuple(x) for x in starts}]
    fits = [optimize.minimize(f, x0, method='L-BFGS-B', bounds=bounds) for x0 in starts]
    best = min(fits, key=lambda res: res.fun)
    x = best.x

    out = {**base, 'r': x[0], 'mu': fixed_mu if fixed_mu is not None else float(np.exp(x[1])),
           'loglik': -best.fun, 'converged': bool(best.success)}
    at_bound = any(abs(x[i] - b) < 1e-3 for i, (lo, hi) in enumerate(bounds) for b in (lo, hi))
    hess = _numerical_hessian(f, x)
    eig = np.linalg.eigvalsh(hess)
    if at_bound:
        return {**out, 'identified': False, 'reason': 'optimum on a parameter bound (likelihood keeps improving '
                                                      'toward an extreme of r or mu)'}
    if eig.min() <= 0 or eig.max() / eig.min() > 1e10:
        return {**out, 'identified': False, 'reason': f'Hessian not positive definite or near-singular '
                                                      f'(eigenvalues {eig.min():.2e}, {eig.max():.2e}): ridge'}

    cov = np.linalg.inv(hess)
    # sandwich: per-person scores, shared within (design, choice) cells
    meat = np.zeros_like(hess)
    for d in range(len(designs)):
        def cell(xx, which):
            log_p, log_q = _design_logliks(xx, designs.iloc[[d]], spec, family, fixed_mu)
            return (log_p if which else log_q)[0]
        for which, count in ((1, designs.n_lottery[d]), (0, designs.n_total[d] - designs.n_lottery[d])):
            grad = optimize.approx_fprime(x, lambda xx: cell(xx, which), 1e-6)
            meat += count * np.outer(grad, grad)
    sandwich = cov @ meat @ cov
    se = np.sqrt(np.diag(cov))
    se_robust = np.sqrt(np.diag(sandwich))
    out.update(r_se=se[0], r_se_robust=se_robust[0])
    if fixed_mu is None:
        out.update(mu_se=out['mu'] * se[1], mu_se_robust=out['mu'] * se_robust[1])  # delta method on exp
    if se[0] > 10 * max(1.0, abs(x[0])):
        return {**out, 'identified': False, 'reason': f'standard error on r ({se[0]:.2f}) is an order of '
                                                      'magnitude larger than the estimate: ridge'}
    return {**out, 'identified': True}


# ------------------------------------------------------------- application

def tk_designs(rates, outcomes, probs):
    """One design per promotion-eligible building: s = theta_k, choosing the
    lottery = declining promotion."""
    rates = rates.dropna(subset=['theta'])
    return pd.DataFrame({
        'school_name': rates.school_name.values,
        's': rates.theta.values,
        'n_lottery': (rates.promotion_eligible - rates.promotion_accepted).values,
        'n_total': rates.promotion_eligible.values,
        'outcomes': [np.asarray(outcomes)] * len(rates),
        'probs': [np.asarray(probs) / np.sum(probs)] * len(rates),
    })


def implied_crra(r_theta, dollars_per_theta, wealth):
    """Relative risk aversion rho = r_$ * W, with r_$ = r_theta / D, if one
    theta unit is worth D dollars and the relevant wealth is W."""
    return r_theta / dollars_per_theta * wealth


def breakeven_dollars_per_theta(r_theta, rho, wealth):
    """The dollar value of one theta unit at which r_theta corresponds to
    relative risk aversion rho at wealth W."""
    return r_theta * wealth / rho


def baseline_lottery(admission_df):
    """The baseline lottery as outcomes/probabilities: school k is reached
    with probability P_k * prod_{j ranked above k} (1 - P_j) (src/capacity.py)."""
    ordered = admission_df.sort_values('theta', ascending=False)
    p = ordered.admission_prob.values
    weights = p * np.concatenate([[1.0], np.cumprod(1 - p)[:-1]])
    return ordered.theta.values, weights / weights.sum()


# Holt and Laury (2002) high-risk option at their baseline (1x) payoffs, at
# the 50/50 decision row: $3.85 or $0.10.
HOLT_LAURY_LOTTERY = (np.array([3.85, 0.10]), np.array([0.5, 0.5]))


def premium_share_of_max(outcomes, probs, ce):
    """Risk premium E[L] - CE as a share of its maximum E[L] - min(L), which
    an infinitely risk-averse agent would pay. Unit-free, so comparable
    across lotteries in different units."""
    mean = float(np.sum(probs * outcomes))
    return (mean - ce) / (mean - np.min(outcomes))


def equivalent_crra_on(lottery, share):
    """CRRA rho at which `lottery` (positive outcomes) carries the same
    premium_share_of_max as `share`."""
    outcomes, probs = lottery
    g = lambda rho: premium_share_of_max(outcomes, probs, certainty_equivalent(outcomes, probs, rho, 'crra')) - share
    return optimize.brentq(g, -5.0, 200.0)


def tail_sensitivity(outcomes, probs, ce, n_drop):
    """r rationalizing certainty equivalent `ce` after dropping the n_drop
    lowest outcomes (renormalizing). inf if ce falls below the new minimum."""
    keep = np.argsort(outcomes)[n_drop:]
    return indifference_point(np.asarray(outcomes)[keep], np.asarray(probs)[keep] / np.sum(np.asarray(probs)[keep]), ce,
                              bracket=(-10.0, 50.0), max_widen=12)


def risk_aversion_analysis(rates, lotteries, wealth=1_000_000, dollar_grid=(10_000, 50_000, 100_000, 250_000),
                           rho_grid=(0.5, 2.0, 4.0)):
    """
    Fit every model to the TK designs under each lottery definition and
    build the canonical-parameter comparisons. lotteries: {name: (outcomes,
    probs)}. Returns (estimates, comparisons, model_a_designs) DataFrames.
    """
    estimates, comparisons, model_a_designs = [], [], []
    for name, (outcomes, probs) in lotteries.items():
        designs = tk_designs(rates, outcomes, probs)
        mean = float(np.sum(designs.probs[0] * outcomes))
        sd = float(np.sqrt(np.sum(designs.probs[0] * (outcomes - mean) ** 2)))

        a = fit_model_a(designs)
        model_a_designs.append(a['designs'].drop(columns=['outcomes', 'probs']).assign(lottery=name))
        upper = a['b_r'] + 1.96 * a['b_r_se']
        estimates.append({
            'lottery': name, 'model': 'A: random parameter (probit on r*)', 'identified': a['identified'],
            'reason': a.get('reason', ''), 'loglik': a['loglik'], 'n': a['n'],
            'n_dropped_dominance': a['n_dropped_dominance'], 'n_designs': a['n_designs'],
            'b_r': a['b_r'], 'b_r_se': a['b_r_se'],
            'sigma_lower_bound_95': 1 / upper if upper > 0 else np.inf,
        })
        for spec, fixed_mu in [('ce', 1.0), ('ce', None), ('fechner', None), ('contextual', None)]:
            b = fit_model_b(designs, spec=spec, fixed_mu=fixed_mu)
            estimates.append({
                'lottery': name, 'model': f'B: {spec}, ' + ('mu = 1 (logit scale)' if fixed_mu else 'mu free'),
                **{k: b.get(k) for k in ('identified', 'reason', 'r', 'r_se', 'r_se_robust', 'mu', 'mu_se',
                                        'loglik', 'n', 'n_designs')},
                'n_dropped_dominance': 0,
            })
            if spec == 'ce' and fixed_mu is not None and b['identified']:
                ce = certainty_equivalent(outcomes, designs.probs[0], b['r'])
                share = premium_share_of_max(outcomes, designs.probs[0], ce)
                row = {
                    'lottery': name, 'r_theta': b['r'], 'r_theta_se': b['r_se_robust'],
                    'lottery_mean': mean, 'lottery_sd': sd, 'lottery_min': float(np.min(outcomes)),
                    'certainty_equivalent': ce, 'risk_premium': mean - ce,
                    'risk_premium_per_sd': (mean - ce) / sd, 'premium_share_of_max': share,
                    'prob_below_ce': float(np.sum(designs.probs[0][outcomes < ce])),
                    'r_drop_lowest_1': tail_sensitivity(outcomes, designs.probs[0], ce, 1),
                    'r_drop_lowest_3': tail_sensitivity(outcomes, designs.probs[0], ce, 3),
                    'holt_laury_equivalent_crra': equivalent_crra_on(HOLT_LAURY_LOTTERY, share),
                    'wealth_assumed': wealth,
                }
                row.update({f'crra_if_theta_worth_{d}': implied_crra(b['r'], d, wealth) for d in dollar_grid})
                row.update({f'breakeven_dollars_per_theta_rho_{rho:g}': breakeven_dollars_per_theta(b['r'], rho, wealth)
                            for rho in rho_grid})
                comparisons.append(row)
    return pd.DataFrame(estimates), pd.DataFrame(comparisons), pd.concat(model_a_designs, ignore_index=True)
