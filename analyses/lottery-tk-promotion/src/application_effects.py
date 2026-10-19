"""
Effect of TK automatic promotion on kindergarten applications per school.

Model (main.tex, "Effect of promotion on applications"). In cycle t the
but-for cohort is N_t lottery applicants plus P_t families who accepted
promotion; P_kt accepted at TK school k. A general applicant ranks school k
with probability s_k. A promoted family from school j would have ranked its
own school with probability q and any other school k with probability s_k.
Then observed applications A_k = N s_k, and the but-for count is

    A^cf_k = (N + P) s_k + P_k (q - s_k),

so log A_k - log A^cf_k = -log(1 + P/N)                    (every school)
                          - log(1 + P_k (q - s_k) / ((N + P) s_k))  (TK schools)

The first term is common to all schools and absorbed by year effects; the
second is what a difference-in-differences between promotion-eligible and
other schools estimates. The same algebra holds for first-choice requests
with q replaced by f, the probability a promoted family would have ranked
its own school first. Inverting the predicted DiD at the estimated one
gives the implied q (or f).

Outcomes: all ranked requests (num_requests) and first-choice requests
(num_first_choice), observed -- i.e. without adding promoted families back.
Treatment: the 40 promotion-eligible buildings; first treated cycle 25-26.
The 26-27 cycle presumably had promotion too, but its counts are not in the
roster, so it enters only as a second post-period dummy.
"""

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import optimize

DID_YEARS = ['21-22', '22-23', '23-24', '24-25', '25-26', '26-27']
PRE_YEARS = DID_YEARS[:4]
REFERENCE_YEAR = '24-25'
OUTCOMES = {'num_requests': 'All ranked requests', 'num_first_choice': 'First-choice requests'}


def balanced_panel(lottery_df, years=DID_YEARS):
    """Schools observed in every cycle of `years`, with treatment indicators
    and promotion intensity P_k / A_k(24-25)."""
    panel = lottery_df[lottery_df.ay.isin(years)].copy()
    complete = panel.groupby('school_name').ay.nunique() == len(years)
    panel = panel[panel.school_name.isin(complete[complete].index)]
    panel['treated'] = panel.promotion_accepted.notna()
    reference = panel[panel.ay == REFERENCE_YEAR].set_index('school_name')
    for col in OUTCOMES:
        panel[f'intensity_{col}'] = (
            panel.promotion_accepted.fillna(0) / panel.school_name.map(reference[col])
        )
    for ay in ('25-26', '26-27'):
        panel[f'post_{ay[:2]}'] = (panel.ay == ay).astype(int)
    return panel


def summary_stats(panel):
    """Per-school means by group and cycle, plus pre-to-post changes."""
    rows = []
    for treated, group in panel.groupby('treated'):
        label = 'Promotion-eligible' if treated else 'Other schools'
        pre = group[group.ay.isin(PRE_YEARS)]
        row = {'group': label, 'n_schools': group.school_name.nunique()}
        for col, name in OUTCOMES.items():
            by_year = group.groupby('ay')[col].mean()
            row.update({f'{col}_{ay}': by_year[ay] for ay in DID_YEARS})
            row[f'{col}_pre_mean'] = pre.groupby('school_name')[col].mean().mean()
            row[f'{col}_change_24_25'] = by_year['25-26'] / by_year['24-25'] - 1
            row[f'{col}_change_24_26'] = by_year['26-27'] / by_year['24-25'] - 1
        if treated:
            accepted = group.drop_duplicates('school_name').promotion_accepted
            row.update(promoted_per_school=accepted.mean(), promoted_total=accepted.sum(),
                       intensity_requests_mean=group.drop_duplicates('school_name').intensity_num_requests.mean(),
                       intensity_first_choice_mean=group.drop_duplicates('school_name').intensity_num_first_choice.mean())
        rows.append(row)
    return pd.DataFrame(rows)


def _fit(formula, data, method):
    if method == 'ols':
        return smf.ols(formula, data).fit(cov_type='cluster', cov_kwds={'groups': data.school_name})
    import statsmodels.api as sm
    return smf.glm(formula, data, family=sm.families.Poisson()).fit(
        cov_type='cluster', cov_kwds={'groups': data.school_name})


def _model_regressor(panel, col, n_applicants, n_promoted):
    """The model's predicted TK-specific log effect under q = 1,
    -log(1 + P_k (1 - s_k) / ((N + P) s_k)), with s_k from the pre-period
    reference cycle (24-25) so the regressor does not use the outcome."""
    reference = panel[panel.ay == REFERENCE_YEAR].set_index('school_name')
    s = reference[col] / reference.ay_num_requests
    p_k = reference.promotion_accepted.fillna(0)
    effect = -np.log1p(p_k * (1 - s) / ((n_applicants + n_promoted) * s))
    return panel.school_name.map(effect)


DID_SPECS = [
    # (label, cycles used, regressors beyond the fixed effects, reported term)
    ('2x2: 2024-25 vs. 2025-26', ['24-25', '25-26'], 'treated_int:post_25', 'treated_int:post_25'),
    ('Pooled pre-period (2021-22 to 2024-25)', DID_YEARS,
     'treated_int:post_25 + treated_int:post_26', 'treated_int:post_25'),
    ('Pooled pre-period + group linear trend', DID_YEARS,
     'treated_int:post_25 + treated_int:post_26 + treated_int:trend', 'treated_int:post_25'),
    ('Model intensity + group linear trend', DID_YEARS,
     'model_effect:post_25 + treated_int:post_26 + treated_int:trend', 'model_effect:post_25'),
]


def did_regressions(panel, n_applicants, n_promoted):
    """
    Two-way fixed-effects DiD (school and cycle effects, school-clustered
    SEs) for each spec in DID_SPECS, by OLS on log counts and Poisson PPML
    on counts. Binary specs report the treated x 2025-26 effect (also
    2026-27 where included); the intensity spec reports the coefficient on
    the model-predicted effect, which equals 1 if every promoted family
    would have ranked its own school (q = 1) and scales roughly with q.
    """
    rows = []
    base = panel.assign(treated_int=panel.treated.astype(int),
                        trend=panel.ay.str[:2].astype(int) - int(REFERENCE_YEAR[:2]))
    for col, name in OUTCOMES.items():
        data = base.assign(log_y=np.log(base[col]),
                           model_effect=_model_regressor(base, col, n_applicants, n_promoted))
        for label, years, rhs, term in DID_SPECS:
            sub = data[data.ay.isin(years)]
            for method, lhs in (('ols', 'log_y'), ('ppml', col)):
                fit = _fit(f'{lhs} ~ C(school_name) + C(ay) + {rhs}', sub, method)
                terms = [term] + (['treated_int:post_26'] if 'post_26' in rhs else [])
                for t in terms:
                    rows.append({
                        'outcome': name, 'spec': label, 'method': method.upper(),
                        'term': t.replace('treated_int', 'treated'),
                        'coef': fit.params[t], 'se': fit.bse[t], 'p': fit.pvalues[t],
                        'pct_effect': np.expm1(fit.params[t]) if 'model_effect' not in t else np.nan,
                        'n_obs': int(fit.nobs), 'n_schools': sub.school_name.nunique(),
                    })
    return pd.DataFrame(rows)


def event_study(lottery_df, schools, reference=REFERENCE_YEAR):
    """log y_kt = a_k + g_t + sum_{t != ref} b_t D_k 1[t], over every cycle,
    for the balanced-panel schools (earlier cycles unbalanced)."""
    data = lottery_df[lottery_df.school_name.isin(schools)].copy()
    data['treated'] = data.promotion_accepted.notna().astype(int)
    years = sorted(data.ay.unique())
    for ay in years:
        data[f'd_{ay[:2]}'] = ((data.ay == ay) & (data.treated == 1)).astype(int)
    rows = []
    for col, name in OUTCOMES.items():
        terms = [f'd_{ay[:2]}' for ay in years if ay != reference]
        fit = _fit(f'np.log({col}) ~ C(school_name) + C(ay) + ' + ' + '.join(terms), data, 'ols')
        ci = fit.conf_int()
        for ay in years:
            term = f'd_{ay[:2]}'
            rows.append({'outcome': name, 'ay': ay,
                         'coef': 0.0 if ay == reference else fit.params[term],
                         'ci_lower': 0.0 if ay == reference else ci.loc[term, 0],
                         'ci_upper': 0.0 if ay == reference else ci.loc[term, 1]})
    pre = [ay for ay in years if ay < reference]
    return pd.DataFrame(rows), pre


def model_prediction(panel, n_applicants, n_promoted, col, q):
    """Mean over treated schools of the model's TK-specific log effect,
    -log(1 + P_k (q - s_k) / ((N + P) s_k)), with s_k = A_k(25-26) / N."""
    treated = panel[(panel.ay == '25-26') & panel.treated]
    s = treated[col] / n_applicants
    extra = treated.promotion_accepted * (q - s) / ((n_applicants + n_promoted) * s)
    with np.errstate(invalid='ignore'):
        return float(np.mean(-np.log1p(extra)))


def implied_own_school_probability(panel, n_applicants, n_promoted, col, did_estimate):
    """q (or f) at which the model's predicted DiD equals the estimate. Values
    outside [0, 1] are reported as-is: below 0 means applications at TK
    schools rose relative to the rest, above 1 means they fell by more than
    the promoted families alone can account for."""
    g = lambda q: model_prediction(panel, n_applicants, n_promoted, col, q) - did_estimate
    try:
        return optimize.brentq(g, -5.0, 10.0)
    except ValueError:
        return np.nan


def model_comparison(panel, did, n_applicants, n_promoted):
    """Predicted DiD under q = 1 and q = 1/2, and the q implied by each binary
    OLS estimate of the 2025-26 effect (with its 95% CI)."""
    rows = []
    for col, name in OUTCOMES.items():
        predicted_1 = model_prediction(panel, n_applicants, n_promoted, col, 1.0)
        estimates = did[(did.outcome == name) & (did.method == 'OLS') & (did.term == 'treated:post_25')]
        for est in estimates.itertuples():
            ci = sorted(implied_own_school_probability(panel, n_applicants, n_promoted, col, b)
                        for b in (est.coef - 1.96 * est.se, est.coef + 1.96 * est.se))
            rows.append({
                'outcome': name, 'spec': est.spec,
                'common_effect_all_schools': -np.log1p(n_promoted / n_applicants),
                'predicted_did_q1': predicted_1,
                'predicted_did_q_half': model_prediction(panel, n_applicants, n_promoted, col, 0.5),
                'estimated_did': est.coef, 'estimated_did_se': est.se,
                'implied_q': implied_own_school_probability(panel, n_applicants, n_promoted, col, est.coef),
                'implied_q_ci_lower': ci[0], 'implied_q_ci_upper': ci[1],
            })
    return pd.DataFrame(rows)
