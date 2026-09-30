"""
School "quality" (demand) estimation from lottery request shares.

Follows the identification argument in main.tex: if a family applies to school
k whenever its (idiosyncratic-shock-perturbed) quality theta_k exceeds an
outside-option threshold theta_0t, then in a logit model

    ln A_kt - ln(N_t - A_kt) = theta_k - theta_0t

Since any individual school's requests are a small share of citywide volume,
ln(N_t - A_kt) ~= ln(N_t), so we approximate with

    log_delta_kt = ln(A_kt) - ln(N_t) ~= theta_k - theta_0t

and estimate theta_k / theta_0t as school and application-year fixed effects
via OLS on log_delta (no intercept, so both sets of dummies are directly
interpretable levels).
"""

import re

import numpy as np
import pandas as pd
import statsmodels.formula.api as sm

FORMULA = 'log_delta ~ C(school_name) + C(ay) + 0'


def _extract_name(param, prefix):
    return re.search(r'\[(.*?)\]$', param).group(1)


def fit_quality_model(lottery_df, requests_col='num_requests_total'):
    """
    Fit the school/year fixed-effect model on log request share.

    requests_col picks which request-volume column defines the share:
      - 'num_requests_total' (default): reconstructs but-for demand at
        promotion-eligible schools in the treatment year, so the one-time
        opt-out-driven demand suppression doesn't bias down our estimate of
        those schools' underlying quality theta_k.
      - 'num_requests': the raw observed volume (matches the original
        exploratory notebook), kept available for comparison.

    Returns (fitted_model, theta_df, ay_effects) where theta_df has one row
    per school with its estimated theta and 95% CI, and ay_effects is a dict
    of application-year -> estimated theta_0t (the outside-option level).
    """
    df = lottery_df.assign(
        log_delta=lambda _df: np.log(_df[requests_col]) - np.log(_df.ay_num_requests)
    )
    model = sm.ols(FORMULA, df).fit()

    conf_int = model.conf_int(0.05).reset_index()
    conf_int.columns = ['param', 'ci_lower', 'ci_upper']

    theta_df = pd.DataFrame(model.params).reset_index()
    theta_df.columns = ['param', 'theta']
    theta_df = theta_df.merge(conf_int, on='param')

    school_mask = theta_df.param.str.contains(r'C\(school_name\)')
    ay_mask = theta_df.param.str.contains(r'C\(ay\)')

    school_theta = theta_df[school_mask].assign(
        school_name=lambda _df: _df.param.apply(_extract_name, prefix='school_name')
    )[['school_name', 'theta', 'ci_lower', 'ci_upper']].reset_index(drop=True)

    ay_theta = theta_df[ay_mask].assign(
        ay=lambda _df: _df.param.apply(_extract_name, prefix='ay')
    ).set_index('ay')['theta'].to_dict()

    return model, school_theta, ay_theta
