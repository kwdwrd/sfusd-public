"""
TK -> K automatic-promotion *rates* by school, and their relationship to
measured school quality.

The promotion roster's "Eligible for Promotion" column is (to within a
student or two at almost every school) the school's 2024-25 TK enrollment
as reported to CDE (GR_TK in cdenroll2425.txt) -- see load_promotion_rates,
which carries both so the match can be checked. So

    promotion_rate_k = promotion_accepted_k / promotion_eligible_k

is exactly "K promotions over last year's TK count".

Quality measures joined on:
  - theta_k: the demand-based school fixed effect from src/quality_model.py
  - SBAC 2023-24 % met-or-exceeded standard (ELA, math), all students, all
    tested grades -- an achievement-based measure independent of the lottery
  - unduplicated-pupil share (FRPM/EL/foster, CALPADS) as a demographic control,
    since SBAC levels are mostly a function of who attends
"""

import io
import os
import zipfile

import numpy as np
import pandas as pd

SCHOOLS_PATH = '../../data/processed/schools.csv'
CALPADS_PATH = '../../data/processed/calpads.csv'
CDE_ENROLLMENT_2425_PATH = '../../data/enrollment/raw/cdenroll2425.txt'
SBAC_ZIP_PATH = '../../data/sbac/raw/sb_ca2024_all_csv_v1.zip'
SBAC_MEMBER = 'sb_ca2024_all_csv_v1.txt'
SBAC_CACHE_PATH = 'output/sbac_2024_sfusd_schools.csv'

SFUSD_DISTRICT_CODE = '68478'
SBAC_ALL_STUDENTS = '1'
SBAC_ALL_GRADES = '13'
SBAC_TESTS = {'1': 'sbac_ela', '2': 'sbac_math'}


def load_sbac_2024(cache_path=SBAC_CACHE_PATH):
    """SFUSD school-level SBAC 2023-24 % met-or-above, ELA and math. The raw
    file is ~1GB, so the SFUSD all-student/all-grade slice is cached."""
    if not os.path.exists(cache_path):
        keep = []
        with zipfile.ZipFile(SBAC_ZIP_PATH) as z, z.open(SBAC_MEMBER) as f:
            lines = io.TextIOWrapper(f, encoding='latin1')
            header = next(lines).rstrip('\n').split('^')
            i_dist, i_group, i_grade = (header.index(c) for c in ('District Code', 'Student Group ID', 'Grade'))
            for line in lines:
                row = line.rstrip('\n').split('^')
                if row[i_dist] == SFUSD_DISTRICT_CODE and row[i_group] == SBAC_ALL_STUDENTS and row[i_grade] == SBAC_ALL_GRADES:
                    keep.append(row)
        raw = pd.DataFrame(keep, columns=header)
        raw = raw[raw['School Code'] != '0000000']
        out = raw.assign(
            calpads_school_code=raw['School Code'].astype(int),
            pct_met=pd.to_numeric(raw['Percentage Standard Met and Above'], errors='coerce'),
            test=raw['Test ID'].map(SBAC_TESTS),
        ).dropna(subset=['test'])
        out = out.pivot_table(index='calpads_school_code', columns='test', values='pct_met').reset_index()
        out.columns.name = None
        out.to_csv(cache_path, index=False)
    return pd.read_csv(cache_path)


def load_cde_tk_2425():
    cde = pd.read_csv(CDE_ENROLLMENT_2425_PATH, sep='\t', dtype=str, encoding='latin1')
    cde = cde[(cde.DistrictName == 'San Francisco Unified') & (cde.AggregateLevel == 'S') & (cde.ReportingCategory == 'TA')]
    return pd.DataFrame({
        'calpads_school_code': cde.SchoolCode.astype(int),
        'cde_tk_2425': pd.to_numeric(cde.GR_TK, errors='coerce'),
    })


def load_unduplicated_share(year=2023):
    calpads = pd.read_csv(CALPADS_PATH)
    calpads = calpads[calpads.year == year].pivot_table(index='school_id', columns='metric', values='value')
    return (calpads.unduplicated_count / calpads.enrollment).rename('unduplicated_share').reset_index()


def load_promotion_rates(promotion_df, school_theta, admission_df):
    """
    One row per promotion-eligible school (multi-program schools, e.g.
    DeAvila's two Chinese-immersion strands, are summed to the building),
    with the promotion rate and every quality measure.
    """
    schools = pd.read_csv(SCHOOLS_PATH)[['school_id', 'calpads_school_code']]
    rates = (
        promotion_df.dropna(subset=['promotion_eligible'])
        .groupby('school_id', as_index=False)[['promotion_eligible', 'promotion_accepted']].sum()
        .assign(promotion_rate=lambda _df: _df.promotion_accepted / _df.promotion_eligible)
        .merge(schools, on='school_id', how='left')
        .merge(load_cde_tk_2425(), on='calpads_school_code', how='left')
        .merge(load_sbac_2024(), on='calpads_school_code', how='left')
        .merge(load_unduplicated_share(), on='school_id', how='left')
    )
    return rates.merge(school_theta[['school_name', 'theta']].merge(
        admission_df[['school_name', 'admission_prob']], on='school_name'
    ).merge(_lottery_school_names(), on='school_name'), on='school_id', how='left')


def _lottery_school_names():
    from data import load_school_mapping
    return load_school_mapping()[['school_id', 'school_name']]


def certainty_premium(rates, expected_value, suffix=''):
    """
    Promotion-rate-aware lottery distaste. Under the individual model in
    main.tex (Section "Model"), family i with a TK seat at k accepts
    promotion iff

        delta_k + c + eps_i1 >= EU + eps_i0,

    with eps i.i.d. Type-I EV, so Pr(accept_k) = Lambda(delta_k - EU + c) and

        c_k = logit(r_k) - (delta_k - EU) = logit(r_k) + distaste_weighted_k.

    The old measure (distaste_weighted = EU - delta_k) is the premium of the
    *marginal* family (r_k = 1/2); the observed rate shifts it up where most
    families accepted and down where most did not.

    suffix names the expected-value variant (e.g. '_capacity'), so the
    baseline and capacity-constrained columns can sit in one table.
    """
    r = rates.promotion_rate.clip(0.5 / rates.promotion_eligible, 1 - 0.5 / rates.promotion_eligible)
    logit_rate = np.log(r / (1 - r))
    distaste = expected_value - rates.theta
    return rates.assign(**{
        'logit_rate': logit_rate,
        f'distaste_weighted{suffix}': distaste,
        f'certainty_premium{suffix}': logit_rate + distaste,
    })


def pooled_certainty_premium(rates, expected_value):
    """
    Single c (and, unrestricted, a free scale on delta_k - EU) by
    grouped-logit MLE: each eligible family is a Bernoulli trial with
    Pr(accept) = Lambda(c + beta * (theta_k - EU)). beta=1 is the
    model-consistent restriction (theta_k is already on the eps scale).
    """
    import statsmodels.api as sm
    df = rates.dropna(subset=['theta'])
    endog = np.column_stack([df.promotion_accepted, df.promotion_eligible - df.promotion_accepted])
    x = (df.theta - expected_value).values
    restricted = sm.GLM(endog, np.ones((len(df), 1)), family=sm.families.Binomial(), offset=x).fit()
    free = sm.GLM(endog, sm.add_constant(x), family=sm.families.Binomial()).fit()
    return restricted, free
