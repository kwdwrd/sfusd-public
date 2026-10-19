"""
Loading and cleaning of SFUSD kindergarten lottery request data and the
TK-to-K automatic promotion roster.

This mirrors the cleaning logic originally worked out interactively in
lottery-tk-promotion.ipynb, factored into reusable functions so it can be
shared between that notebook and the extended distaste analysis in
run_distaste_analysis.py.
"""

import re

import numpy as np
import pandas as pd

LOTTERY_REQUESTS_PATH = '../../data/lottery/merged/sfusd-lottery-all.csv'
SCHOOL_MAPPING_PATH = '../../data/lottery/merged/sfusd-school-id-mapping.csv'
PROMOTION_PATH = './Promotes from TK to K - Sheet1.csv'

TREATMENT_YEAR = '25-26'

# Main estimation window: the four pre-treatment, post-COVID application
# cycles. These are the years families deciding on promotion (early 2025)
# had seen, they exclude the post-decision 26-27 cycle, and their request
# volume matches the CDE enrollment years used for seat capacity
# (src/capacity.py) -- the 2015-18 cycles saw over twice today's requests.
ESTIMATION_YEARS = ['21-22', '22-23', '23-24', '24-25']


def load_promotion(path=PROMOTION_PATH):
    """Load the TK->K automatic-promotion roster: eligible/accepted counts by school."""
    promotion_df = pd.read_csv(path)
    return promotion_df.rename(columns={
        'idSchool': 'school_id',
        'Elilgible for Promotion': 'promotion_eligible',
        'Actually Promoted': 'promotion_accepted',
    })[['school_id', 'promotion_eligible', 'promotion_accepted']]


def load_school_mapping(path=SCHOOL_MAPPING_PATH):
    return pd.read_csv(path)


def load_kindergarten_requests(
    lottery_path=LOTTERY_REQUESTS_PATH,
    school_path=SCHOOL_MAPPING_PATH,
    promotion_path=PROMOTION_PATH,
):
    """
    Build a school x application-year panel of kindergarten lottery request
    volume, joined to TK automatic-promotion eligibility/acceptance.

    Returns a DataFrame with one row per (ay, school_id) with:
      - num_requests: raw observed ranked-seat requests for the school that year
      - num_requests_total: num_requests, plus (for the 25-26 treatment year at
        promotion-eligible schools) the number of families who accepted automatic
        promotion instead of entering the lottery. This reconstructs the "but-for"
        request volume as if TK opt-out did not exist, which matters because the
        opt-out mechanically suppresses 25-26 demand at exactly the schools whose
        quality we're trying to estimate.
      - num_first_choice: requests ranking the school first (observed)
      - ay_num_requests: total citywide kindergarten lottery requests that year
        (from families' #1-ranked choice only, i.e. actual lottery participants;
        promotion-accepting families never enter this count since they never rank
        anything)
      - ay_num_applicants_total: ay_num_requests plus (in the 25-26 treatment
        year) every family that accepted promotion -- the but-for applicant
        count matching num_requests_total
      - promotion_eligible / promotion_accepted: from the roster, NaN if the
        school had no TK program / automatic-promotion offer that year
      - is_treatment: True for promotion-eligible schools in the 25-26 cycle
    """
    raw = pd.read_csv(lottery_path)
    school_df = load_school_mapping(school_path)
    promotion_df = load_promotion(promotion_path)

    lottery_df = raw[
        (raw.ay != '21-22') | (raw.choice_min != 6) | (raw.choice_max != 10)
    ].assign(
        num_requests=raw.num_requests.fillna(0).apply(
            lambda x: re.sub(r'(?:\,|\.\d*$)', '', str(x))
        )
    ).assign(
        num_requests=lambda _df: np.where(
            _df.num_requests.str.len() == 0, 0, _df.num_requests.fillna(0)
        ).astype(int)
    ).merge(
        raw[raw.grade == 'K'][['ay', 'school_id']].drop_duplicates()
    )[lambda _df: _df.grade == 'K']

    unique_student_df = (
        lottery_df[lottery_df.choice_max == 1][['ay', 'num_requests']]
        .groupby('ay', as_index=False).sum()
        .rename(columns={'num_requests': 'ay_num_requests'})
    )

    first_choice_df = (
        lottery_df[lottery_df.choice_max == 1][['ay', 'school_id', 'num_requests']]
        .groupby(['ay', 'school_id'], as_index=False).sum()
        .rename(columns={'num_requests': 'num_first_choice'})
    )

    lottery_df = (
        lottery_df[lambda _df: _df.choice_min > 0][['ay', 'school_id', 'num_requests']]
        .groupby(['ay', 'school_id'], as_index=False).sum()
        .merge(first_choice_df, on=['ay', 'school_id'], how='left')
        .fillna({'num_first_choice': 0})
        .merge(school_df, on='school_id')
    )
    lottery_df = lottery_df[
        ~lottery_df.school_id.isin(lottery_df[lottery_df.num_requests == 0].school_id.unique())
    ].merge(unique_student_df, on='ay')

    # The roster has one row per TK program, and two buildings run two
    # (DeAvila's Chinese-immersion strands, Huerta's Spanish strands); sum to
    # the building before merging so the panel stays one row per school-year.
    promotion_by_school = promotion_df.groupby('school_id', as_index=False)[
        ['promotion_eligible', 'promotion_accepted']
    ].sum(min_count=1)
    lottery_df = lottery_df.merge(promotion_by_school, on='school_id', how='left').assign(
        is_treatment=lambda _df: (_df.ay == TREATMENT_YEAR) & (~pd.isnull(_df.promotion_accepted)),
        num_requests_total=lambda _df: _df.num_requests + np.where(
            _df.ay == TREATMENT_YEAR, _df.promotion_accepted.fillna(0), 0
        ),
        ay_num_applicants_total=lambda _df: _df.ay_num_requests + np.where(
            _df.ay == TREATMENT_YEAR, promotion_df.promotion_accepted.sum(), 0
        ),
    )

    return lottery_df
