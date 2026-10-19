"""
Capacity-constrained simulation of the kindergarten lottery.

The baseline expected value (src/capacity.py's sequential_expected_value)
gives one representative family an independent admission draw P_k at every
school down a list of all 86 schools. Summed over families, that overfills
the best schools: a family "collects" a ticket at each of them, so far more
families land in the top schools than there are seats.

This module instead simulates a full applicant cohort under the logit model
in main.tex, with aggregate seat limits:

  1. Each family i draws u_ik = theta_k + eps_ik and u_i0 = xi + eps_i0
     (i.i.d. Type-I EV), and ranks every school with u_ik > u_i0 in order of
     u_ik. xi is calibrated so the simulated mean list length matches the
     observed requests-per-applicant in the simulated year.
  2. Seats are assigned by random serial dictatorship (a single lottery
     number per family, no priorities -- SFUSD's sibling / attendance-area /
     CTIP1 priorities aren't observed without the choice data). Each school's
     seats are its CDE kindergarten enrollment; standalone TK sites with no
     CDE K count get their own request volume, i.e. never bind (matching the
     P_k = 1 fallback in src/capacity.py).
  3. Families left unplaced are administratively assigned, as SFUSD does, to
     a random remaining seat anywhere in the district.

The expected value of lottery participation is the mean theta of families'
assignments (common-value component only, so it is directly comparable to
the baseline and to theta_k of the certain TK seat). Unplaced families with
no seat left anywhere are reported but excluded from the mean.
"""

import numpy as np
import pandas as pd


def _seats(school_names, admission_df, requests):
    adm = admission_df.set_index('school_name')
    return np.array([
        adm.capacity[s] if adm.matched.fillna(False)[s] and not np.isnan(adm.capacity[s]) else requests[s]
        for s in school_names
    ]).round().astype(int)


def _calibrate_outside_option(theta, eps, eps0, target_length, iters=50):
    lo, hi = -20.0, 20.0
    for _ in range(iters):
        xi = (lo + hi) / 2
        mean_length = ((theta + eps) > (xi + eps0)[:, None]).sum(axis=1).mean()
        lo, hi = (xi, hi) if mean_length > target_length else (lo, xi)
    return (lo + hi) / 2


def _simulate_once(theta, seats, n_families, target_length, rng):
    eps = rng.gumbel(size=(n_families, len(theta)))
    eps0 = rng.gumbel(size=n_families)
    xi = _calibrate_outside_option(theta, eps, eps0, target_length)
    utility = theta + eps
    ranked = utility > (xi + eps0)[:, None]

    left = seats.copy()
    assigned = np.full(n_families, -1)
    lottery_placed = np.zeros(n_families, dtype=bool)
    order = rng.permutation(n_families)
    for i in order:
        prefs = np.flatnonzero(ranked[i])
        for k in prefs[np.argsort(-utility[i, prefs])]:
            if left[k] > 0:
                left[k] -= 1
                assigned[i] = k
                lottery_placed[i] = True
                break
    for i in order[~lottery_placed[order]]:
        if left.sum() == 0:
            break
        k = rng.choice(len(theta), p=left / left.sum())
        left[k] -= 1
        assigned[i] = k

    has_seat = assigned >= 0
    return np.bincount(assigned[has_seat], minlength=len(theta)), {
        'expected_value': theta[assigned[has_seat]].mean(),
        'share_placed_from_list': lottery_placed.mean(),
        'share_administrative': (has_seat & ~lottery_placed).mean(),
        'share_no_seat': (~has_seat).mean(),
        'mean_list_length': ranked.sum(axis=1).mean(),
        'outside_option_xi': xi,
    }


def capacity_constrained_expected_value(lottery_df, school_theta, admission_df, ay, n_sims=20, seed=0):
    """
    Simulate the lottery for application year `ay` n_sims times. Uses the
    but-for cohort (num_requests_total / ay_num_applicants_total, i.e. as if
    no family had opted out via TK promotion). Returns (mean over draws, a
    DataFrame of per-draw results). The summary also carries the assigned-
    school distribution as lottery_outcomes / lottery_probs.
    """
    year = lottery_df[lottery_df.ay == ay]
    requests = year.set_index('school_name').num_requests_total
    n_families = int(year.ay_num_applicants_total.iloc[0])
    target_length = requests.sum() / n_families

    schools = school_theta[school_theta.school_name.isin(requests.index)]
    theta = schools.theta.values
    seats = _seats(schools.school_name, admission_df, requests)

    rng = np.random.default_rng(seed)
    counts, results = zip(*[_simulate_once(theta, seats, n_families, target_length, rng) for _ in range(n_sims)])
    draws = pd.DataFrame(results)
    summary = draws.mean().to_dict()
    # Distribution of the assigned school's theta among families who get a
    # seat: the capacity-constrained lottery as a set of outcomes and
    # probabilities (src/risk_aversion.py).
    shares = np.sum(counts, axis=0) / np.sum(counts)
    summary['lottery_outcomes'] = theta[shares > 0]
    summary['lottery_probs'] = shares[shares > 0]
    summary.update({
        'expected_value_sd': draws.expected_value.std(),
        'n_families': n_families,
        'target_list_length': target_length,
        'total_seats': int(seats.sum()),
        'n_schools': len(theta),
        'ay': ay,
    })
    return summary, draws
