# Review status

## Round 1 — started 2026-10-18

| Agent | Status | Report |
|---|---|---|
| numbers-auditor | done | [round1-numbers-auditor.md](round1-numbers-auditor.md) |
| replicator | done — reproduces byte-identical | [round1-replicator.md](round1-replicator.md) |
| paper-referee | done — **Reject** | [round1-paper-referee.md](round1-paper-referee.md) |
| lit-scout | done | [round1-lit-scout.md](round1-lit-scout.md) |
| copy-editor | deferred — content not settled; runs after the main.tex fix pass | |

### Must-fix before anything else
1. **Abstract sign** (`main.tex:26`): says eligible schools saw "relatively more
   applicants". False as written -- all first-choice DiD estimates are negative
   (verified against `output/application_effects_did.csv`). Author direction
   (2026-10-18): the claim is true for **rank-1** requests once promoted families
   are counted as ranking their own school first, so refine rather than reverse.
   Arithmetic from `first_rankings_by_year.csv`: eligible share 48.9% -> 50.4%
   (counts 1,853 -> 2,007, +8.3%, against other schools' 1,938 -> 1,974, +1.9%);
   log DiD +0.061, i.e. +6.3% relative. On all ranked requests the same
   imputation gives only +2.0%, so the claim must be specific to rank-1.
   **Caveat to resolve:** that imputation sets f = 1. The sign flips at
   f = 0.801, and the paper's own implied f values are 0.60 (2x2), 0.77 (trend),
   0.74 (PPML intensity) and 0.30 (pooled) -- all below break-even. The abstract
   must therefore state the f = 1 accounting convention explicitly. Also,
   `main.tex` currently says the effect "vanishes" under this imputation; for
   rank-1 it modestly reverses.
2. **Duplicate-school bug** in `compute_distaste` -- **FIXED 2026-10-18.**
   `src/distaste.py` merged the roster program by program while `src/data.py`
   summed to the building. Patched to group by `school_id` with
   `sum(min_count=1)`, mirroring `data.py:114-119`.
   Result: `lottery_distaste_estimates.csv` 42 -> 40 rows, no duplicate ids.
   `output/numbers.tex` and `output/tables/*` are **byte-identical**, so no
   macro and no number in `main.tex` changes. Family-weighted means unchanged
   exactly (1.2449, 0.4309); c-hat, EU, free slope and take-up unchanged.
   Unreported by-school means moved +0.024 (1.3013 -> 1.3257 baseline,
   0.4873 -> 0.5117 capacity). Only `figures/quality_by_promotion_status.png`
   changed -- its right panel now plots 40 points instead of 42.
   `robustness_quality_measurement.csv` moved only in the last float digit.
   Note: this makes `main.tex:128`'s parenthetical ("fixing this moved no
   reported estimate by more than 0.01") literally true for this fix too --
   nothing reported moved at all. That sentence describes the earlier `data.py`
   fix and could be sharpened to cover both.
3. **"Best vs median" equivalence** (`main.tex:26`, `:38`) is true for the
   capacity version (c-hat 1.60 vs a 1.70 gap) but not the baseline (2.41, which
   is the gap to the #55 school). Needs an explicit label. AUTHOR DECISION.
4. **Abstract's risk-aversion sentence** overclaims against the paper's own
   §risk-aversion discussion. AUTHOR DECISION.
5. **Build order is undocumented and the documented command fails cold:**
   `validate_risk_aversion.py` must precede `run_distaste_analysis.py`, and
   `plot_first_rankings.py` is a required separate step. Fix in code, or correct
   the CLAUDE.md Build section. AUTHOR DECISION (CLAUDE.md edit).

### Open, lower priority
Stray draft marker at `main.tex:36`; hand-typed 5.0 requests per applicant
(target is 4.84); "top-5 ranks" at `:64`; the 2026-27 contradiction between
`:64` and `:177`; mean-of-ratios vs ratio-of-means labelling at `:160`/`:169`;
the CDE-roster "within a student or two" claim at `:238`; selective mover list
at `:281`; "noisiest cycle" at `:281`; collinearity range at `:240`; untraced
claim at `:128`; undocumented 39 designs / 11 dropped families at `:304`;
unpinned Python dependencies; `src/robustness.py:35` uses `n_sims=1`.

## Author directions received 2026-10-18 (session 2)
1. **Capacity EU is the relevant model; baseline EU "isn't a good model."** Headline
   numbers become the capacity-constrained ones. NB: referee M2.3 argues neither
   EU is right, since the code sums over all 71 schools while main.tex:111 says
   the rank list, and a 5-rank EU is neither computed nor bounded.
2. **Correct the f inconsistencies.** Maintained assumption is f = 1: accepting a
   guaranteed seat is taken to reveal the family would have ranked that school
   first. The DiD-implied f (0.30-0.77) must be reconciled, and the distinction
   between "would have ranked it first" and "prefers it to the lottery" called
   out explicitly somewhere.
3. **Risk aversion out of the abstract**; pin down the appendix analysis.
4. **Fix `compute_distaste`** — DONE (see above).
5. **Fix the build order if the code doesn't run** — DONE. `run_distaste_analysis.py`
   now calls `ensure_risk_aversion_validation()` (reruns the ~50s validator only
   when `output/risk_aversion_validation.csv` is missing or older than
   `validate_risk_aversion.py` / `src/risk_aversion.py`) and `plot_first_rankings.main()`
   before `make_numbers.main()`. Verified: runs from the clean failure state,
   exit 0, every output byte-identical, validator skipped when current.
6. **The $200M figure** comes from: 30% of SF students private; 55k in SFUSD;
   hence ~23.6k not in SFUSD; LCFF ~$10k per student. **The arithmetic does not
   reach $200M — see below.**

## The $200M calculation (main.tex:32) — UNRESOLVED, needs an author decision
Inputs verified:
- SFUSD enrollment: CDE `cdenroll2324-v2.txt`, SFUSD district total **55,452**
  (Charter=ALL) or **48,736** (Charter=N), 2023-24. The 55k figure is the
  charter-inclusive one. Note LCFF flows to SFUSD, not to charters, so the
  revenue calculation arguably wants 48,736.
- SF private share ~30-31%: ~23,964 private students (SFEDup, 2023-24), and KQED
  citing state data for 30% of SF K-12. Implies ~79,900 total SF students and
  ~23.6k non-SFUSD, matching the author's chain.
- **"The national average of 15%" is WRONG.** NCES puts private enrollment at
  about **9%** of US K-12 (fall 2021 and 2019). (lit-scout, §1E.)

Arithmetic at LCFF = $10k/student:
| Counterfactual | Students gained | Revenue |
|---|---|---|
| 30% -> 15% (what the sentence says) | ~11,980 | **$120M** |
| 30% -> 9% (the actual national average) | ~16,780 | **$168M** |
| 30% -> 0% (the author's "23.6k x $10k" chain) | ~23,960 | **$240M** |

So "nearly $200 million" matches none of them. Reaching $200M on the 30%->15%
counterfactual needs ~$16,700 per student, which is total per-pupil revenue
rather than marginal LCFF — and much of total per-pupil funding (parcel taxes,
fixed federal grants) does not scale with enrollment.
**Recommendation:** 30% -> 9% at ~$10k LCFF, stated as "about $170 million", with
each input cited. Also note referee M5: the families in this counterfactual never
enter the lottery, so they are exactly the families outside the estimation
sample; the claim needs an explicit out-of-sample flag or should be dropped
(route (b)).

## LCFF check (2026-10-19) — resolves the $200M input, but not the $200M
Rather than edit the figure, the author asked to check LCFF contributions first.

**Sources.** CDE 2024-25 Principal Apportionment funding rates
(https://www.cde.ca.gov/fg/aa/pa/pa2425rates.asp): adjusted base grant per ADA
is $11,068 (TK-3), $10,177 (4-6), $10,478 (7-8), $12,460 (9-12); supplemental =
20% x base x UPP; concentration = 65% x base x (UPP - 55%), i.e. only above a
**55% UPP threshold**. SFUSD 1st Interim 2024-25: LCFF $634.4M on funded ADA
45,682 = **$13,887 per ADA**; adopted-budget figure was $631.0M.
SFUSD UPP computed from `data/processed/calpads.csv`: **60.4%** in 2023
(enrollment 55,644, unduplicated 33,613) — above the threshold, so the
concentration grant is live. That file also corroborates the author's "55k in
SFUSD" (CDE `cdenroll2324-v2.txt`: 55,452 charter-inclusive, 48,736 non-charter).

**Why the district average is the wrong rate.** Students arriving from private
school are unlikely to be unduplicated pupils, so they bring only the base
grant; and because they raise ADA without raising the unduplicated count, they
**dilute the concentration grant for every existing student**. The marginal rate
while UPP stays above 55% is base x (1 - 0.65 x 0.55) = **$7,719 per ADA**, not
$13,887. At a large enough influx SFUSD drops below 55% UPP and loses the
concentration grant entirely.

**Counterfactuals** (entrants assumed not unduplicated; ADA = 0.937 x enrollment):
| Counterfactual | Added students | New UPP | LCFF change |
|---|---|---|---|
| 30% -> 15% (what main.tex:32 says) | 11,980 | 48.5% | **+$116M** |
| 30% -> 9% (true national average, NCES) | 16,780 | 44.9% | **+$170M** |
| 30% -> 0% (the author's 23.6k chain) | 23,960 | 40.5% | **+$250M** |

So the draft's $10k/student was a fair approximation of the marginal rate by
coincidence, and the stated 30%->15% counterfactual yields about **$116M**.
"Nearly $200 million" overstates it by roughly 70%. Reaching ~$200M requires the
9% national benchmark (~$170M) or near-full capture (~$250M).
Two caveats for the text: this is revenue, not net of the cost of educating the
additional students; and the 55%-threshold crossing is a real wrinkle worth a
footnote, since a large private-to-public shift would cost SFUSD the
concentration grant on its existing students.

## Citations resolved (2026-10-19)
- **"Thayer" = Thayer Morrill.** The paper is Aryal, G., Morrill, T. and Troyan, P.,
  "Desirable Rankings" (arXiv:2205.11684; SSRN 5554078, Oct 2025). Earlier version:
  Morrill & Troyan, "Desirable Rankings: A New Method for Ranking Outcomes of a
  Competitive Process," ACM EC '22, DOI 10.1145/3490486.3538272. It turns students'
  rank-order lists into a collective quality ranking via the IRUS algorithm
  (Iterated Removal of Underdemanded Schools) and proves that in large markets any
  desirable ranking coincides with the true quality ranking; applied to Chilean
  medical school admissions 2006-2009. Directly on point for estimating theta_k
  from rank data.
- **"the paper Thayer cites"** is most likely Avery, Glickman, Hoxby & Metrick,
  "A Revealed Preference Ranking of U.S. Colleges and Universities," QJE 2013 —
  NEEDS AUTHOR CONFIRMATION, not verified against the Desirable Rankings
  reference list.
- **71 of 300 largest districts (main.tex:30):** author states the Lagos paper is
  the source and will double-check. Not independently verified here: no PDF text
  extractor on this machine (no pdftotext/poppler/pypdf), and WebFetch returns the
  PDF as compressed binary. lit-scout's separate caveat stands — the paper counts
  "centralized-admission lotteries" (the SSRN version says "centralized school
  assignment systems"), which may not be the same set as "some form of lottery
  system" as the draft puts it.

## main.tex fix pass (2026-10-19) — applied, compiles clean (latexmk exit 0, no undefined refs)

**Abstract.** Rewritten. Misdating fixed (the promotion applied to the 2024-25 TK
cohort entering K in 2025-26, not "2023-2024"). The applicant claim is now the
rank-1 claim with the counting convention stated, and the observed first-choice
fall is given via `\numDidFirstTwoPct`. The risk-aversion sentence is gone. A
closing sentence reads the estimate as an upper bound and points to the
individual-data section. *(The upper-bound sentence is new editorial content --
flag if you want it out.)*

**Introduction.**
- `XX%` replaced with SFUSD's own fall 2026 survey: 82% chose a guaranteed spot
  near home, with a footnote giving n = 2,978, the field dates, the non-random
  sample, and the district's note that reweighting moves it by <=3 points.
- "some form of lottery system" narrowed to "a centralized-admission lottery",
  matching what the Lagos paper actually counts.
- The $200M sentence now reads 30% -> 9% (NCES national share) and "roughly $170
  million", with a footnote carrying the whole chain: enrollment, private count,
  ADA conversion, the base-grant-only marginal rate, the 60.4% UPP against the
  55% concentration threshold, the fact that an influx this large pushes SFUSD
  below it, and that this is revenue not net of cost. **Pick a different variant
  if you prefer (see the table above); this one is internally consistent.**
- The stray `<important to note...>` marker is replaced with prose on TK seat
  scarcity, and the "must do so because they dislike lottery participation"
  tautology (referee M6) is now conditional on TK families valuing their school
  as the city does, with a forward reference to where that is shown to fail.
- The roadmap paragraph: "this effect vanishes" corrected to "more than offsets";
  the Thayer footnote now cites `\cite{aryal2025}`; the risk-aversion consistency
  claim is replaced by the upper-bound reading and a pointer to
  §\ref{sec:individual-data}.

**Data.** The "top-5 ranks" description is replaced with the actual reporting
structure (ranks individually for the first 7-9 choices, deeper ranks top-coded,
scheme varying by cycle, 2021-22 carrying both), which also documents why list
length is variable and coarsely observed. The false "and 2026--27" promotion-count
claim is removed, resolving the contradiction with the roster paragraph.

**Figures.** The first-rankings caption, previously "Mean...", now describes both
panels and the hypothetical series. The "Now that we actually plot it" and "Now we
take this to a structural model" draft notes are now prose.

**f.** `f = 1` is now the maintained assumption, justified by the first-rankings
figure, with the DiD-implied values reported as lower and attributed to the
pre-trend. The requested distinction is spelled out: "would have ranked its own
school first" is not "prefers that school to the lottery" -- acceptance reveals
the second directly and implies the first only because top priority makes ranking
first and accepting equivalent, an equivalence that fails under length-capped
lists or uncertain priority.

**Capacity EU is now the headline.** "Bracketing the truth" is replaced: the
simulation is the preferred measure, the baseline is reported as an upper bound on
the lottery's value, and the baseline's bias is quantified from the existing
top-8 macros. The certainty-premium paragraph leads with `\numChatCap` and
relegates `\numChat`. §"A problem" leads with the capacity figure and adds the
signed-bias argument (match value and learning push c-hat up, only SFUSD exit
pushes down, hence an upper bound). The author's point that list length is highly
variable is recorded as the reason EU-over-n for a range of n is the right
robustness check rather than EU-over-5.

**Risk aversion moved to an appendix** (`\appendix` + §"Estimating risk aversion
directly"), label `sec:risk-aversion` preserved. Three substantive fixes inside:
the 0.3-0.7 comparison no longer claims the capacity figure is "close to" the
range (it is just above it) and no longer reads the agreement as support; the
Model A probit now documents `\numRiskDesignsBase`/`Cap` designs and the
`\numRiskDroppedCap` dominated families dropped; and the priority-spread claim is
qualified as requiring an exclusion restriction the paper does not have.

**Code/macro changes.** `make_numbers.py` gained `numChatCapSE` (so the headline
estimate quotes its own s.e. rather than the baseline's) and
`numRiskDesigns*`/`numRiskDropped*`. `\usepackage{url}` added to the preamble.

**Still open, needs an author:** the "(cite the simple descriptions paper)"
placeholder; whether "the paper Thayer cites" is Avery, Glickman, Hoxby & Metrick
(2013) (unverified, marked TODO in the bibliography); whether the Cabalin
roulette-wheel claim can be sourced (marked TODO); the empty §Literature; and
§"Other concerns" still being a bullet list.

## Round 1 — copy-editor (complete)
Report: the agent verified the `\num...` macro multiset is identical before and
after its pass, and that math, tables and citation keys were untouched;
`latexmk -pdf` compiles at 19 pages.

Fixed: "kidnergarten", "it is necessary understand", "acheive"; a dangling
modifier in the abstract ("Counting promoted families as having ranked..."); and
`\numOneUnitRank th`, which would have rendered as "16 th". Standardized
"first rankings"/"rank-1 requests" to "first-choice requests", "TK-linked" to
"promotion-eligible", and "FOIA data" to "individual-level data". Renamed the
§"A problem" heading to "The slope restriction". Added the missing figure
pointer in the f paragraph.

### Acted on from its flag list (2026-10-19)
- **The f paragraph read as if the paper both maintained and estimated f** — an
  inconsistency introduced by the round-1 fix pass. The duplicated DiD-implied f
  clause is removed, and the column-(4) sentence no longer says "in line with
  f ~ 0.6-0.75"; it now reports the coefficient as of the same order as the
  implied f and short of the 1 that f = 1 predicts.
- **Hand-typed q range** "0.5--0.8" replaced with `\numImpliedQMin`/`\numImpliedQMax`
  (0.47 to 0.80), new macros from `application_effects_model.csv`.
- **"the district has a priority order over schools"** corrected to "each school
  has a priority order over students" — in deferred acceptance the priorities run
  the other way.
- **Hand-typed "of 71"** replaced with `\numSchools`.
- **The upper-bound claim** sat next to a channel biasing c-hat down. Now stated
  as a balance, naming the SFUSD-exit channel as the one offset and noting it
  could be bounded with October enrollment for the `\numNonAccepting` (219)
  non-accepting families. New macro.

### Still flagged, NOT acted on — author decisions
- **Notation collisions**, deliberately left alone because renaming would
  desynchronize the text from the code and macros: delta_k/xi_t vs theta_k/theta_0t;
  **r** means the promotion rate, the CARA coefficient, and appears in the
  robustness header "Corr. (r_k, theta_k)"; **beta** is the slope in "beta = 1",
  the characteristic coefficients beta_k, and the DiD coefficients beta_25/beta_26;
  **gamma** is both a characteristics coefficient and the DiD year effects;
  **P_k** is an admission probability while P_kt/P_t are counts of promoted
  families; **rho** is both the collinearity and CRRA.
- "should be $\Lambda'$ more likely to accept" is imprecise.
- ":225's "the dollar-denominated answer the introduction wants" has no
  counterpart in the introduction, where the dollar discussion is commented out.
- The §"Schools versus programs" parenthetical about an earlier pipeline version
  reads as a changelog rather than paper text.
- Remaining hand-typed: "the 40 buildings" (appendix), "73% take-up" in the
  risk-aversion figure caption, and the word-quantities "a quarter"/"half"/"a
  tenth", "5 schools".
- Appendix citations are plain author-year text; the bibitems exist but are never
  `\cite`d, so they do not render as numbered cites.

## Round 2 — started 2026-10-19
| Agent | Status |
|---|---|
| numbers-auditor | done ([round2-numbers-auditor.md](round2-numbers-auditor.md)) |
| replicator | done — **reproduces**, all three fixes verified ([round2-replicator.md](round2-replicator.md)) |
| paper-referee | not started — pending author decisions below |
| lit-scout | not started |
| copy-editor | not started (runs last) |

Blocking a useful round-2 referee pass: §Literature is still empty, §Other
concerns is still a bullet list, and the "(cite the simple descriptions paper)"
placeholder is unresolved. A fresh referee would mostly re-flag these.

### Round 2 replicator result
All three code changes verified from a genuinely clean state:
- A single `python3 run_distaste_analysis.py` from an **empty** `output/` and
  `figures/` now exits 0 in ~78 s (including the ~50 s validator, which runs
  automatically), producing all 21 output files and all 6 PNGs with no manual
  step. That was the round-1 failure case.
- Staleness logic confirmed both ways: a second run prints "up to date;
  skipping re-validation" and takes 22 s; after `touch src/risk_aversion.py` the
  validator re-runs (76 s) and outputs are again identical.
- `lottery_distaste_estimates.csv`: 40 rows, 0 duplicate school_ids.
- `numbers.tex`: 133 macros, each of the 8 new ones defined exactly once.
- Everything byte-identical across three runs; `latexmk -pdf` from only the
  regenerated outputs exits 0 with no undefined references. Two Overfull hbox
  warnings remain (near :102 and :235-236) — copy-editor material.

### Reproducibility gap confirmed (2026-10-19)
The replicator could not tell whether the pipeline's inputs are in git. They
largely are not, and none are gitignored:

| Input | Size | State |
|---|---|---|
| `data/lottery/merged/sfusd-lottery-all.csv` | 2.6M | tracked |
| `data/lottery/merged/sfusd-school-id-mapping.csv` | 4K | tracked |
| `data/processed/schools.csv` | 12K | **untracked** |
| `data/processed/calpads.csv` | 264K | **untracked** |
| `data/enrollment/raw/cdenroll2324-v2.txt` | 30M | **untracked** |
| `data/enrollment/raw/cdenroll2425.txt` | 31M | **untracked** |
| `data/sbac/raw/sb_ca2024_all_csv_v1.zip` | 150M | **untracked** |

So a clean clone cannot run the pipeline. Note the SBAC zip exceeds GitHub's
100 MB per-file limit, so it can never be committed directly.
**Cheapest fix, in order of leverage:**
1. Commit `output/sbac_2024_sfusd_schools.csv` (4K, currently untracked).
   `src/promotion_rate.py` skips the 150 MB zip whenever that cache exists, so
   committing it removes the largest dependency outright.
2. Commit `data/processed/schools.csv` and `calpads.csv` (276K together).
3. Add a fetch script for the two CDE enrollment files, which are public
   downloads, rather than committing 61 MB.
Still open from round 1: packages are unpinned (no requirements/pyproject/lock),
and `src/robustness.py:35` runs the capacity simulation with `n_sims=1`.

### Round 2 numbers audit — fixed (2026-10-19)
The audit caught six errors introduced by the round-1 fix pass, plus two
long-standing ones. All eight are now fixed; `latexmk` exit 0, 19 pages, no
undefined references, 136 macros, and `main.pdf` is newer than the outputs.

1. **Dominance direction was reversed** in the appendix. `indifference_point`
   in `src/risk_aversion.py` returns `+inf` when `s <= min(L)`, which its own
   docstring calls "the lottery weakly dominates". The dropped school is Malcolm
   X Academy (own theta -4.69, at or below the capacity lottery's worst outcome,
   `r_star = inf`, 11 families). The text said no $r_i$ makes the lottery the
   better choice -- the opposite. Now states that the lottery weakly dominates
   for every $r_i$, so the design is uninformative about $m$ and $\sigma$.
2. **"first seven to nine choices"** -> **"five to nine"**. The 2019-20 and
   2020-21 cycles report only ranks 1--5 individually (then 6--10 pooled).
3. **Wrong cross-reference**: the rank-coarseness point pointed at
   `sec:individual-data` ("What survives without choice data") instead of §Data.
   Added `\label{sec:data}` and repointed it.
4. **"short of the 1 that f = 1 predicts"** was one-sided: the first-choice
   intensity CI is 0.03 to 1.23 and includes 1. Now says so, and notes the
   all-ranks coefficient's CI does too.
5. **"just above"** for the capacity Holt-Laury equivalent (0.91 against a
   0.3--0.7 range) understated a 0.21 gap. Reworded.
6. **The break-even f is now stated.** New macro `\numFBreakeven` (0.80), derived
   in `make_numbers.py` from `first_rankings_by_year.csv`. The f paragraph now
   says plainly that adding promoted families back reverses the first-choice
   comparison only if f exceeds 0.80, that every DiD-implied f is below that, and
   that the reversal therefore rests on the maintained f = 1 rather than the
   estimated f. Also added `\numEligiblePlusPromoted` (2,007).
7. **"the observed 5.0 requests per applicant"** was wrong: `src/simulation.py`
   calibrates to `requests.sum()/n_families` = **4.84**. Rather than retype it,
   `simulation.py` now returns `target_list_length`, `run_distaste_analysis.py`
   writes `simulation_target_list_length` into `lottery_distaste_summary.csv`,
   and `make_numbers.py` exposes `\numReqPerApplicant`. Both the simulation
   paragraph and the "average applicant applies to 5 schools" claim now use it.
8. **Hand-typed "40 buildings"** -> `\numEligibleBuildings`.

### Round 2 — still open, needs an author
- **The $170M footnote does not reproduce from its own stated chain.** Every
  input checks out (30.2% private share, ~16,800 students, ~15,700 ADA, 60.4%
  UPP, the 55% threshold, post-influx UPP of 46.4%, and $13,887 x ADA = $218M),
  but the footnote omits the 0.937 ADA conversion and never states which base
  grant it uses, so a reader cannot land on $170M: the auditor gets ~$154M from
  the chain as written, $174M at the TK-3 base before dilution, and $121M at the
  README's marginal rate. It also still mixes charter-inclusive enrollment
  (55,452) with LCFF, which flows only to non-charter SFUSD (48,736). This should
  become a script-backed calculation with stated assumptions rather than prose
  arithmetic. **Needs a decision on the base rate and the charter question.**
- `|rho| ~ 0.7-0.8` (:242) is wrong: pairwise |rho| runs 0.68 to **0.97**
  (ELA-math is 0.97).
- "within a student or two at almost every school" (:240): only 29 of 40 are
  within 2; eleven differ by 3 to 6.
- School-mover magnitudes (:283) remain untraced, and selective.
- The abstract's "fell by 27%" is the DiD effect relative to other schools; the
  raw fall at eligible schools is 24% and other schools rose 1.9%. The macro is
  right; the wording invites misreading.
- `sec:risk-aversion` is never referenced from the body and nothing points to the
  new appendix. Add a pointer.
- "2023-24 school year" (:35) vs the 2024-25 promoted cohort (:27) should be
  reconciled in wording.
- CLAUDE.md still says the two EU versions "bracket the truth", which the
  revision has superseded.
- The risk-aversion figure annotation hardcodes "73%" in
  `run_distaste_analysis.py:228`.
- Defined but unused macros: numUndersubscribed, numNegFOVbaseSchools,
  numPromotedShareFirst, numPromotedShareAll, numRiskDroppedBase.

## Macro refactor (2026-10-19) — author-approved, applied
Pipeline exit 0, `latexmk` exit 0, 19 pages, no undefined references, **161 macros**.

### The intro revenue figure is now a script, not prose arithmetic
New `lcff_counterfactual.py` → `output/lcff_counterfactual.csv` → macros. It
derives the base-grant blend from SFUSD's own grade distribution in the CDE file
the pipeline already reads, takes UPP from `data/processed/calpads.csv`, and
applies the full LCFF formula (base + 20% supplemental + 65% concentration above
a 55% UPP threshold).

**Result: `\numLcffGain` = $136M**, replacing the unreproducible $170M.
Chain, all macro-ized: SFUSD non-charter enrollment 48,736; SF private students
23,765 at a 30% share; national share 9%; students returning to the district
14,621 after splitting the city-wide gain between district and charters in
current proportions; 13,705 ADA; UPP 60.4% → 46.5%, crossing below the
threshold so the concentration grant is lost on existing students too. Against
$190M if one simply multiplied by SFUSD's current $13,887 per ADA.
**Built-in validity check:** the same formula reproduces SFUSD's reported LCFF
per ADA to within `\numLcffModelError` (7%), the residual being LCFF add-ons
(TIBG, transportation, TUPE) outside base/supplemental/concentration.
**Two choices left as flippable constants** at the top of the script, since the
author did not specify: `BASE_GRANT_MODE` ('blended' default vs 'tk3') and
`INCLUDE_CHARTERS` (False default, since charter LCFF does not reach the
district). 'tk3' and charter-inclusive variants both move the headline.

### CLAUDE.md's hand-typed list is now empty
- `\numApplicantsPrev` (3,792); `\numRequestsPrev`/`\numRequestsNow`. These come
  from the balanced 70-school panel, so the figure is **21,951, not 21,955** —
  the old number included Mission Education Center, which the caption said was
  excluded. That inconsistency is resolved by construction.
- `\numReqPerApplicant` (4.84). `src/simulation.py` now returns
  `target_list_length`, the driver writes `simulation_target_list_length` to the
  summary CSV, and the macro reads it, so the simulation's calibration target
  can no longer drift from the text. The text had said 5.0, which was wrong.
- `\numRhoMin`/`\numRhoMax` (0.68–0.97), computed over all pairwise correlations
  among theta, SBAC ELA, SBAC math and unduplicated share. The old "0.7–0.8"
  omitted the 0.97 ELA–math pair.
- `\numCdeMatchWithinTwo` (29) and `\numCdeMatchMaxDiff` (6) replace "within a
  student or two at almost every school", which held for only 29 of 40.
- Movers: `output/school_quality_full_panel.csv` is now exported (the full-panel
  per-school theta was computed inside the robustness loop and thrown away,
  which is why this claim was untraceable). Macros `\numMoverUpSchool`/`Max`
  (Muir, 0.38) and `\numMoverDownSchool`/`Max` (Grattan, 0.28) match the
  auditor's independent recomputation, and `\numMoversAboveTwoTenths` (10 of 71)
  replaces the selective three-up/two-down naming the audit objected to.
- The f range was already macro-ized, plus `\numFBreakeven`.

The one surviving instance of 21,955/18,681 is inside a commented-out paragraph,
so the live text is clear.

### CLAUDE.md updated to match the session's decisions
Lottery-value section now states EU^cap as preferred with the baseline as an
upper bound; a 2026-10-19 author-directions block records f = 1, the appendix
move, and the LCFF script with its two flippable constants; the Build section
documents the single self-sufficient command, the ~78 s cost, and the untracked-
inputs gap; the rules-for-numbers section records the cleared list and what
remains hand-typed ("73%" inside the risk-aversion figure at
`run_distaste_analysis.py:228`, the word-quantities, "15 standalone TK sites").

### Still open for the author
- The f discussion ("ranked first" vs "prefers to the lottery") is flagged for a
  fuller treatment later, per the author.
- `BASE_GRANT_MODE` and `INCLUDE_CHARTERS` defaults to confirm.
- Abstract's "fell by 27%" is the DiD effect, not the raw fall (24%).
- Nothing in the body points to the new appendix; `sec:risk-aversion` is unreferenced.
- "2023–24 school year" (:35) vs the 2024–25 promoted cohort (:27) wording.
- §Literature empty; §Other concerns a bullet list; the "simple descriptions
  paper" placeholder; the Avery and Cabalin confirmations.
- Notation collisions (beta, gamma, r, rho, P) — a coordinated code+text rename.
