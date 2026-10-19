# Round 1 — numbers-auditor (2026-10-18)

Line numbers refer to `main.tex` as of 2026-10-18.

## Macro health
- `make_numbers.py` ran cleanly, wrote 125 macros. `output/numbers.tex` and
  `output/tables/*` byte-identical to pre-run copies: main.tex was NOT compiled
  against stale macros. All 125 macros agree with their source CSVs.
- Every `\num...` macro used in main.tex is defined.
- Defined but never used: `numNegFOVbaseSchools`, `numOneUnitSchool`,
  `numPromotedShareAll`, `numPromotedShareFirst`, `numUndersubscribed`.
  The text hard-codes the matching facts instead.
- Staleness: `run_distaste_analysis.py` and `make_numbers.py` (Oct 18) are newer
  than every output/figure (Oct 8-9), with large git diffs. `src/*.py` are older
  than the outputs, so the estimators are not stale. (The replicator
  subsequently confirmed the outputs reproduce exactly from current code.)

## Mismatches, most serious first

| # | Line | Claim | Status | Note |
|---|---|---|---|---|
| 1 | 26 (abstract) | eligible schools saw "relatively **more** applicants" | **WRONG SIGN** | All first-choice DiD estimates negative: -0.31 (2x2), -0.18 (pooled), -0.38 (trend). Line 38 correctly says "reduced". |
| 2 | 26, 38 | c-hat "roughly equivalent to the quality difference between the best and median schools" | **MISMATCH for baseline** | Best-minus-median gap = 1.70 (best -0.81, median -2.51, rank 36/71). Baseline c-hat 2.41 = gap to Bryant (#55/71). Capacity c-hat 1.60 = Lakeshore (#32), close to the median gap. Text never says which EU is meant. |
| 3 | 26 | "broadly consistent with the experimental findings of risk aversion" | **OVERCLAIM** | Lines 306-311 report CRRA rho of 100 and 125 at $50k/theta-unit and call these implausible, Sydnor-like, "better described by overweighting of small probabilities". Only the unit-free Holt-Laury-equivalent rho = 0.57 is lab-like; capacity gives 0.91, outside the 0.3-0.7 range quoted at line 310. |
| 4 | 64 vs 177 | data "report the number of families who accepted promotion ... in 2025-26 **and 2026-27**" | **INTERNAL CONTRADICTION** | Line 177 says the 2026-27 counts "are not in our roster". The roster is used only for 2025-26. |
| 5 | 64 | "top-5 ranks" | **MISMATCH** | Requests per applicant run 5.8 (24-25) to 11.4 (18-19); rank lists are longer than 5. |
| 6 | 134, 227 | "applies to 5 schools"; "calibrated to match the observed 5.0 requests per applicant" | **MISMATCH** | Calibration target in `simulation.py` is 19,282/3,981 = **4.84**. Observed is 5.53 (2025-26) and 5.79 (2024-25). Neither is 5.0. Hand-typed. |
| 7 | 160, 169, `application_summary.tex` | promoted families = "half" (0.50) of first choices, "a tenth" (0.11) of requests | **MISLEADING** | Those are means of school-level ratios. Ratio of means is 0.32 and 0.058 (15.0 promoted vs 46.3 first choices vs 259.4 requests per school). Label as "school-level mean ratio". |
| 8 | 238 | roster eligibility matches CDE TK "to within a student or two at almost every school" | **MISMATCH** | Within 1 at 27/40, within 2 at 29/40. Eleven schools differ by 3-6: Chavez, Bryant, Parks, Sanchez, Harte, Lakeshore, El Dorado, Drew, Malcolm X, Tenderloin, Stevenson. |
| 9 | 128 | "fixing this moved no reported estimate by more than 0.01" | **UNTRACED** | Refers to a deleted earlier run. |
| 10 | 128 vs code | "we sum their roster rows to the building" | **CODE BUG** | `data.py` merges to building, but `compute_distaste` uses the raw roster. `lottery_distaste_estimates.csv` has **42 rows**, DeAvila (509) and Huerta (537) each duplicated; `lottery_distaste_summary.csv` n_schools = 42. The negative-capacity-school count is 13 from `promotion_rates.csv` (40 rows) but 14 from the distaste file. The quality figure's right panel plots 42 points. Family-weighted means unaffected (1.2449, 0.4309 either way). |
| 11 | 199 | pre-period rise "(left panel: flat at eligible schools while falling elsewhere)" | wording | Event-study coefficients -0.355, -0.267, -0.160, -0.125, 0 are monotone rising; the pre-trend shows in the event-study panel too. Cite both panels. |
| 12 | 201 | all-ranks coefficient 1.87 is "imprecise" | note | p = 0.029, starred ** in the table. The CI-includes-one statement is correct (0.18 to 3.55). |
| 13 | 199 | "f between roughly 0.6 and 0.8" | **UNTRACED (hand-typed)** | Implied f = 0.60, 0.30 (pooled), 0.77 (trend); PPML intensity 0.74. The pooled 0.30 is excluded without comment. |
| 16 | 240 | collinearity "abs(rho) ~ 0.7-0.8" | hand-typed, incomplete | corr(theta, sbac_math) = 0.78; corr(theta, unduplicated) = -0.80; corr(sbac_math, unduplicated) = -0.68; corr(sbac_ela, sbac_math) = **0.97**. The range omits 0.97 and -0.68. |
| 17 | 281 | "2019-20 stands out as the noisiest cycle" (rho = 0.96) | **MISLEADING** | Noisier than its neighbors (0.98-0.99), but 2012-13 to 2017-18 are lower (0.90-0.97). It is also the only cycle with 65 schools. Say "relative to its neighbors". |
| 18 | 281 | movers "roughly 0.3-0.4 up; 0.25 down" | traced but **selective** | McCoppin +0.38, Muir +0.38, Cobb +0.31, Grattan -0.28, Spring Valley -0.24. Clarendon, McKinley and Rooftop also fall ~0.22 and go unmentioned. |
| 25 | 332 | coverage "0.94 and 0.94" | rounding | Actually 0.935 and 0.945. Monte Carlo design hand-typed. |
| 26 | 304 | designs and families | **UNDOCUMENTED** | Capacity Model A drops 11 dominated families and uses 39 designs; the text implies 820 families and 40 designs throughout. |
| 29 | 36 | stray draft marker `<important to note that there are not enough TK seats>` | **FIX** | |
| 19 | 32 | 30%->15% private rate; "nearly $200 million" | exempt but unsupported | Footnote says "own calculations" with no code or output behind it. |
| 20 | 30 | "71 of the 300 largest districts"; "XX%" | exempt / TODO | Cite key undefined. |

## Traced and consistent (spot list)
601 of 820 and 73%; 3,981 = 3,380 + 601; 3,642 seats; the 77/14/9 shares;
EU gap 0.81 = 2.41 - 1.60; c-hat 2.41 (s.e. 0.08) and 1.60; FOV 1.24 and 0.43;
CE -3.89 = EU - c-hat in both versions; ranks #24, #4, #55, #32, #16; 7 and 30
schools above EU; P(top 8) 0.52, P(top 15) 0.78; 629 seats in the top 8 (~16%);
admission probabilities 5.5%, 7.5%, 17%; only MEC has P = 1; all DiD stars and
p-values, with thresholds 0.01/0.05/0.10 as the table note says; implied q
0.52/0.47/0.80 and f 0.60/0.30/0.77; the 2x2 -0.31 as 27%; first-choice fall of
24% and other-school rise of 2%; robustness ranges over all 7 rows; rank
correlations 0.95-0.99, 0.86, 0.99, 0.98; promotion-rate correlations, including
"none distinguishable from zero" (p = 0.15); r-hat 5.0 (0.14) and 6.3 (0.82);
tail-drop 21 and 8.0, and 100.02 and 106.5; probit slopes -0.004 (0.008) and
-0.002 (0.005); sigma bounds 79 and 115 (~16-18x r-hat); 15 of the bottom 17
rank slots are eligible schools; "about 50,000 vs about 20,000 requests" (ratio
>= 2.06); 3,792 -> 3,380 applicants; 18,681 requests; 86 vs 71 schools.
Note: 21,955 includes the 4 Mission Education Center requests, so the table row
means sum to 21,951 (MEC excluded).

## On the author directions
**Single distaste model.** The text carries two EU versions throughout
(-1.48, -2.29), hence two c-hat, two FOV, two r-hat. Sections needing a headline
decision: abstract, line 38, 219-231, 253, 261, 302. Specification-ambiguous
numbers: "best vs median" (row 2), the CRRA 100/125 pair, "premium share 0.42 vs
0.67". The text calls the two "bracketing the truth" yet the abstract and intro
quote one equivalence with no label. Dead or uncited in code/CSVs: the Fechner
and contextual Model B fits, `risk_aversion_model_a_designs.csv`, and the
"2.75 / 1.94 mean family-weighted c-hat" summary entries.

**Risk-aversion appendix.** All risk numbers trace to `src/risk_aversion.py`,
its CSVs and `validate_risk_aversion.py`. Not macro-ized: the "73%" in the
figure caption, the "100" threshold, the validation design. Row 26 undocumented.
The 8.0 and 106.5 tail entries exist in the CSV but the table shows only
"100 / 107".

**Reduced form.** Everything traces except f in 0.6-0.8, and 3,792 / 21,955 /
18,681, which are hand-typed but traceable. The event-study magnitudes
(e.g. -0.355 in 2020-21) are never quoted; if the pre-trend claim stays, give them.

## Recommended new macros
`numApplicants2425` (3,792), `numRequests2425` (21,955), `numRequests2526`
(18,681), `numReqPerApplicantTarget` (4.84), `numReqPerApplicantObs`;
`numImpliedFLow`/`numImpliedFHigh` (0.60/0.77), `numPromotedPerSchool` (15.0),
`numPromotedShareFirstMeanOfRatios`; `numCollinMin`/`numCollinMax`,
`numNegFOVbaseSchool1`/`2` with ranks, `numBestMedianGap` (1.70),
`numMedianSchool`/`numMedianRank`, `numRateMinSchools`, `numRateMaxSchool`;
`numMoverUp*`/`numMoverDown*` from `school_quality_by_year.csv`;
`numRiskDesignsCap` (39), `numRiskDroppedCap` (11), `numRDropThreeBase`/`Cap`
(100.0/106.5), `numValDesign`, `numCdeRosterWithin2` (29 of 40),
`numCdeRosterMaxDiff` (6). Plus `\numTakeup` in both hard-coded figure strings.

## Source cleanup
Make `compute_distaste` use the building-level roster so
`lottery_distaste_estimates.csv` has 40 rows (see row 10).
