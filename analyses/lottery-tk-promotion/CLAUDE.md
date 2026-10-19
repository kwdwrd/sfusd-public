# Paper: How Much Do Families Dislike School Lotteries?

Hadad & Woodward. Draft in `main.tex`. Target journal: TODO.

## Research question and design

- **Question:** How large is families' distaste for participating in a school assignment lottery?
- **Natural experiment:** SFUSD's 2025–26 TK→K automatic promotion. Families whose child was in TK at school k could take a guaranteed K seat at k, or decline it and enter the K lottery. 601 of 820 eligible families accepted.
- **Estimand:** c is the utility a family gives up to avoid the lottery (a certainty premium). It is measured in θ units, which the unit-scale logit shocks normalize. There is no dollar conversion yet. That needs a distance coefficient from individual data.
- **Quality:** θ_k is a school fixed effect from OLS of the log-odds of being ranked, ln A_kt − ln(N_t − A_kt), on school and year FEs. Main window: 2021–22 through 2024–25.
- **Lottery value:** the **capacity-constrained EU^cap** (a random-serial-dictatorship simulation with seat limits) is the preferred measure and the source of the headline numbers (author direction, 2026-10-19). The baseline EU = Σ θ_k P_k Π(1−P_j), with independent admission draws, is reported alongside it as an *upper bound on the lottery's value*: it ignores aggregate capacity, placing a family in a top-8 school with probability 0.52 when at most ~16% of families can be, so it overstates the lottery and therefore overstates distaste. Neither values the lottery over the list a family actually submits; EU over the top *n* ranks for a range of *n* is the open robustness check (list lengths are variable and top-coded, so a single *n* won't do).
- **Promotion model:** Pr(accept | k) = Λ(θ_k − EU + c).

## Known weaknesses (don't hide them, don't overclaim)

- **The data are aggregate only.** Everything is school-by-year counts. Individual data (rank lists, home location, priorities) has been requested by public records request but not received. Selection into TK on match value can't be separated from lottery distaste.
- **The slope test fails.** The free slope on θ_k − EU is ≈ 0 (s.e. 0.09), so the model's β = 1 restriction is rejected. Read ĉ as "the average premium implied by 73% take-up", not as a structural estimate validated by cross-school variation.
- **c is not separately identified from risk aversion,** because both fit one moment. The CARA r̂ is driven by the lowest few schools in the tail (see §risk aversion).
- **Some non-accepting families left SFUSD rather than entering the lottery.** This biases ĉ down.
- **Capacity is measured with CDE occupied seats,** which understate true capacity.

## Author directions (2026-10-19)

- **f = 1 is the maintained accounting assumption.** Accepting a guaranteed seat is taken to reveal the family would have ranked that school first. The DiD-implied f (0.30–0.77) is reported as lower and attributed to the pre-trend. The abstract's first-choice claim reverses sign only above f = `\numFBreakeven` (0.80), which the text now states plainly. The author is content with this; a fuller discussion of "would have ranked first" vs "prefers it to the lottery" is still to be written.
- **EU^cap is the headline**; see above.
- **Risk aversion lives in an appendix** (`\appendix`, §"Estimating risk aversion directly"), not the main text, and is out of the abstract.
- **The $200M/$170M intro figure is now a script**, `lcff_counterfactual.py` → `output/lcff_counterfactual.csv` → macros. It yields `\numLcffGain` ($136M). Two modelling choices are exposed as constants at the top of that script and are the authors' to flip: `BASE_GRANT_MODE` ('blended' by default, weighting CDE grade-span rates by SFUSD's own grade distribution; 'tk3' uses the TK-3 rate alone) and `INCLUDE_CHARTERS` (False by default, since LCFF for charter students does not flow to the district). The script self-checks by reproducing SFUSD's reported LCFF per ADA to within `\numLcffModelError`.

## Author directions (2026-10-18)

- **One model of lottery distaste.** The main text uses the certainty-premium model, Pr(accept | k) = Λ(θ_k − EU + c). The CARA risk-aversion analysis moves to an appendix as an alternative reading. It is not a competing headline.
- **Summarize the reduced-form evidence** clearly and early: the DiD and event study on first-choice and all-rank requests, and take-up versus quality. Do this before the structural part.
- **Make the case for individual-level choice data with covariates and demographics.** Explain concretely what it would identify: selection on match value via distance, attendance area and siblings; heterogeneity by demographics; individual P_ik; risk aversion versus fixed distaste (via the spread of lotteries faced); and a dollar conversion via distance.
- **Discuss other reasons families might skip the lottery,** such as learning to love their TK school, an endowment effect, or switching costs. Be explicit that these (1) are not identifiable from the current data, and (2) if true, mean the school system is good: families who try a school like it.
- **Source figures on parent complaints about the lottery** wherever possible, for example the intro's "XX%" survey placeholder. Use surveys, news coverage and district reports. Only verified sources; anything that can't be sourced stays marked TODO.

## Notation (keep consistent)

| Symbol | Meaning |
|---|---|
| θ_k (≡ δ_k) | school quality |
| θ_0t (≡ ξ_t) | outside option / year effect |
| P_k | admission probability |
| EU, EU^cap | lottery value (baseline, capacity-constrained) |
| c, ĉ | certainty premium (lottery distaste) |
| r_k | promotion rate |
| q | probability a promoted family would have ranked its own school |
| f | the same, for ranking it first |
| r (CARA) | risk aversion per θ unit |

- "Forgone option value" = EU − θ_k.
- "Promotion-eligible" = a building with a TK program on the roster (40 buildings; DeAvila and Huerta merged).

## Rules for numbers

- **Never type a result into `main.tex` by hand.** Every number that comes from the data is a `\num...` macro defined in `output/numbers.tex`. Table bodies are `\inputtable{...}` from `output/tables/`. Both are generated by `make_numbers.py` from `output/*.csv`.
- **To add a number:** add it to `make_numbers.py` with a source comment, rerun the script, then use the macro.
- **Exempt numbers:** external facts (district counts, private-school rates, budget figures), sample-design constants (15 cycles, window years), and illustrative assumptions (W = $1M, D = $50k). Each external fact still needs a citation or a footnote saying where it comes from.
- **The hand-typed list is cleared (2026-10-19).** All six became macros:
  `\numApplicantsPrev`; `\numRequestsPrev`/`\numRequestsNow` (note these are the
  balanced 70-school panel, so 21,951 not 21,955 — the old figure included
  Mission Education Center, which the caption said it excluded);
  `\numReqPerApplicant` (4.84, exported from `src/simulation.py`'s own
  calibration target — the text had said 5.0, which was wrong);
  `\numMoverUpSchool`/`\numMoverUpMax`/`\numMoverDownSchool`/`\numMoverDownMax`
  plus `\numMoversAboveTwoTenths` (the full-panel per-school θ is now exported to
  `output/school_quality_full_panel.csv` so this claim can trace at all); the
  f range is gone, replaced by `\numImpliedFTwo`/`\numImpliedFTrend` and
  `\numFBreakeven`; and `\numRhoMin`/`\numRhoMax` (0.68–0.97 — the old
  "0.7–0.8" omitted the 0.97 ELA–math pair).
- **Still hand-typed:** the "73%" annotation inside the risk-aversion figure is
  hardcoded at `run_distaste_analysis.py:228`; the word-quantities "a quarter",
  "half" and "a tenth" in §application-effects; and "15 standalone TK sites".

## Build

```
python3 run_distaste_analysis.py   # full pipeline: estimates, CSVs, figures, numbers.tex
python3 make_numbers.py            # regenerate macros/tables from existing CSVs only
latexmk -pdf main.tex
```

`run_distaste_analysis.py` is self-sufficient from a clean checkout (~78 s): it
runs `validate_risk_aversion.py` itself when `output/risk_aversion_validation.csv`
is missing or older than the code it checks (~50 s, skipped otherwise), then
`plot_first_rankings.py` and `lcff_counterfactual.py`, then `make_numbers.py`.
Those three can still be run standalone. **Known gap:** five of the seven data
inputs are untracked in git (`data/processed/*`, `data/enrollment/raw/*`, the
150 MB `data/sbac/raw/*.zip`), so a clean *clone* cannot run the pipeline;
committing the 4 KB `output/sbac_2024_sfusd_schools.csv` cache would remove the
largest dependency, since `src/promotion_rate.py` skips the zip when it exists.
Python packages are unpinned (no requirements/pyproject/lockfile).

`output/` and `figures/` are generated. Don't edit them by hand.

## Open TODOs in the draft

- Intro: "XX%" survey placeholder; "(cite the simple descriptions paper)"; Thayer cite.
- `\cite` keys `lagosfranciscoEffect...` and `cabalinEducacion...` are undefined.
- The bibliography was compiled from memory and needs verifying.
- The Literature section is empty. "Other concerns" is a bullet list.
- A note in the text ("Now that we actually plot it…") and the transitional sentence at the end of §application effects need proper prose.

## Agent workflow

The agents are in the repo's `.claude/agents/`. Typical order after a substantive revision:

1. `numbers-auditor` and `replicator`, in parallel.
2. `paper-referee`, using a new instance each round.
3. Triage the referee report with the authors.
4. `lit-scout` for citations.
5. `copy-editor` last.

Save each agent's report to `reviews/round<N>-<agent>.md`, and keep the current status in `reviews/README.md`. Agents report findings. Except for the copy editor, they don't edit `main.tex`. Never tune specifications in response to results.
