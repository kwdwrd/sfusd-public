# Round 2 - replicator (2026-10-19)

**Verdict: reproduces.** From an empty output/ and figures/, one command
(`python3 run_distaste_analysis.py`, repo .venv Python 3.11.14) exits 0 and every
output is byte-identical to the working tree.

## Setup
Scratch tree with code, main.tex and the roster CSV copied in, `output/` and `figures/` created empty,
`data` symlinked to the repo `data/`.

## 1. Clean-state run (the round-1 failure case)
- Single invocation, exit 0, **78 s** wall (validator ~50 s of that, run automatically).
- Produced all 21 files in output/ (incl. risk_aversion_validation.csv, first_rankings_by_year.csv, the SBAC cache, tables/), and all 6 PNGs.
- Compared with the working tree: 0 differences (cmp on all CSVs, numbers.tex, 4 tables, 6 PNGs). No file on one side only.

## 2. Changes verified
1. compute_distaste: lottery_distaste_estimates.csv has 40 rows, 0 duplicate school_ids.
2. Build order: fixed (see 1). Staleness logic:
   - second run: prints "risk_aversion_validation.csv is up to date; skipping re-validation", 22 s, outputs and figures byte-identical to the first run;
   - after `touch src/risk_aversion.py`: validator re-ran, 76 s, outputs and figures again byte-identical.
3. numbers.tex defines **133** macros; numChatCapSE, numRiskDesignsBase/Cap, numRiskDroppedBase/Cap, numImpliedQMin/Max, numNonAccepting each defined exactly once.

## 3. Differences / orphans / nondeterminism
- Differences: none (max abs diff 0).
- Orphans: none new. output/ has 21 files on both sides.
- Nondeterminism: three full runs gave identical bytes (including PNGs and the validator CSV). Seeds as in round 1: simulation.py seed=0, n_sims=20; robustness.py:35 still uses n_sims=1 with the default seed (reproducible but a weak setting). default_rng streams may vary across numpy versions.

## 4. LaTeX
`latexmk -pdf main.tex` in the scratch tree, from regenerated outputs only: exit 0, main.pdf produced, no undefined references/citations in main.log (only two Overfull hbox warnings, lines ~102 and 235-236).

## 5. Dependencies (unchanged from round 1 except as noted)
- No absolute paths; relative `../../data/...`, cwd must be the paper dir.
- Reads data/lottery/merged/sfusd-lottery-all.csv, sfusd-school-id-mapping.csv, data/processed/schools.csv, calpads.csv, data/enrollment/raw/cdenroll2324-v2.txt, cdenroll2425.txt, data/sbac/raw/sb_ca2024_all_csv_v1.zip, and ./Promotes from TK to K - Sheet1.csv.
- `git ls-files` shows only the two lottery/merged files tracked; the processed/, enrollment/raw and sbac zip inputs are untracked (may be gitignored or raw-data-only), so a clean clone may not run.
- output/sbac_2024_sfusd_schools.csv is a cache (regenerated from the zip here, matched).
- The pipeline now shells out to validate_risk_aversion.py via sys.executable and keys re-validation on file mtimes: a fresh git checkout gives arbitrary mtimes, but a missing CSV always triggers validation, so this is safe.
- Packages unpinned (no requirements/pyproject); tested pandas 2.3.3, numpy 2.3.5, scipy 1.16.3, matplotlib 3.10.8, statsmodels 0.14.6.
- No network.

## Commands
```
cp code -> $S/r2/analyses/lottery-tk-promotion; mkdir output figures; ln -s <repo>/data $S/r2/data
time <repo>/.venv/bin/python run_distaste_analysis.py      # run 1 (empty output/)
cmp / diff against the working tree; count macros; check duplicates
python run_distaste_analysis.py                            # run 2 (skip)
touch src/risk_aversion.py; python run_distaste_analysis.py # run 3 (re-validate)
latexmk -pdf -interaction=nonstopmode main.tex
```
