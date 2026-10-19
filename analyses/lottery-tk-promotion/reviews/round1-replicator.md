# Round 1 — replicator (2026-10-18)

**Verdict: reproduces.** Every regenerated file byte-identical to the working tree.

## Setup
Scratch tree under the session scratchpad; `data` symlinked to the repo `data/`.
`output/` and `figures/` started empty. Repo `.venv` Python 3.11.14 with
pandas 2.3.3, numpy 2.3.5, scipy 1.16.3, matplotlib 3.10.8, statsmodels 0.14.6.

## 1. Does it run end to end?
**Not from one command on a clean state.** `run_distaste_analysis.py` computes
everything and writes all figures (~28 s), then crashes in `make_numbers.main()`
at `make_numbers.py:375` (the `risk_aversion` step) with
`FileNotFoundError: output/risk_aversion_validation.csv`. That file is written by
`validate_risk_aversion.py`, which must therefore run first (it passed 30/30
checks). After it, the second pipeline run exited 0.

`plot_first_rankings.py` is not called by the pipeline but is required for
`figures/first_rankings.png` and `output/first_rankings_by_year.csv`.

Required order: `validate_risk_aversion.py` -> `run_distaste_analysis.py` ->
`plot_first_rankings.py`. The CLAUDE.md Build section does not say this.

## 2. Differences
None. 19 output CSVs, `output/numbers.tex`, all 4 files in `output/tables/`, and
all 6 PNGs in `figures/` are identical. No file on only one side. The Oct 4-5
timestamps on `risk_aversion_validation.csv` and `sbac_2024_sfusd_schools.csv`
are not stale.

## 3. Orphans
None. `first_rankings.*` from `plot_first_rankings.py`;
`risk_aversion_validation.csv` from `validate_risk_aversion.py`;
`sbac_2024_sfusd_schools.csv` is a cache written by `src/promotion_rate.py`.
`lottery-tk-promotion.ipynb` is a legacy notebook, outside the pipeline.
Every figure and table used in `main.tex` is produced by code.

## 4. Nondeterminism
Only the capacity-constrained RSD simulation in `src/simulation.py`, seeded via
`np.random.default_rng(seed)` with `seed=0`, `n_sims=20`.
**`src/robustness.py:35` calls it with `n_sims=1`** and the default seed.
`validate_risk_aversion.py` uses fixed seeds. No other randomness found.
Caveat: `default_rng` streams can change across numpy versions, so
reproducibility is numpy-version-dependent. No systematic audit of dict/set
ordering or merge sorting was done.

## 5. Hidden dependencies
- No hardcoded `/Users` paths. Relative paths assume cwd is the paper directory.
- Reads `data/lottery/merged/sfusd-lottery-all.csv`,
  `data/lottery/merged/sfusd-school-id-mapping.csv`, `data/processed/schools.csv`,
  `data/processed/calpads.csv`, `data/enrollment/raw/cdenroll2324-v2.txt`,
  `data/enrollment/raw/cdenroll2425.txt`,
  `data/sbac/raw/sb_ca2024_all_csv_v1.zip` (~1 GB), and local
  `./Promotes from TK to K - Sheet1.csv`.
- `output/sbac_2024_sfusd_schools.csv` acts as a cache: if present, the SBAC zip
  is skipped. Regenerated from the zip here and matched.
- **Packages are unpinned** — no `requirements.txt`, `pyproject.toml` or lockfile;
  only an untracked `.python-version`.
- Unresolved: whether the `data/` inputs are tracked in git. A clean clone may
  not be able to run this.
- No network access needed. `latexmk` only for the PDF (not run).
