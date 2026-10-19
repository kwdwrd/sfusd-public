"""Build data/processed/calpads.csv: CALPADS enrollment/FRPM metrics in long format,
joined to the canonical school_id via data/processed/schools.csv.

Input: data/calpads/merged/sfusd-calpads-data.csv (school_code, academic_year, plus
10 metric columns: enrollment, frpm_count, foster_count, homeless_count,
migrant_program_count, direct_certification_count, frpm_eligible_count,
english_learned_count, unduplicated_count, foster_tribal_count).

CALPADS has no grade or demographic sub-cuts, so rather than the generic
school_id/year/grade/cut_type/cut_code/value shape, the output is the simpler
school_id/year/metric/value (one row per school/year/metric).

Dropped:
- school_code == "0000000" (district/LEA-level totals, not a single school)
- Duplicate rows that differ only by source_sheet (LEA-level vs school-level
  reporting for charter schools that are their own LEA) - identical metric
  values, deduplicated.
- school_codes with no match in schools.csv (alternative/continuation/special-ed/
  court schools and a few closed/unmatched charters - out of scope for now)
- NaN metric values (e.g. foster_tribal_count wasn't tracked before 2020-21)

year = the first year of the academic_year range (e.g. "2019-2020" -> 2019).
"""

from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
CALPADS_DATA = REPO_ROOT / "data/calpads/merged/sfusd-calpads-data.csv"
SCHOOLS = REPO_ROOT / "data/processed/schools.csv"
OUT_PATH = REPO_ROOT / "data/processed/calpads.csv"

METRIC_COLUMNS = [
    "enrollment",
    "frpm_count",
    "foster_count",
    "homeless_count",
    "migrant_program_count",
    "direct_certification_count",
    "frpm_eligible_count",
    "english_learned_count",
    "unduplicated_count",
    "foster_tribal_count",
]


def main():
    df = pd.read_csv(CALPADS_DATA, dtype={"school_code": str})
    df = df[df["school_code"] != "0000000"]
    df = df.drop(columns=["source_sheet"]).drop_duplicates()

    schools = pd.read_csv(SCHOOLS, dtype={"calpads_school_code": str})
    crosswalk = schools[schools["calpads_school_code"].notna() & (schools["calpads_school_code"] != "")]
    code_to_id = dict(zip(crosswalk["calpads_school_code"], crosswalk["school_id"]))

    unmatched = sorted(set(df["school_code"]) - set(code_to_id))
    if unmatched:
        print(f"Dropping {len(unmatched)} CALPADS school_codes with no schools.csv match: {unmatched}")
    df = df[df["school_code"].isin(code_to_id)].copy()
    df["school_id"] = df["school_code"].map(code_to_id)
    df["year"] = df["academic_year"].str.split("-").str[0].astype(int)

    long = df.melt(
        id_vars=["school_id", "year"],
        value_vars=METRIC_COLUMNS,
        var_name="metric",
        value_name="value",
    )
    long = long.dropna(subset=["value"])
    long["value"] = long["value"].astype(int)
    long = long.sort_values(["school_id", "year", "metric"]).reset_index(drop=True)

    print(f"Wrote {len(long)} rows for {long['school_id'].nunique()} schools, years {long['year'].min()}-{long['year'].max()}")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    long.to_csv(OUT_PATH, index=False)
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
