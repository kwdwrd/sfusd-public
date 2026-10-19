"""Build data/processed/enrollment.csv: school enrollment by race/ethnicity in long
format, joined to the canonical school_id via data/processed/schools.csv.

Input: data/enrollment/merged/sfusd-school-ethnicity-sum.csv (ACADEMIC_YEAR, CDS_CODE,
SCHOOL, plus 9 race/ethnicity count columns). Uses the "sum" (counts) file rather than
the pct variants - percentages can be derived downstream.

Like CALPADS, enrollment has a single cut dimension (race), so the output is
school_id/year/race/value rather than the generic cut_type/cut_code shape.

CDS_CODE crosswalk: for 2007-08 through 2022-23 the file uses the full 14-digit CDE
CDSCode (matches schools.csv cds_code directly). For 2023-24/2024-25 it uses the
short CDE "school" code only (matches schools.csv calpads_school_code, the same
last-7-digits value used for the CALPADS crosswalk). Both formats are tried.

Dropped:
- CDS_CODE 0/1 (District Office / Nonpublic-Nonsectarian Schools - not a school)
- County Office of Education schools (CDSCode district portion != SFUSD's 68478):
  S.F. County Court/Opportunity/Civic Center/Special Education programs
- CDS_CODEs with no match in schools.csv: closed/renamed/merged schools (e.g. the
  pre-2008 "Brown Jr (Willie L) Elementary", "Excelsior Middle", "Leadership High")
  and schools already out of scope from Phase 1/2 (Ida B Wells, Downtown, Gateway to
  College, a few unmatched charters)
"""

from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
ENROLLMENT_DATA = REPO_ROOT / "data/enrollment/merged/sfusd-school-ethnicity-sum.csv"
SCHOOLS = REPO_ROOT / "data/processed/schools.csv"
OUT_PATH = REPO_ROOT / "data/processed/enrollment.csv"

RACE_COLUMNS = [
    "African American",
    "American Indian or Alaska Native",
    "Asian",
    "Filipino",
    "Hispanic or Latino",
    "Not Reported",
    "Pacific Islander",
    "Two or More Races",
    "White",
]


def race_to_code(name: str) -> str:
    return name.lower().replace(" ", "_")


def main():
    df = pd.read_csv(ENROLLMENT_DATA)
    df["cds_code"] = df["CDS_CODE"].apply(lambda x: str(int(x)))

    schools = pd.read_csv(SCHOOLS, dtype={"cds_code": str, "calpads_school_code": str})
    cds_to_id = dict(
        zip(
            schools[schools["cds_code"].notna() & (schools["cds_code"] != "")]["cds_code"],
            schools["school_id"],
        )
    )
    calpads_to_id = dict(
        zip(
            schools[schools["calpads_school_code"].notna() & (schools["calpads_school_code"] != "")][
                "calpads_school_code"
            ].apply(int),
            schools["school_id"],
        )
    )

    def match(cds_code: str):
        if cds_code in cds_to_id:
            return cds_to_id[cds_code]
        if len(cds_code) <= 7:
            return calpads_to_id.get(int(cds_code))
        return None

    df["school_id"] = df["cds_code"].apply(match)

    unmatched = df[df["school_id"].isna()][["cds_code", "SCHOOL"]].drop_duplicates()
    if not unmatched.empty:
        print(f"Dropping {len(unmatched)} unmatched (cds_code, SCHOOL) combos with no schools.csv match:")
        for _, row in unmatched.iterrows():
            print(f"  {row['cds_code']}: {row['SCHOOL']}")
    df = df[df["school_id"].notna()].copy()
    df["school_id"] = df["school_id"].astype(int)
    df["year"] = df["ACADEMIC_YEAR"].str.split("-").str[0].astype(int)

    long = df.melt(
        id_vars=["school_id", "year"],
        value_vars=RACE_COLUMNS,
        var_name="race",
        value_name="value",
    )
    long["race"] = long["race"].apply(race_to_code)
    long["value"] = long["value"].astype(int)
    long = long.sort_values(["school_id", "year", "race"]).reset_index(drop=True)

    print(
        f"Wrote {len(long)} rows for {long['school_id'].nunique()} schools, "
        f"years {long['year'].min()}-{long['year'].max()}"
    )

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    long.to_csv(OUT_PATH, index=False)
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
