"""SFUSD grade progression ratios from CDE census-day enrollment files.

Grade progression ratio (GPR) for grade g in year y:

    enrollment(grade g, year y) / enrollment(grade g-1, year y-1)

Above 1.0 means the grade gained students as the cohort advanced (net inflow);
below 1.0 means it lost them (net exits to private/charter/out-of-district,
moves, or grade retention).

Input: data/enrollment/raw/ CDE files (1981-82 through 1997-98, 2007-08 through
2024-25; CDE publishes no school-level files for 1998-99 through 2006-07, so
ratios for 1998-99 through 2007-08 are missing).

Two scopes:
- all:          every school under SFUSD's district code (68478), including
                SFUSD-authorized charters
- non_charter:  SFUSD-operated schools only

Both scopes exclude the Five Keys charters (SF Sheriff's jail-based adult
programs). They report thousands of adult students as grade 9/10/11 and would
swamp the high-school ratios.

TK caveat: CDE counted transitional kindergarten inside KN until 2022-23 and
reports GR_TK separately from 2023-24. TK expanded from 2012-13, and since TK
students go on to K (not grade 1), K->1 ratios from 2013-14 through 2023-24 are
depressed. From 2024-25 the K->1 denominator is K only, and a TK->K ratio is
available.

Outputs (alongside this script):
- sfusd_enrollment_by_grade.csv     scope, year, grade, enrollment
- grade_progression_long.csv        scope, year, cohort_k_year, class_of, grade,
                                    prior_grade, prior_enrollment, enrollment, ratio
- grade_progression_by_year.csv     scope=all, rows=year, cols=grade (ratio)
- grade_progression_by_cohort.csv   scope=all, rows=cohort (K year), cols=grade (ratio)
- cohort_retention_vs_k.csv         scope=all, rows=cohort, cols=grade
                                    (enrollment / that cohort's K enrollment)

Year convention: fall start year (2024 = 2024-25), matching data/processed/.
"""

from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
RAW = REPO_ROOT / "data/enrollment/raw"
OUT_DIR = Path(__file__).resolve().parent

SFUSD_PREFIX = "3868478"  # county 38 + district 68478
FIVE_KEYS_SCHOOL_CODES = {"0101774", "0118133", "0118141"}

OLD_FILES = [
    "enr198192-v2.txt",
    "enr199397-v2.txt",
    "enr200713-v3.txt",
    "enr201416-v2.txt",
    "enr201719-v2.txt",
    "enr202022-v2.txt",
]
NEW_FILES = ["cdenroll2324-v2.txt", "cdenroll2425.txt"]

GRADES = ["TK", "K"] + [str(g) for g in range(1, 13)]
GRADE_NUM = {"TK": -1, "K": 0, **{str(g): g for g in range(1, 13)}}
OLD_GRADE_COLS = {"GR_KN": "K", **{f"GR_{g}": str(g) for g in range(1, 13)}}
NEW_GRADE_COLS = {"GR_TK": "TK", "GR_KN": "K", **{f"GR_{g:02d}": str(g) for g in range(1, 13)}}


def parse_year(s: str) -> int:
    s = str(s)
    if "-" in s:  # "2007-08"
        return int(s.split("-")[0])
    return 1900 + int(s[:2])  # "8182"


def charter_cds_codes() -> set:
    ps = pd.read_csv(RAW / "pubschls.txt", sep="\t", dtype=str, encoding="latin1")
    return set(ps.loc[ps["Charter"] == "Y", "CDSCode"])


def load_old(charters: set) -> pd.DataFrame:
    frames = []
    for name in OLD_FILES:
        df = pd.read_csv(RAW / name, sep="\t", dtype={"CDS_CODE": str, "ACADEMIC_YEAR": str}, low_memory=False)
        df = df[df["CDS_CODE"].str.startswith(SFUSD_PREFIX)]
        if "ENR_TYPE" in df:  # 2014+ files repeat each row as C (cumulative) and P (primary)
            df = df[df["ENR_TYPE"] == "P"]
        df = df[~df["CDS_CODE"].str[-7:].isin(FIVE_KEYS_SCHOOL_CODES)]
        long = df.melt(id_vars=["ACADEMIC_YEAR", "CDS_CODE"], value_vars=list(OLD_GRADE_COLS),
                       var_name="col", value_name="enrollment")
        long["grade"] = long["col"].map(OLD_GRADE_COLS)
        long["year"] = long["ACADEMIC_YEAR"].map(parse_year)
        long["is_charter"] = long["CDS_CODE"].isin(charters)
        long["enrollment"] = pd.to_numeric(long["enrollment"], errors="coerce").fillna(0)
        frames.append(long[["year", "grade", "is_charter", "enrollment"]])
    return pd.concat(frames)


def load_new() -> pd.DataFrame:
    frames = []
    for name in NEW_FILES:
        df = pd.read_csv(RAW / name, sep="\t", dtype=str, encoding="latin1")
        df = df[(df["DistrictCode"] == "68478") & (df["AggregateLevel"] == "S") & (df["ReportingCategory"] == "TA")]
        df = df[~df["SchoolCode"].isin(FIVE_KEYS_SCHOOL_CODES)]
        long = df.melt(id_vars=["AcademicYear", "Charter"], value_vars=list(NEW_GRADE_COLS),
                       var_name="col", value_name="enrollment")
        long["grade"] = long["col"].map(NEW_GRADE_COLS)
        long["year"] = long["AcademicYear"].map(parse_year)
        long["is_charter"] = long["Charter"] == "Y"
        long["enrollment"] = pd.to_numeric(long["enrollment"], errors="coerce").fillna(0)
        frames.append(long[["year", "grade", "is_charter", "enrollment"]])
    return pd.concat(frames)


def by_grade(raw: pd.DataFrame) -> pd.DataFrame:
    all_ = raw.groupby(["year", "grade"], as_index=False)["enrollment"].sum().assign(scope="all")
    nc = raw[~raw["is_charter"]].groupby(["year", "grade"], as_index=False)["enrollment"].sum().assign(scope="non_charter")
    out = pd.concat([all_, nc])
    out["enrollment"] = out["enrollment"].astype(int)
    out["grade_num"] = out["grade"].map(GRADE_NUM)
    return out.sort_values(["scope", "year", "grade_num"])[["scope", "year", "grade", "grade_num", "enrollment"]]


def progression(enr: pd.DataFrame) -> pd.DataFrame:
    prior = enr.rename(columns={"grade": "prior_grade", "enrollment": "prior_enrollment"})
    prior = prior.assign(year=prior["year"] + 1, grade_num=prior["grade_num"] + 1)
    df = enr.merge(prior, on=["scope", "year", "grade_num"], how="inner")
    df = df[df["prior_enrollment"] > 0]
    df["ratio"] = (df["enrollment"] / df["prior_enrollment"]).round(4)
    df["cohort_k_year"] = df["year"] - df["grade_num"]
    df["class_of"] = df["cohort_k_year"] + 13
    cols = ["scope", "year", "cohort_k_year", "class_of", "grade", "prior_grade",
            "prior_enrollment", "enrollment", "ratio"]
    return df.sort_values(["scope", "year", "grade_num"])[cols]


def main():
    raw = pd.concat([load_old(charter_cds_codes()), load_new()])
    enr = by_grade(raw)
    prog = progression(enr)

    enr.drop(columns="grade_num").to_csv(OUT_DIR / "sfusd_enrollment_by_grade.csv", index=False)
    prog.to_csv(OUT_DIR / "grade_progression_long.csv", index=False)

    a = prog[prog["scope"] == "all"]
    grade_order = [g for g in GRADES if g in set(a["grade"])]
    by_year = a.pivot(index="year", columns="grade", values="ratio")[grade_order]
    by_year.to_csv(OUT_DIR / "grade_progression_by_year.csv")

    by_cohort = a.pivot(index="cohort_k_year", columns="grade", values="ratio")[grade_order]
    by_cohort.insert(0, "class_of", by_cohort.index + 13)
    by_cohort.to_csv(OUT_DIR / "grade_progression_by_cohort.csv")

    e = enr[enr["scope"] == "all"].assign(cohort_k_year=lambda d: d["year"] - d["grade_num"])
    e = e[e["grade"] != "TK"]
    counts = e.pivot(index="cohort_k_year", columns="grade", values="enrollment")
    counts = counts[[g for g in GRADES if g in counts.columns]]
    counts = counts[counts["K"].notna()]
    retention = counts.div(counts["K"], axis=0).round(4)
    retention.insert(0, "class_of", retention.index + 13)
    retention.to_csv(OUT_DIR / "cohort_retention_vs_k.csv")

    print(f"Years: {sorted(int(y) for y in enr['year'].unique())}")
    print(f"Wrote {len(prog)} progression rows to {OUT_DIR}")
    pd.set_option("display.width", 200)
    print(by_year.round(3).to_string())


if __name__ == "__main__":
    main()
