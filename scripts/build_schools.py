"""Build data/processed/schools.csv, the canonical school master table.

Combines:
- data/school-mapping/merged/school-id-mapping.csv (107 traditional SFUSD schools,
  with the SFUSD lottery school_id and CDE CDSCode)
- data/enrollment/raw/pubschls.txt (CDE master list; source for active SF charter
  schools and for CDSCodes of EES/preschool sites)
- data/calpads/merged/sfusd-school-codes.csv (CALPADS school codes, incl. EES/preschool
  sites not present in the lottery school list)

CALPADS school_code is the last 7 digits of CDSCode (county+district+school = 2+5+7
digits), so the CALPADS crosswalk is computed arithmetically rather than by name match.
"""

import re
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHOOL_ID_MAPPING = REPO_ROOT / "data/school-mapping/merged/school-id-mapping.csv"
PUBSCHLS = REPO_ROOT / "data/enrollment/raw/pubschls.txt"
CALPADS_SCHOOL_CODES = REPO_ROOT / "data/calpads/merged/sfusd-school-codes.csv"
CALPADS_DATA = REPO_ROOT / "data/calpads/merged/sfusd-calpads-data.csv"
OUT_PATH = REPO_ROOT / "data/processed/schools.csv"

NAME_INVERSION = re.compile(r"^(\S+) \(([^)]+)\)(.*)$")

CHARTER_SCHOOL_ID_START = 1000

COLUMNS = [
    "school_id",
    "slug",
    "school_name",
    "cds_code",
    "calpads_school_code",
    "is_charter",
    "school_level",
    "grade_low",
    "grade_high",
]


def normalize_name(name: str) -> str:
    """"Harte (Bret) ES" -> "Bret Harte ES"; names without a single-token
    prefix before the parenthetical pass through unchanged."""
    m = NAME_INVERSION.match(name)
    if not m:
        return name
    return f"{m.group(2)} {m.group(1)}{m.group(3)}"


def slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower())
    return slug.strip("-")


def cds_code_from_float(value) -> str:
    if pd.isna(value):
        return ""
    return str(int(value))


def calpads_code_from_cds(cds_code: str) -> str:
    if not cds_code:
        return ""
    return cds_code[-7:].zfill(7)


def grade_span_to_level(grade_low: str, grade_high: str):
    if not grade_high:
        return None
    high = 0 if grade_high == "K" else int(grade_high)
    low = 0 if grade_low == "K" else int(grade_low)
    if high <= 5:
        return "ES"
    if low == 0 and high == 8:
        return "K8"
    if high == 8:
        return "MS"
    if high == 12:
        return "HS"
    return None


def split_grade_span(gs_served: str):
    if pd.isna(gs_served) or not gs_served:
        return "", ""
    low, high = gs_served.split("-")
    return low, high


def build_traditional_schools() -> pd.DataFrame:
    df = pd.read_csv(SCHOOL_ID_MAPPING)

    dropped = df[df["school_id"].isna()]["school_name"].tolist()
    if dropped:
        print(f"Dropping {len(dropped)} schools with no school_id: {dropped}")
    df = df[df["school_id"].notna()].copy()

    rows = []
    for _, row in df.iterrows():
        cds_code = cds_code_from_float(row["CDSCode"])
        grade_low, grade_high = split_grade_span(row["GSserved"])
        if not cds_code and not grade_low:
            # The "Access SFUSD" special-education program sites have no CDE
            # record (no CDSCode, no grade span).
            school_level = "SPED"
        else:
            school_level = grade_span_to_level(grade_low, grade_high)

        name = normalize_name(row["school_name"])
        rows.append(
            {
                "school_id": int(row["school_id"]),
                "slug": slugify(name),
                "school_name": name,
                "cds_code": cds_code,
                "calpads_school_code": calpads_code_from_cds(cds_code),
                "is_charter": False,
                "school_level": school_level,
                "grade_low": grade_low,
                "grade_high": grade_high,
            }
        )
    return pd.DataFrame(rows, columns=COLUMNS)


def build_charter_schools(next_school_id: int) -> pd.DataFrame:
    df = pd.read_csv(PUBSCHLS, sep="\t", low_memory=False)
    charters = df[
        (df["County"] == "San Francisco")
        & (df["Charter"] == "Y")
        & (df["StatusType"] == "Active")
    ].copy()

    charters["normalized_name"] = charters["School"].apply(normalize_name)
    charters = charters.sort_values("normalized_name").reset_index(drop=True)

    rows = []
    for i, row in charters.iterrows():
        cds_code = str(int(row["CDSCode"]))
        grade_low, grade_high = split_grade_span(row["GSserved"])
        name = row["normalized_name"]
        rows.append(
            {
                "school_id": next_school_id + i,
                "slug": slugify(name),
                "school_name": name,
                "cds_code": cds_code,
                "calpads_school_code": calpads_code_from_cds(cds_code),
                "is_charter": True,
                "school_level": grade_span_to_level(grade_low, grade_high),
                "grade_low": grade_low,
                "grade_high": grade_high,
            }
        )
    return pd.DataFrame(rows, columns=COLUMNS)


def build_ees_schools(next_school_id: int) -> pd.DataFrame:
    calpads_codes = pd.read_csv(CALPADS_SCHOOL_CODES, dtype={"school_code": str})
    preschools = calpads_codes[calpads_codes["school_type"] == "Preschool"]

    pubschls = pd.read_csv(PUBSCHLS, sep="\t", low_memory=False)
    sf = pubschls[pubschls["County"] == "San Francisco"]
    cds_by_name = dict(zip(sf["School"], sf["CDSCode"]))

    rows = []
    sorted_preschools = preschools.sort_values("school_name").reset_index(drop=True)
    for i, row in sorted_preschools.iterrows():
        raw_name = row["school_name"]
        cds_code = ""
        if raw_name in cds_by_name:
            cds_code = str(int(cds_by_name[raw_name]))
        else:
            print(f"Warning: no CDSCode match for EES site '{raw_name}'")

        name = normalize_name(raw_name)
        rows.append(
            {
                "school_id": next_school_id + i,
                "slug": slugify(name),
                "school_name": name,
                "cds_code": cds_code,
                "calpads_school_code": row["school_code"],
                "is_charter": False,
                "school_level": "EES",
                "grade_low": "P",
                "grade_high": "P",
            }
        )
    return pd.DataFrame(rows, columns=COLUMNS)


def verify(schools: pd.DataFrame):
    dupe_ids = schools["school_id"][schools["school_id"].duplicated()]
    dupe_slugs = schools["slug"][schools["slug"].duplicated()]
    if not dupe_ids.empty:
        print(f"WARNING: duplicate school_id values: {dupe_ids.tolist()}")
    if not dupe_slugs.empty:
        print(f"WARNING: duplicate slug values: {dupe_slugs.tolist()}")

    calpads_data = pd.read_csv(CALPADS_DATA, dtype={"school_code": str})
    known_codes = set(calpads_data["school_code"].unique())
    computed = schools[schools["calpads_school_code"] != ""]
    unmatched = computed[~computed["calpads_school_code"].isin(known_codes)]
    if not unmatched.empty:
        print(
            f"Note: {len(unmatched)} computed calpads_school_code values have no "
            f"matching rows in sfusd-calpads-data.csv:"
        )
        for _, row in unmatched.iterrows():
            print(f"  {row['school_name']}: {row['calpads_school_code']}")

    print("\nSummary by school_level / is_charter:")
    print(schools.groupby(["school_level", "is_charter"]).size())


def main():
    traditional = build_traditional_schools()
    charters = build_charter_schools(CHARTER_SCHOOL_ID_START)
    ees = build_ees_schools(CHARTER_SCHOOL_ID_START + len(charters))

    schools = pd.concat([traditional, charters, ees], ignore_index=True)
    schools = schools.sort_values("school_id").reset_index(drop=True)

    print(
        f"Built schools.csv: {len(traditional)} traditional + {len(charters)} "
        f"charter + {len(ees)} EES = {len(schools)} rows"
    )
    verify(schools)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    schools.to_csv(OUT_PATH, index=False)
    print(f"\nWrote {OUT_PATH}")


if __name__ == "__main__":
    main()
