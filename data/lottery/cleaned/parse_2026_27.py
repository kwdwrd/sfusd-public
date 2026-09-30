#!/usr/bin/env python3
"""
Extract the 2026-27 kindergarten (and other grades') lottery request PDF into
a CSV matching the format already used for 2024-25 and 2025-26
("3. March <year> SFUSD Requests by School" reports share one template, so
this just needs pdfplumber's table extraction -- no reconstruction needed,
unlike 2018-19).

The report repeats its title, a blank row, and the column header on every
page; we drop those and concatenate the 11 pages' data rows.
"""

import pdfplumber
import pandas as pd

SRC = '../raw/3. March 2026 SFUSD Requests by School.pdf'
OUT = '3. March 2026 SFUSD Requests by School.csv'

COLUMNS = [
    'idSchool', 'SchoolName', 'Grade', 'Program Code', 'Program Name', 'Total Requests',
    '1st Choice', '2nd Choice', '3rd Choice', '4th Choice', '5th Choice',
    '6th Choice', '7th Choice', '8th Choice', '9th Choice', '10th+',
]


def main():
    rows = []
    with pdfplumber.open(SRC) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables():
                for row in table:
                    if row[0] in (None, '', 'idSchool') or row[1] is None:
                        continue  # title/blank/repeated-header rows
                    rows.append(row[:len(COLUMNS)])

    df = pd.DataFrame(rows, columns=COLUMNS)

    # A couple of Mission Education Center rows have their Program Name cell
    # truncated mid-word by the PDF's fixed column width (e.g. "...Spanish
    # Immersion (Non-Nat"), and the overflow letter ('N' from "Native)",
    # 'S' from "Speakers)") bleeds into the adjacent Total Requests cell
    # (e.g. "N 78" instead of "78"). Strip any such stray leading letter.
    before = df['Total Requests'].copy()
    df['Total Requests'] = df['Total Requests'].str.extract(r'(\d+)$')[0]
    n_fixed = (before != df['Total Requests']).sum()
    if n_fixed:
        print(f"Stripped a stray leading character from Total Requests on {n_fixed} row(s).")

    # sanity checks
    assert df.idSchool.str.match(r'^\d+$').all(), "non-numeric idSchool found"
    numeric_cols = ['Total Requests'] + COLUMNS[6:]
    for col in numeric_cols:
        bad = df[~df[col].str.match(r'^\d+$')]
        assert bad.empty, f"non-numeric value in {col}: {bad}"
    # each row's own choices should sum to its Total Requests (or possibly
    # exceed it slightly, if requests beyond 10th choice aren't separately
    # tracked -- SFUSD's own recent-year files already carry this same
    # convention, so we check consistency rather than assuming exact equality)
    choice_sum = df[COLUMNS[6:]].astype(int).sum(axis=1)
    total = df['Total Requests'].astype(int)
    mismatch = df[choice_sum != total]
    print(f"{len(mismatch)} / {len(df)} rows where summed choices != Total Requests "
          f"(compare against how often this already happens in an existing clean year, "
          f"e.g. 2024-25, before treating this as a problem)")

    df.to_csv(OUT, index=False)
    print(f"Wrote {len(df)} rows to {OUT}")


if __name__ == '__main__':
    main()
