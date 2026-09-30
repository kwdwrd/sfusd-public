#!/usr/bin/env python3
"""
Extract the 2020-21 lottery PDF into a CSV matching the layout already used
for 2021-22 ("Enrollment Highlights 2021-22 MR - Program by Rank.xlsx -
Table 1.csv"): one row per (Grade, Program, SchoolName) with preference-rank
bucket columns.

Per the district's own title for this report, 2020-21 (a COVID year) only
ran the lottery for transition grades: TK, Kindergarten, 6th Grade, 9th
Grade -- not a full K-12 cycle.

Unlike 2018-19, there's no missing source PDF here to reconstruct blind:
this is a direct pdfplumber extraction of the actual PDF. The catch is that
the source table uses merged cells for Grade and Program (each printed only
once, on the first row of its group; later rows leave those columns visually
blank and start directly with a school name), which a naive text dump would
lose. We recover the true structure using each word's x-position instead of
text order: the header row shows

    Grade @ x=73, Program @ x=108, SchoolName @ x=215, and 8 preference-rank
    columns starting at x=354 (1st, 2nd, 3rd, 4th, 5th, 6th-10th, 11th-25th,
    26th+), each ~23pt apart.

Words are grouped into visual rows by y-position, then bucketed into these
columns by x-position; a row with no Grade/Program words simply continues
whatever grade/program was last seen (forward-fill), exactly matching how a
person reading the PDF would parse a merged-cell table.
"""

import pdfplumber
import pandas as pd

SRC = '../raw/Program Requests by Rank 2020-2021.pdf'
OUT = 'sfusd_lottery_2020_2021_parsed.csv'

# From the header row (see module docstring): rank-column left edges.
RANK_COLUMNS = ['1st', '2nd', '3rd', '4th', '5th', '6th-10th', '11th-25th', '26th+']
RANK_ANCHORS = [354.1, 375.9, 399.4, 422.9, 446.0, 462.8, 486.4, 516.2]

GRADE_MAX_X = 95      # Grade column (~73) ends before Program starts (~108)
PROGRAM_MAX_X = 205   # Program column (~108) ends before SchoolName starts (~215)
SCHOOL_MAX_X = 350     # SchoolName column (~215) ends before rank columns start (~354)

VALID_GRADES = {'TK', 'Kindergarten', '6th Grade', '9th Grade'}


def group_into_rows(words, tol=2.5):
    """Cluster words into visual rows by y-position (pdfplumber's 'top')."""
    words = sorted(words, key=lambda w: w['top'])
    rows, current, current_top = [], [], None
    for w in words:
        if current_top is None or abs(w['top'] - current_top) <= tol:
            current.append(w)
            current_top = w['top'] if current_top is None else current_top
        else:
            rows.append(current)
            current, current_top = [w], w['top']
    if current:
        rows.append(current)
    return rows


def nearest_rank_column(x0):
    return min(range(len(RANK_ANCHORS)), key=lambda i: abs(RANK_ANCHORS[i] - x0))


def parse_pdf():
    records = []
    current_grade = None
    current_program = None

    with pdfplumber.open(SRC) as pdf:
        for page_num, page in enumerate(pdf.pages):
            words = page.extract_words()
            for row in group_into_rows(words):
                row = sorted(row, key=lambda w: w['x0'])
                row_text = {w['text'] for w in row}
                if row_text & {'SFUSD', 'ProgramCode', 'Preference', 'choice'}:
                    continue  # title / repeated column-header rows

                grade_words = [w['text'] for w in row if w['x0'] < GRADE_MAX_X]
                program_words = [w['text'] for w in row if GRADE_MAX_X <= w['x0'] < PROGRAM_MAX_X]
                school_words = [w['text'] for w in row if PROGRAM_MAX_X <= w['x0'] < SCHOOL_MAX_X]
                number_words = [w for w in row if w['x0'] >= SCHOOL_MAX_X]

                if grade_words:
                    grade_text = ' '.join(grade_words)
                    assert grade_text in VALID_GRADES, (
                        f"page {page_num+1}: unrecognized grade token {grade_text!r} "
                        f"(row: {[w['text'] for w in row]})"
                    )
                    current_grade = grade_text

                if program_words:
                    current_program = ' '.join(program_words)

                if not school_words:
                    continue  # bare grade- or program-continuation header line, no data

                school_name = ' '.join(school_words)
                counts = {}
                for w in number_words:
                    col = RANK_COLUMNS[nearest_rank_column(w['x0'])]
                    assert col not in counts, (
                        f"page {page_num+1}: two values landed in the same rank column "
                        f"({school_name}, column {col}: {counts[col]!r} and {w['text']!r})"
                    )
                    counts[col] = int(w['text'])

                assert current_grade is not None and current_program is not None, (
                    f"page {page_num+1}: data row before any grade/program header seen "
                    f"({school_name})"
                )
                records.append({
                    'Grade': current_grade,
                    'Program': current_program,
                    'SchoolName': school_name,
                    **{col: counts.get(col, '') for col in RANK_COLUMNS},
                })

    return pd.DataFrame(records)


def main():
    df = parse_pdf()
    print(f"Parsed {len(df)} rows.")
    print("Grades found:", sorted(df.Grade.unique()))
    print("Distinct (Grade, Program) groups:", df[['Grade', 'Program']].drop_duplicates().shape[0])

    known_schools = set(pd.read_csv('../merged/sfusd-school-names.csv').school_name)
    unknown = sorted(set(df.SchoolName) - known_schools)
    if unknown:
        print(f"\n{len(unknown)} school name(s) not found in sfusd-school-names.csv "
              f"(double check these -- could be a genuinely new/renamed school, or a parsing slip):")
        for name in unknown:
            print(" ", repr(name))

    df.to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == '__main__':
    main()
