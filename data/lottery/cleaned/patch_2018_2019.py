#!/usr/bin/env python3
"""
Patch the 21 corrupted rows in sfusd_lottery_2018_2019_parsed.csv.

Root cause (identified 2026-09-29): the source PDF for 2018-19 no longer
exists in this repo -- it was converted to CSV by an earlier LLM pass, which
evidently free-form-transcribed the table rather than extracting it against
a fixed column grid. For 3 schools, a handful of rows had one or more
leading IDENTIFIER columns (idSchool, and for some rows also Grade and
Program) blank or otherwise unreadable in the source; the transcription
dropped those blank cells instead of preserving them as empty, so every
later numeric column silently shifted left by however many identifier
columns were missing. The numeric request-count columns (1st Choice through
Grand Total) were never themselves corrupted -- they just landed under the
wrong header.

This is recoverable because:
  (a) the request-count columns still appear as a contiguous, ordered run
      whose sum equals the (now mis-positioned) Grand Total -- so the shift
      amount for each row is exactly the number of trailing NaN cells, and
      re-aligning by that amount is checksum-verifiable (every one of the
      21 rows below sums exactly), and
  (b) the "missing" Grade/Program values for the worst-corrupted rows (see
      SHIFT_3_GRADE_PROGRAM below) survived as garbled text spliced into
      SchoolName (e.g. "...DeAvila E5S09 KG CE" -> Grade=KG, Program=CE;
      "...5A0c7adTeKmy ESGE" -> "Ac[TK]ademy ES[GE]" -> Grade=TK, Program=GE),
      which we can read back out by hand, and
  (c) idSchool -- which looks entirely absent -- actually survives too, the
      same way: it's interleaved character-by-character into the corrupted
      SchoolName alongside a normal name fragment. "E5S09" is "ES" and "509"
      woven together (E-5-S-0-9); "5A0c7ademy" is "507" and "Academy"
      (5-A-0-c-7-ademy); "McAte8e3r2 HS" is "832" and "er" spliced into
      "McAte" + "er" + " HS" (8-e-3-r-2). This is a nice independent check
      (see verify_ids_from_corrupted_text below) but we still source the
      canonical id from sfusd-school-id-mapping.csv rather than parsing it
      out of each row, since the mapping is the authoritative source and
      the interleaving is a one-off transcription artifact, not a format
      we want other code to have to understand.

Run from this directory: `python3 patch_2018_2019.py`. Writes
sfusd_lottery_2018_2019_patched.csv alongside the original (which is left
untouched).
"""

import pandas as pd

SRC = 'sfusd_lottery_2018_2019_parsed.csv'
OUT = 'sfusd_lottery_2018_2019_patched.csv'

CHOICE_COLS = [
    '1st Choice', '2nd Choice', '3rd Choice', '4th Choice',
    '5th Choice', '6th Choice', '7th Choice', '8th Choice+', 'Grand Total',
]

SCHOOL_IDS = {
    'Chinese Immersion School at DeAvila ES': 509,
    'Drew (Dr Charles) College Preparatory Academy ES': 507,
    'The Academy - SF @ McAteer HS': 832,
}

# Rows where only idSchool was dropped (shift=1): Grade/Program are already
# correctly present, just mislabeled one column early. Row index -> clean
# school name.
SHIFT_1_ROWS = {
    175: 'Chinese Immersion School at DeAvila ES', 176: 'Chinese Immersion School at DeAvila ES',
    177: 'Chinese Immersion School at DeAvila ES', 178: 'Chinese Immersion School at DeAvila ES',
    179: 'Chinese Immersion School at DeAvila ES', 180: 'Chinese Immersion School at DeAvila ES',
    1138: 'The Academy - SF @ McAteer HS', 1139: 'The Academy - SF @ McAteer HS',
    1140: 'The Academy - SF @ McAteer HS', 1141: 'The Academy - SF @ McAteer HS',
    1142: 'The Academy - SF @ McAteer HS', 1143: 'The Academy - SF @ McAteer HS',
}

# Rows where idSchool, Grade, and Program were all dropped (shift=3):
# Grade/Program recovered from text spliced into the corrupted SchoolName.
SHIFT_3_GRADE_PROGRAM = {
    173: ('Chinese Immersion School at DeAvila ES', 'KG', 'CE'),
    174: ('Chinese Immersion School at DeAvila ES', 'KG', 'CN'),
    232: ('Drew (Dr Charles) College Preparatory Academy ES', 'TK', 'GE'),
    233: ('Drew (Dr Charles) College Preparatory Academy ES', 'KG', 'GE'),
    234: ('Drew (Dr Charles) College Preparatory Academy ES', '1', 'GE'),
    235: ('Drew (Dr Charles) College Preparatory Academy ES', '2', 'GE'),
    236: ('Drew (Dr Charles) College Preparatory Academy ES', '3', 'GE'),
    237: ('Drew (Dr Charles) College Preparatory Academy ES', '4', 'GE'),
    238: ('Drew (Dr Charles) College Preparatory Academy ES', '5', 'GE'),
}


def verify_ids_from_corrupted_text(original_df, patched_idx, assigned_ids):
    """
    Independent cross-check: the true school id is separately recoverable by
    pulling the digit characters straight out of each row's *original*
    (pre-patch) corrupted SchoolName -- e.g. 'E5S09' -> '509' -- since the id
    was transcribed character-interleaved with a normal name fragment rather
    than actually dropped. If that digit extraction doesn't agree with the
    id we're about to assign from sfusd-school-id-mapping.csv, something
    about our reconstruction (or this discovery) is wrong.
    """
    for idx in patched_idx:
        digits = ''.join(ch for ch in original_df.loc[idx, 'SchoolName'] if ch.isdigit())
        assigned = assigned_ids[idx]
        assert digits.startswith(str(assigned)), (
            f"row {idx}: digits {digits!r} extracted from corrupted name don't start with "
            f"assigned id {assigned}"
        )


def patch(df):
    df = df.copy()

    def apply_true_choices(idx, school_name, grade, program, true_choices):
        # true_choices is always [true_C1, true_C2, ..., true_C8plus, true_GrandTotal]
        # (9 values) -- the last one IS the grand total already, recovered
        # directly from the row's shifted data, not recomputed by summing.
        assert len(true_choices) == 9
        true_choices = [int(float(v)) for v in true_choices]  # request counts are always whole numbers
        recomputed_total = sum(true_choices[:-1])
        assert recomputed_total == true_choices[-1], (
            f"row {idx}: choice columns {true_choices[:-1]} sum to {recomputed_total}, "
            f"expected grand total {true_choices[-1]}"
        )
        df.loc[idx, 'SchoolName'] = school_name
        df.loc[idx, 'idSchool'] = SCHOOL_IDS[school_name]
        df.loc[idx, 'Grade'] = grade
        df.loc[idx, 'Program'] = program
        for col, value in zip(CHOICE_COLS[:-1], true_choices[:-1]):  # 1st..8th+
            df.loc[idx, col] = value
        df.loc[idx, 'Grand Total'] = true_choices[-1]

    for idx, school_name in SHIFT_1_ROWS.items():
        row = df.loc[idx]
        # cast to str: Grade is a string column elsewhere in the file (e.g.
        # 'KG', '1', ..., '13'); leaving this as the raw int64 pulled from
        # the idSchool column would create a mixed-type column.
        grade, program = str(int(row['idSchool'])), row['Grade']
        # true_C1 was printed under 'Program'; true_C2..true_C8plus were
        # printed under '1st Choice'..'7th Choice'; true_GrandTotal was
        # printed under '8th Choice+'.
        true_choices = [row['Program']] + [row[c] for c in CHOICE_COLS[:-1]]
        apply_true_choices(idx, school_name, grade, program, true_choices)

    for idx, (school_name, grade, program) in SHIFT_3_GRADE_PROGRAM.items():
        row = df.loc[idx]
        # true_C1..true_C3 were printed under 'idSchool'/'Grade'/'Program';
        # true_C4..true_C8plus under '1st Choice'..'5th Choice'; true_GrandTotal
        # under '6th Choice'.
        true_choices = [row['idSchool'], row['Grade'], row['Program']] + [row[c] for c in CHOICE_COLS[:-3]]
        apply_true_choices(idx, school_name, grade, program, true_choices)

    return df


def main():
    df = pd.read_csv(SRC)
    patched_idx = list(SHIFT_1_ROWS) + list(SHIFT_3_GRADE_PROGRAM)
    assert len(patched_idx) == 21, f"expected 21 rows to patch, got {len(patched_idx)}"
    assert set(SHIFT_1_ROWS) & set(SHIFT_3_GRADE_PROGRAM) == set(), "row index in both shift groups"

    assigned_ids = {
        idx: SCHOOL_IDS[name] for idx, name in SHIFT_1_ROWS.items()
    } | {
        idx: SCHOOL_IDS[name] for idx, (name, _, _) in SHIFT_3_GRADE_PROGRAM.items()
    }
    verify_ids_from_corrupted_text(df, patched_idx, assigned_ids)
    print(f"Independent check passed: the school id recovered by interleaving digits out of "
          f"each row's original corrupted SchoolName agrees with sfusd-school-id-mapping.csv "
          f"for all {len(patched_idx)} rows.")

    patched = patch(df)
    after = patched.loc[patched_idx]

    print(f"\nPatched {len(patched_idx)} rows.")
    print(after[['SchoolName', 'idSchool', 'Grade', 'Program'] + CHOICE_COLS].to_string())

    # sanity: no more NaNs among the patched rows, and idSchool is always known
    assert not after[CHOICE_COLS].isnull().any().any(), "patched rows still contain NaN choice counts"
    assert after.idSchool.isin(SCHOOL_IDS.values()).all(), "patched rows have an unexpected idSchool"

    patched.to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == '__main__':
    main()
