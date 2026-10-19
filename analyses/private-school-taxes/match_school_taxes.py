#!/usr/bin/env python3
"""
Match private school addresses to historical property tax records.

The assessor file uses a peculiar address format:
  "0000 0175 GOLDEN GATE         AV0000"
   ^^^^ ^^^^ ^^^^^^^^^^^^^^^^^^^ ^^^^^^
   |    |    street name          suffix + unit
   |    street number
   secondary number (or 0000)

The suffix codes are abbreviations: ST=Street, AV=Avenue, BL=Boulevard,
DR=Drive, WY=Way, TE=Terrace, CT=Court, PL=Place, LN=Lane, CR=Circle,
HW=Highway, AL=Alley, PK=Park, PZ=Plaza, RD=Road, etc.

Some streets like Broadway have no suffix code (just spaces + "0000").

This script parses school addresses into (number, street_name) and searches
the assessor data for matching records using the street number embedded in
the assessor's property_location field.
"""

import pandas as pd
import re
import sys


# Map common street suffixes to their assessor abbreviations
SUFFIX_MAP = {
    'street': 'ST', 'st': 'ST',
    'avenue': 'AV', 'ave': 'AV', 'av': 'AV',
    'boulevard': 'BL', 'blvd': 'BL', 'bl': 'BL',
    'drive': 'DR', 'dr': 'DR',
    'way': 'WY', 'wy': 'WY',
    'terrace': 'TE', 'ter': 'TE',
    'court': 'CT', 'ct': 'CT',
    'place': 'PL', 'pl': 'PL',
    'lane': 'LN', 'ln': 'LN',
    'circle': 'CR', 'cir': 'CR',
    'highway': 'HW', 'hwy': 'HW',
    'alley': 'AL', 'aly': 'AL',
    'road': 'RD', 'rd': 'RD',
    'plaza': 'PZ', 'plz': 'PZ',
    'park': 'PK', 'pk': 'PK',
    'walk': 'WK', 'wk': 'WK',
    'stairway': 'SW',
    'row': 'RW',
}


def parse_school_address(address):
    """
    Parse a school address like "3250 19th Ave, San Francisco, CA 94132"
    into (street_number, street_name, suffix).

    Returns:
        tuple: (street_number, street_name, suffix) or None if unparseable
    """
    if not address or not isinstance(address, str):
        return None

    # Strip city/state/zip
    addr = address.split(',')[0].strip()

    # Remove suite/unit info
    addr = re.sub(r'\s*(?:Suite|Ste|Apt|#|Unit)\s*\S+', '', addr, flags=re.IGNORECASE)

    # Match: number + street name + optional suffix
    m = re.match(r'^(\d+)\s+(.+)$', addr.strip())
    if not m:
        return None

    street_number = m.group(1)
    street_part = m.group(2).strip()

    # Split street name from suffix
    # Try matching known suffixes at the end
    suffix = None
    street_name = street_part
    words = street_part.split()
    if words:
        last_word = words[-1].lower().rstrip('.')
        if last_word in SUFFIX_MAP:
            suffix = SUFFIX_MAP[last_word]
            street_name = ' '.join(words[:-1])

    street_name = street_name.strip().upper()
    return (street_number, street_name, suffix)


def parse_assessor_address(prop_loc):
    """
    Parse an assessor property_location like "0000 3250 19TH                AV0000"
    into a list of (street_number, street_name, suffix) tuples.

    The first 4-digit field is sometimes a secondary/alternate street number.
    When non-zero, both it and the second field are valid street numbers for
    the same property (e.g. "2750 2700 JACKSON ST0000" means the property
    is addressable as both 2750 Jackson and 2700 Jackson).

    Returns:
        list[tuple]: List of (street_number, street_name, suffix) tuples
    """
    if not prop_loc or not isinstance(prop_loc, str):
        return []

    # Match: field1 field2 street_name suffix_and_unit
    m = re.match(
        r'^(\d{4}[A-Z]?)\s*(\d{4}[A-Z]?)\s*(.+?)\s{2,}(\S+)$',
        prop_loc.strip()
    )
    if not m:
        return []

    field1 = m.group(1).lstrip('0') or '0'
    field2 = m.group(2).lstrip('0') or '0'
    street_name = m.group(3).strip().upper()
    suffix_unit = m.group(4)

    # Extract suffix (first 2 chars if alphabetic, otherwise no suffix)
    suffix = None
    if len(suffix_unit) >= 2 and suffix_unit[:2].isalpha():
        suffix = suffix_unit[:2].upper()

    results = [(field2, street_name, suffix)]
    # Index the first field too when it's a real street number (non-zero)
    if field1 != '0' and field1 != field2:
        results.append((field1, street_name, suffix))

    return results


def build_assessor_index(assessor_df):
    """
    Build a lookup index from the assessor data.

    Returns:
        dict: Maps (street_number, street_name_normalized) -> list of row indices
    """
    index = {}
    for idx, row in assessor_df.iterrows():
        entries = parse_assessor_address(row['Property Location'])
        for street_number, street_name, suffix in entries:
            key = (street_number, street_name)
            if key not in index:
                index[key] = []
            index[key].append(idx)
    return index


def match_school(school_name, address, assessor_index, assessor_df):
    """
    Try to match a school address to assessor records.

    Returns:
        tuple: (matched: bool, match_info: str, matching_rows: DataFrame or None)
    """
    parsed = parse_school_address(address)
    if parsed is None:
        return (False, f"Could not parse address: {address!r}", None)

    street_number, street_name, suffix = parsed

    # Try exact match on (number, street_name)
    key = (street_number, street_name)
    if key in assessor_index:
        rows = assessor_df.iloc[assessor_index[key]]
        return (True, f"Matched on {street_number} {street_name}", rows)

    # Try with common street name variations
    # "Broadway St" -> "BROADWAY" (no suffix in assessor)
    # "9th Ave" -> "9TH" (street_name might need suffix stripped)
    # Numbers: "19th Ave" street_name = "19TH"
    # Handle ordinals: strip "St/Ave" that got absorbed into name
    for alt_name in _name_variations(street_name):
        alt_key = (street_number, alt_name)
        if alt_key in assessor_index:
            rows = assessor_df.iloc[assessor_index[alt_key]]
            return (True, f"Matched on {street_number} {alt_name} (variant)", rows)

    return (False, f"No match for {street_number} {street_name} (suffix={suffix})", None)


def _name_variations(name):
    """Generate alternate street name forms to try."""
    variations = []

    # "BROADWAY ST" -> "BROADWAY"
    for suf_word in ['ST', 'AV', 'AVE', 'BL', 'BLVD', 'DR', 'WY', 'CT', 'PL', 'LN']:
        if name.endswith(' ' + suf_word):
            variations.append(name[: -(len(suf_word) + 1)])

    # "LAGUNA HONDA BLVD" -> "LAGUNA HONDA"
    # Already handled above

    # "SAINT" <-> "ST" or "ST."
    if 'SAINT ' in name:
        variations.append(name.replace('SAINT ', 'ST '))
    if name.startswith('ST '):
        variations.append('SAINT ' + name[3:])

    return variations


def main():
    schools_csv = 'SF Private Schools - Database - Sheet1.csv'
    assessor_csv = 'Assessor_Historical_Secured_Property_Tax_Rolls_20260128-filtered.csv'

    print("Loading assessor data...")
    assessor_df = pd.read_csv(assessor_csv, low_memory=False)
    print(f"  Loaded {len(assessor_df):,} assessor records")

    print("Building address index...")
    assessor_index = build_assessor_index(assessor_df)
    print(f"  Indexed {len(assessor_index):,} unique addresses")

    print("Loading schools...")
    schools_df = pd.read_csv(schools_csv)
    # Drop empty rows and non-school rows (e.g. totals row at bottom of spreadsheet)
    schools_df = schools_df[
        schools_df['School Name'].notna()
        & (schools_df['School Name'].str.strip() != '')
        & ~schools_df['School Name'].str.contains('Total|Revenue', case=False, na=False)
    ].copy()
    print(f"  Loaded {len(schools_df)} schools")

    # Match each school
    results = []
    matched_records = []

    print("\n" + "=" * 80)
    print("MATCHING RESULTS")
    print("=" * 80)

    for _, school in schools_df.iterrows():
        name = school['School Name']
        address = school.get('Address', '')

        if not address or not isinstance(address, str) or address.strip() == '':
            print(f"\n  SKIP: {name}")
            print(f"    No address provided")
            results.append({
                'school_name': name,
                'address': '',
                'matched': False,
                'match_info': 'No address provided',
                'num_tax_records': 0,
                'years_covered': '',
            })
            continue

        matched, info, rows = match_school(name, address, assessor_index, assessor_df)

        if matched:
            n_records = len(rows)
            years = sorted(rows['Closed Roll Year'].unique())
            year_range = f"{years[0]}-{years[-1]}" if len(years) > 1 else str(years[0])
            print(f"\n  MATCH: {name}")
            print(f"    Address: {address}")
            print(f"    {info}")
            print(f"    {n_records} tax records ({year_range})")

            # Tag matched records with school name
            matched_rows = rows.copy()
            matched_rows['school_name'] = name
            matched_rows['school_address'] = address
            matched_records.append(matched_rows)

            results.append({
                'school_name': name,
                'address': address,
                'matched': True,
                'match_info': info,
                'num_tax_records': n_records,
                'years_covered': year_range,
            })
        else:
            print(f"\n  NO MATCH: {name}")
            print(f"    Address: {address}")
            print(f"    {info}")
            results.append({
                'school_name': name,
                'address': address,
                'matched': False,
                'match_info': info,
                'num_tax_records': 0,
                'years_covered': '',
            })

    # Summary
    results_df = pd.DataFrame(results)
    n_matched = results_df['matched'].sum()
    n_total = len(results_df)
    n_with_address = len(results_df[results_df['address'] != ''])

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total schools: {n_total}")
    print(f"Schools with addresses: {n_with_address}")
    print(f"Matched: {n_matched}")
    print(f"Unmatched (with address): {n_with_address - n_matched}")
    print(f"No address provided: {n_total - n_with_address}")

    # Save match results
    results_file = 'school_tax_match_results.csv'
    results_df.to_csv(results_file, index=False)
    print(f"\nMatch results saved to: {results_file}")

    # Save matched tax records
    if matched_records:
        all_matched = pd.concat(matched_records, ignore_index=True)
        tax_file = 'school_tax_records.csv'
        all_matched.to_csv(tax_file, index=False)
        print(f"Tax records saved to: {tax_file} ({len(all_matched):,} records)")

    # Print unmatched schools for review
    unmatched = results_df[(~results_df['matched']) & (results_df['address'] != '')]
    if len(unmatched) > 0:
        print(f"\n{'=' * 80}")
        print("UNMATCHED SCHOOLS (have address but no tax match)")
        print("=" * 80)
        for _, row in unmatched.iterrows():
            print(f"  {row['school_name']}")
            print(f"    Address: {row['address']}")
            print(f"    Reason: {row['match_info']}")


if __name__ == '__main__':
    main()
