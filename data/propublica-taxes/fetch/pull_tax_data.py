"""
Script 2: Pull tax data for PTAs and PTOs

This script:
1. Loads the PTA search results from Script 1
2. Loads an external CSV of additional PTOs not found in the search
3. Pulls detailed tax information from ProPublica for all organizations
4. Merges with a mapping CSV to convert ProPublica names to legible school names
5. Exports the merged data to a CSV file
"""

import json
import os
import pandas as pd
from requests import get
from time import sleep
from tqdm import tqdm


def load_organizations(search_csv='sf_ptas.csv', extra_ptos_csv='extra_ptos.csv'):
    """
    Load EINs from search results and additional PTOs.

    Args:
        search_csv (str): Path to the PTA search results CSV from Script 1
        extra_ptos_csv (str): Path to CSV containing additional PTO EINs

    Returns:
        set: Set of EINs to query
    """
    eins = set()

    # Load EINs from search results
    if os.path.exists(search_csv):
        search_df = pd.read_csv(search_csv)
        eins.update(search_df['ein'].astype(str).tolist())
    else:
        print(f"Warning: {search_csv} not found. Run search_ptas.py first.")
        return eins

    # Load additional PTOs if file exists
    if os.path.exists(extra_ptos_csv):
        extra_df = pd.read_csv(extra_ptos_csv)
        if 'ein' in extra_df.columns:
            eins.update(extra_df['ein'].astype(str).tolist())
            print(f"Added {len(extra_df)} additional PTOs from {extra_ptos_csv}")

    print(f"Found {len(eins)} unique organizations to query")
    return eins


def pull_tax_data(eins, output_file='sf_pta_taxes.csv'):
    """
    Pull detailed tax information from ProPublica for each organization.

    Args:
        eins (set): Set of EINs to query
        output_file (str): Path to save the CSV output

    Returns:
        pd.DataFrame: DataFrame containing tax information for all organizations
    """
    base_url = 'https://projects.propublica.org/nonprofits/api/v2/organizations/'
    all_tax_data = pd.DataFrame()

    print("\nPulling tax data from ProPublica...")

    for ein in tqdm(eins):
        try:
            # Request organization data
            propublica_url = f'https://projects.propublica.org/nonprofits/organizations/{ein}'
            response = get(f'{base_url}/{ein}.json')
            data = json.loads(response.text)

            # Skip if no tax filings available
            if 'filings_with_data' not in data:
                org_name = data.get('organization', {}).get('name', 'Unknown')
                print(f"\nWarning: No filings_with_data for EIN {ein}")
                print(f"  Organization: {org_name}")
                print(f"  ProPublica URL: {propublica_url}")
                continue

            # Check if filings_with_data is empty
            if not data['filings_with_data']:
                org = data.get('organization', {})
                org_name = org.get('name', 'Unknown')
                print(f"\nWarning: Empty filings_with_data for EIN {ein}")
                print(f"  Organization: {org_name}")
                print(f"  ProPublica URL: {propublica_url}")
                continue

            org = data['organization']

            # Combine filing data with organization metadata
            filings_df = pd.DataFrame.from_records(data['filings_with_data'])

            # Check if filings DataFrame is empty
            if filings_df.empty:
                org_name = org.get('name', 'Unknown')
                print(f"\nWarning: Empty filings DataFrame for EIN {ein}")
                print(f"  Organization: {org_name}")
                print(f"  ProPublica URL: {propublica_url}")
                continue

            # Add organization-level fields (for the most recent filing)
            org_fields = {
                key: value
                for key, value in org.items()
                if key in ['tax_period', 'asset_amount', 'income_amount', 'revenue_amount']
            }
            org_df = pd.DataFrame.from_records([org_fields]) if org_fields else pd.DataFrame()

            # Concatenate filings with organization data
            org_tax_df = pd.concat([filings_df, org_df], axis=0, ignore_index=True)

            # Final check: ensure we have data before adding to all_tax_data
            if org_tax_df.empty:
                org_name = org.get('name', 'Unknown')
                print(f"\nWarning: Empty tax data for EIN {ein}")
                print(f"  Organization: {org_name}")
                print(f"  ProPublica URL: {propublica_url}")
                continue

            # Check if org_tax_df has nothing but NA entries
            if org_tax_df.isna().all().all():
                org_name = org.get('name', 'Unknown')
                print(f"\nWarning: Tax data contains only NA values for EIN {ein}")
                print(f"  Organization: {org_name}")
                print(f"  ProPublica URL: {propublica_url}")
                continue

            # Add organization identifiers to all rows
            org_tax_df['org_name'] = org['name']
            org_tax_df['org_subname'] = org.get('sub_name', org.get('sort_name', ''))
            org_tax_df['ein'] = org['ein']

            # Drop columns that are all-NA to avoid pandas FutureWarning
            org_tax_df = org_tax_df.dropna(axis=1, how='all')

            # Only concat if we have non-empty data
            all_tax_data = pd.concat([all_tax_data, org_tax_df], axis=0, ignore_index=True)

        except Exception as e:
            propublica_url = f'https://projects.propublica.org/nonprofits/organizations/{ein}'
            print(f"\nError processing EIN {ein}: {e}")
            print(f"  ProPublica URL: {propublica_url}")
            continue

        # Be respectful to the API
        sleep(1)

    # Save raw tax data
    all_tax_data.to_csv(output_file, index=False)
    print(f"\nTax data saved to {output_file}")

    return all_tax_data


def merge_with_school_names(tax_df, mapping_csv='ein_recodes.csv', extra_ptos_csv='extra_ptos.csv', output_file='sf_pta_taxes_merged.csv'):
    """
    Merge tax data with school name mappings.

    Args:
        tax_df (pd.DataFrame): DataFrame containing tax data
        mapping_csv (str): Path to CSV containing org_subname to school_name mappings
        extra_ptos_csv (str): Path to CSV containing EIN to associated_school mappings
        output_file (str): Path to save the merged CSV output

    Returns:
        pd.DataFrame: Merged DataFrame with school names
    """
    # Load mapping from CSV
    if not os.path.exists(mapping_csv):
        print(f"Warning: {mapping_csv} not found. Creating output without school name mapping.")
        tax_df.to_csv(output_file, index=False)
        return tax_df

    mapping_df = pd.read_csv(mapping_csv)

    # Merge tax data with school names based on org_subname
    # Keep only org_subname and school_name from mapping to avoid duplicate ein columns
    merged_df = tax_df.merge(
        mapping_df[['org_subname', 'school_name']],
        on='org_subname',
        how='left'
    )

    # Load extra PTOs and merge their associated schools based on EIN
    # IMPORTANT: extra_ptos.csv takes priority over ein_recodes.csv for EINs listed there
    if os.path.exists(extra_ptos_csv):
        extra_ptos_df = pd.read_csv(extra_ptos_csv)
        if 'associated_school' in extra_ptos_df.columns:
            # Create a mapping from EIN to associated_school
            ein_to_school = extra_ptos_df[['ein', 'associated_school']].copy()
            ein_to_school['ein'] = ein_to_school['ein'].astype(str)

            # Merge extra PTO school mappings
            merged_df['ein_str'] = merged_df['ein'].astype(str)
            merged_df = merged_df.merge(
                ein_to_school,
                left_on='ein_str',
                right_on='ein',
                how='left',
                suffixes=('', '_extra')
            )

            # For EINs in extra_ptos.csv, override with associated_school
            # For other EINs, use existing school_name if available, otherwise fill from associated_school
            mask_extra_pto = merged_df['associated_school'].notna()
            merged_df.loc[mask_extra_pto, 'school_name'] = merged_df.loc[mask_extra_pto, 'associated_school']

            # For non-extra PTOs, fill missing school_name values
            merged_df.loc[~mask_extra_pto, 'school_name'] = merged_df.loc[~mask_extra_pto, 'school_name'].fillna(
                merged_df.loc[~mask_extra_pto, 'associated_school']
            )

            # Clean up temporary columns
            merged_df = merged_df.drop(columns=['ein_str', 'ein_extra', 'associated_school'], errors='ignore')

    # Save merged data
    merged_df.to_csv(output_file, index=False)
    print(f"Merged data saved to {output_file}")

    # Print summary statistics
    total_orgs = merged_df['ein'].nunique()
    orgs_with_names = merged_df[merged_df['school_name'].notna()]['ein'].nunique()
    orgs_without_names = merged_df[merged_df['school_name'].isna()]['ein'].nunique()

    print(f"\nTotal organizations: {total_orgs}")
    print(f"Organizations with school names: {orgs_with_names}")
    print(f"Organizations without school names: {orgs_without_names}")

    return merged_df


def main(search_csv='sf_ptas.csv',
         extra_ptos_csv='extra_ptos.csv',
         mapping_csv='ein_recodes.csv',
         tax_output='sf_pta_taxes.csv',
         merged_output='sf_pta_taxes_merged.csv'):
    """
    Main function to orchestrate the tax data pulling and merging process.

    Args:
        search_csv (str): Path to PTA search results from Script 1
        extra_ptos_csv (str): Path to additional PTOs CSV
        mapping_csv (str): Path to org_subname to school_name mapping CSV
        tax_output (str): Path to save raw tax data
        merged_output (str): Path to save merged data with school names
    """
    # Load all organization EINs
    eins = load_organizations(search_csv, extra_ptos_csv)

    if not eins:
        print("No organizations to process. Exiting.")
        return None

    # Pull tax data from ProPublica
    tax_df = pull_tax_data(eins, tax_output)

    # Merge with school name mappings
    merged_df = merge_with_school_names(tax_df, mapping_csv, extra_ptos_csv, merged_output)

    return merged_df


if __name__ == '__main__':
    main()
