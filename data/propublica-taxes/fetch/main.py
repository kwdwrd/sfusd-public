"""
Main script to run the complete ProPublica tax data collection pipeline.

This script runs both data collection steps in sequence:
1. Search for PTAs in San Francisco (search_ptas.py)
2. Pull detailed tax information and merge with school names (pull_tax_data.py)
"""

import sys
from search_ptas import search_ptas
from pull_tax_data import main as pull_and_merge_tax_data


def main():
    """
    Run the complete tax data collection pipeline.
    """
    print("=" * 70)
    print("ProPublica Tax Data Collection Pipeline")
    print("=" * 70)

    # Step 1: Search for PTAs
    print("\n[STEP 1/2] Searching for PTAs in San Francisco...")
    print("-" * 70)
    try:
        search_results = search_ptas(output_file='sf_ptas.csv')
        print(f"Successfully found {len(search_results)} PTAs")
    except Exception as e:
        print(f"Error in Step 1: {e}")
        print("Exiting pipeline.")
        sys.exit(1)

    # Step 2: Pull tax data and merge with school names
    print("\n[STEP 2/2] Pulling tax data and merging with school names...")
    print("-" * 70)
    try:
        merged_data = pull_and_merge_tax_data(
            search_csv='sf_ptas.csv',
            extra_ptos_csv='extra_ptos.csv',
            mapping_csv='ein_recodes.csv',
            tax_output='sf_pta_taxes.csv',
            merged_output='sf_pta_taxes_merged.csv'
        )
        if merged_data is not None:
            print(f"Successfully processed {merged_data['ein'].nunique()} organizations")
        else:
            print("Warning: No data was processed in Step 2")
    except Exception as e:
        print(f"Error in Step 2: {e}")
        print("Exiting pipeline.")
        sys.exit(1)

    # Summary
    print("\n" + "=" * 70)
    print("Pipeline completed successfully!")
    print("=" * 70)
    print("\nOutput files created:")
    print("  - sf_ptas.csv              (PTA search results)")
    print("  - sf_pta_taxes.csv         (Raw tax data)")
    print("  - sf_pta_taxes_merged.csv  (Tax data with school names)")
    print()


if __name__ == '__main__':
    main()
