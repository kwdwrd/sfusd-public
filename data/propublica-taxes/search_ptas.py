"""
Script 1: Search ProPublica API for PTAs in San Francisco

This script queries the ProPublica NonProfit API to search for all PTA organizations
in California and filters results to only include those in San Francisco.
Results are exported to a CSV file.
"""

import json
import pandas as pd
from requests import get
from time import sleep
from tqdm import tqdm


def search_ptas(output_file='sf_ptas.csv'):
    """
    Search for PTAs in San Francisco using the ProPublica API.

    Args:
        output_file (str): Path to save the CSV output

    Returns:
        pd.DataFrame: DataFrame containing PTA organization information
    """
    # Search for California PTA organizations
    pta_name = 'Pta California Congress Of Parents Teachers & Students Inc'
    pta_url = f'https://projects.propublica.org/nonprofits/api/v2/search.json?q={pta_name}&state[id]=CA'

    page_num = 0
    sf_ptas = []
    total_pages = None

    print("Searching for PTAs in San Francisco...")
    progress_bar = tqdm()

    while True:
        # Make API request for current page
        response = get(f'{pta_url}&page={page_num}')
        data = json.loads(response.text)

        # Initialize progress bar on first page
        if page_num == 0:
            total_pages = int(data['num_pages'])
            progress_bar.reset(total=total_pages)

        progress_bar.update(1)

        # Filter for San Francisco organizations only
        sf_orgs = [org for org in data['organizations'] if org['city'] == 'San Francisco']
        sf_ptas.extend(sf_orgs)

        page_num += 1

        # Check if we've reached the last page
        if page_num >= total_pages:
            break

        # Be respectful to the API - wait 1 second between requests
        sleep(1)

    progress_bar.close()

    # Convert to DataFrame and save
    df = pd.DataFrame.from_records(sf_ptas)
    df.to_csv(output_file, index=False)

    print(f"\nFound {len(sf_ptas)} PTAs in San Francisco")
    print(f"Results saved to {output_file}")

    return df


if __name__ == '__main__':
    search_ptas()
