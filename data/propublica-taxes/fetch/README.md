# ProPublica Tax Data Collection

This directory contains scripts to collect tax information for San Francisco public school PTAs and PTOs from the ProPublica NonProfit Explorer API.

## Overview

The data collection process is split into two scripts:

1. **`search_ptas.py`** - Searches for all PTAs in San Francisco
2. **`pull_tax_data.py`** - Pulls detailed tax information and merges with school names

A **`main.py`** script is provided to run the complete pipeline in one command.

## Requirements

```bash
pip install pandas requests tqdm
```

## Quick Start

Run the complete pipeline with a single command:

```bash
python main.py
```

This will execute both scripts in sequence and create all output files.

## Manual Usage

### Step 1: Search for PTAs

```bash
python search_ptas.py
```

This will create `sf_ptas.csv` containing basic information about all PTAs found.

### Step 2: Pull Tax Data

```bash
python pull_tax_data.py
```

This will:
- Load the PTA search results from `sf_ptas.csv`
- Add additional PTOs from `extra_ptos.csv`
- Query the ProPublica API for detailed tax information
- Merge with school name mappings from `ein_recodes.csv`
- Create two output files:
  - `sf_pta_taxes.csv` - Raw tax data
  - `sf_pta_taxes_merged.csv` - Tax data merged with school names

## Configuration Files

### ein_recodes.csv
Maps ProPublica organization names to standardized SFUSD school names for organizations found via PTA search. This mapping handles:
- Abbreviations (e.g., "Pta" → full school names)
- Naming inconsistencies
- School code prefixes in organization names

This CSV has three columns:
- `ein` - Employer Identification Number
- `org_subname` - ProPublica organization sub-name (from API search results)
- `school_name` - Standardized SFUSD school name

**Important**: Do NOT include EINs that are listed in `extra_ptos.csv` - those take priority and should only be in `extra_ptos.csv`.

To add more mappings, add rows to this CSV with all three columns. The EIN can be found from the ProPublica data or IRS records.

### extra_ptos.csv
Contains parent organizations NOT found in the PTA API search:
- Parent Advisory Council Japanese Bilingual Bicultural Program (Parks (Rosa) ES)
- Parents Club West Portal School (West Portal ES)
- Argonne Council Of Empowerment (Argonne ES)

This CSV has three columns:
- `ein` - Employer Identification Number
- `org_name` - Organization name
- `associated_school` - SFUSD school name associated with this organization

**Priority**: Organizations listed in this file take priority over `ein_recodes.csv`. If an EIN appears in both files, the school name from `extra_ptos.csv` will be used. This is important because these organizations may not follow standard PTA naming patterns in ProPublica's database.

To add more organizations, add rows to this CSV with all three columns.

## Output Files

### sf_ptas.csv
Basic organization information from the search:
- `ein` - Employer Identification Number
- `name` - Organization name
- `city` - City (filtered to San Francisco)
- Other organizational metadata

### sf_pta_taxes.csv
Detailed tax filing information:
- Multiple years of tax filings per organization
- Revenue, expenses, assets, liabilities
- Various tax form fields (Form 990)
- Organization identifiers (`org_name`, `org_subname`, `ein`)

### sf_pta_taxes_merged.csv
Same as `sf_pta_taxes.csv` but with an additional column:
- `school_name` - Standardized SFUSD school name mapped from ProPublica organization names

## API Notes

- The ProPublica NonProfit Explorer API does not require authentication
- The scripts include 1-second delays between requests to be respectful to the API
- API documentation: https://projects.propublica.org/nonprofits/api

## Legacy Files

- `propublica-explorer.ipynb` - Original Jupyter notebook (kept for reference)
- These Python scripts replace the notebook functionality
