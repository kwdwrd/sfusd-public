# CCLD Child Care Scraper

This script scrapes the California Community Care Licensing Division child care search for San Francisco ZIP codes.

What it does
- Submits the search form at `https://www.ccld.dss.ca.gov/carefacilitysearch/Search/ChildCare`
- Sets **Facility Type** to **Child Care Center Preschool**
- Submits the form for each ZIP in San Francisco (configurable)
- For each facility it records (two-phase approach):
  - **Phase 1:** collect facility IDs from the search results for all ZIPs
  - **Phase 2:** for each unique facility ID, fetch details from `https://www.ccld.dss.ca.gov/carefacilitysearch/FacDetail/[facilityID]` and record:
    - facility name
    - facility ID
    - facility comment (from the View page Comments tab)
    - capacity from the most recent Visit summary (Visit tab)

Requirements
- Python 3.9+
- Playwright

Installation

1. Create a venv and activate it:

   python -m venv .venv
   source .venv/bin/activate

2. Install Playwright and browsers:

   pip install playwright
   python -m playwright install

Usage

Run the script:

  python scripts/scrape_ccld_cc.py --out data/ccld-sf.csv

Options
- `--zipcodes` : comma-separated zip codes (default is a built-in list of SF zip codes)
- `--ids-only` : only collect facility IDs from the search results and write to `--ids-out` (default: phase 1 and 2 run)
- `--from-csv PATH` : read facility IDs from `PATH` and spider details for those IDs (skips collection phase)
- `--ids-out` : path to write the CSV of collected IDs (default: `data/ccld-ids.csv`)
- The scraper now uses the CCLD transparency API for search: `https://www.ccld.dss.ca.gov/transparencyapi/api/FacilitySearch` with `facType=850` and `zip=[zip]` which is faster. The API returns facility records containing a `FacilityNumber`/`FACILITYNUMBER` field — the script now prefers these explicit fields for IDs and no longer scans arbitrary numeric values in the JSON to avoid picking street numbers or addresses.

For facility details the script now calls `https://www.ccld.dss.ca.gov/transparencyapi/api/FacilityDetail/[facilityID]` and extracts:
- `FacilityDetail.COMMENTS` → facility comments
- `VisitSummary` / `Visits` entries → most recent visit capacity if present
- `FacilityReports/[ID]` → `REPORTARRAY` listing available reports; each report can be fetched via `FacilityReports?facNum=[ID]&inx=[index]`. The script will attempt to extract a `REPORTDATE`, `REPORTTITLE`, and the report content; if a report contains a 'Capacity' value, it will be used as the facility capacity when visit data is not available.
If the API detail request fails, the script falls back to the HTML `FacDetail/[ID]` page for scraping.
- `--no-headless` : run browser visible for debugging

Notes & Caveats
- The CCLD site may change structure; the script uses multiple fallback selectors but may need updates if the HTML changes.
- Pagination and tab labels vary; if you get incomplete results, re-run with `--no-headless` and inspect the page to adjust selectors.
- This is provided for research/data collection; follow the site's terms of use and rate-limit requests.

Output
- CSV with columns: `zipcode, facility_name, facility_id, facility_comment, recent_capacity, detail_url`

If you want, I can add a small test run script or export to JSON/SQLite — tell me which format you prefer.