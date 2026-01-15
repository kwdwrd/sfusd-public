#!/usr/bin/env python3
"""Scrape CA Community Care Licensing Division child care search for SF ZIPs.

This script uses Playwright to submit the search form at
https://www.ccld.dss.ca.gov/carefacilitysearch/Search/ChildCare
with Facility Type set to "Child Care Center Preschool" and a ZIP code.

For each result (across pages) it records:
 - facility name
 - facility ID
 - facility comment (from the "View" page's Comments tab)
 - capacity from the most recent Visit summary (from the Visit tab)

Usage:
  pip install playwright
  python -m playwright install
  python scripts/scrape_ccld_cc.py --out data/ccld-sf.csv

Options:
  --headless/--no-headless  Run browser headless (default: headless)
  --zipcodes ZIP1,ZIP2,...  Comma-separated ZIP list to limit
"""

import argparse
import csv
import logging
import sys
import time
from pathlib import Path
from typing import List, Optional

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import re
import json

# Default SF ZIP codes. Adjust as needed.
SF_ZIPCODES = [
    "94102","94103","94104","94105","94107","94108","94109","94110",
    "94111","94112","94114","94115","94116","94117","94118","94121",
    "94122","94123","94124","94127","94129","94131","94132","94133",
    "94134","94158"
]

SEARCH_URL = "https://www.ccld.dss.ca.gov/carefacilitysearch/Search/ChildCare"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def ensure_element_text(locator, timeout=5000) -> Optional[str]:
    try:
        return locator.inner_text(timeout=timeout)
    except PlaywrightTimeout:
        return None


def extract_facility_from_row(row) -> dict:
    # Extract just the facility ID from a result row (we only need IDs from search results).
    facility_id = None
    try:
        txt = row.inner_text()
        link = row.query_selector('a')
        href = link.get_attribute('href') if link else ''
        # Try to find ID in href (e.g., /FacDetail/12345) first, then in text
        m = re.search(r"/FacDetail/([0-9]{3,10})", href)
        if not m:
            m = re.search(r"ID[:\s]*([0-9]{3,10})", txt)
        if m:
            facility_id = m.group(1)
        else:
            m2 = re.search(r"\b([0-9]{5,7})\b", txt)
            if m2:
                facility_id = m2.group(1)
    except Exception as e:
        logger.debug("extract row error: %s", e)

    return {"id": facility_id} 


def scrape_for_zip(page, zipcode: str) -> List[str]:
    """Collect facility IDs for a ZIP using the transparency API (single request; results are not paginated).

    API endpoint used:
    https://www.ccld.dss.ca.gov/transparencyapi/api/FacilitySearch?facType=850&facility=&Street=&city=&zip=[zip]&county=&facnum=
    """
    logger.info("API search for ZIP %s", zipcode)
    base = "https://www.ccld.dss.ca.gov/transparencyapi/api/FacilitySearch"
    results_set = set()

    # Try to use requests if available, otherwise fall back to urllib
    use_requests = True
    try:
        import requests
    except ModuleNotFoundError:
        use_requests = False
        import urllib.request, urllib.parse

    params = {
        'facType': '850',
        'facility': '',
        'Street': '',
        'city': '',
        'zip': zipcode,
        'county': '',
        'facnum': ''
    }

    # Single request with retries
    data = None
    for attempt in range(1, 4):
        try:
            # Log the full URL we are about to request
            try:
                url_str = base + '?' + (urllib.parse.urlencode(params) if not use_requests else '')
            except Exception:
                url_str = base
            logger.info('Requesting %s (attempt %d)', url_str or base, attempt)

            if use_requests:
                session = requests.Session()
                session.headers.update({
                    'User-Agent': 'sfusd-public-scraper/1.0 (+https://github.com/kwdwrd/sfusd-public)'
                })
                r = session.get(base, params=params, timeout=20)
                r.raise_for_status()
                data = r.json()
            else:
                qs = urllib.parse.urlencode(params)
                url = base + '?' + qs
                with urllib.request.urlopen(url, timeout=20) as resp:
                    data = json.load(resp)
            break
        except Exception as e:
            logger.warning('API request (attempt %d) failed for %s: %s', attempt, zipcode, e)
            time.sleep(0.5 * attempt)

    if data is None:
        logger.error('API request ultimately failed for %s', zipcode)
        return []

    # Find the array of result items
    items = None
    if isinstance(data, dict):
        for key in ('data', 'Data', 'items', 'Items', 'results', 'Results', 'd', 'facilities', 'Facilities', 'rows'):
            if key in data and isinstance(data[key], list):
                items = data[key]
                break
    if items is None and isinstance(data, list):
        items = data
    if items is None:
        # try to find the first list in the dict
        if isinstance(data, dict):
            for v in data.values():
                if isinstance(v, list):
                    items = v
                    break
    if not items:
        logger.debug('No items found for %s', zipcode)
        return []

    # Process items and extract facility IDs
    # Prefer explicit facility id fields (case-insensitive). Avoid generic numeric scanning to prevent picking street numbers.
    id_field_candidates = {'facilitynumber', 'facilitynum', 'facnum', 'facnum', 'facility_number', 'facilitynum', 'facilitynumber', 'facilitynum'}
    for it in items:
        if isinstance(it, dict):
            found_id = False
            for k, v in it.items():
                if k and k.lower() in id_field_candidates and v:
                    # Accept value as facility ID (string)
                    results_set.add(str(v))
                    found_id = True
                    break
            if not found_id:
                # No explicit id field found; log for diagnostics
                logger.debug('No facility id field in item: keys=%s', ','.join(sorted(it.keys())))
        else:
            # Non-dict items are unexpected; skip
            continue

    logger.info('Collected %d IDs for ZIP %s', len(results_set), zipcode)
    return list(results_set) 


def fetch_detail_via_api(facility_id) -> dict:
    """Fetch facility details via the transparency API and extract comments + recent capacity."""
    base = f"https://www.ccld.dss.ca.gov/transparencyapi/api/FacilityDetail/{facility_id}"
    logger.info("Fetching FacilityDetail JSON for %s", facility_id)

    # Try requests, otherwise urllib
    try:
        import requests
        use_requests = True
    except ModuleNotFoundError:
        use_requests = False
        import urllib.request

    data = None
    for attempt in range(1, 4):
        try:
            logger.info('Requesting %s (attempt %d)', base, attempt)
            if use_requests:
                r = requests.get(base, timeout=15, headers={'User-Agent': 'sfusd-public-scraper/1.0'})
                r.raise_for_status()
                data = r.json()
            else:
                with urllib.request.urlopen(base, timeout=15) as resp:
                    data = json.load(resp)
            break
        except Exception as e:
            logger.warning('FacilityDetail request (attempt %d) failed for %s: %s', attempt, facility_id, e)
            time.sleep(0.5 * attempt)
    if not data:
        logger.error('Failed to fetch FacilityDetail for %s', facility_id)
        return {}

    # Data usually contains a 'FacilityDetail' dict
    fd = None
    if isinstance(data, dict):
        for k in ('FacilityDetail', 'facilityDetail', 'Facility', 'facility'):
            if k in data and isinstance(data[k], dict):
                fd = data[k]
                break
        if fd is None:
            # sometimes the dict itself is the detail
            if any(k in data for k in ('FacilityNumber', 'FACILITYNUMBER')):
                fd = data

    if not fd:
        logger.debug('No FacilityDetail object found for %s', facility_id)
        return {}

    facility_name = None
    facility_comment = None
    recent_capacity = None

    # Facility name
    for name_key in ('FacilityName', 'FACILITYNAME', 'facilityName', 'facility'):
        if name_key in fd and fd[name_key]:
            facility_name = str(fd[name_key]).strip()
            break

    # Facility comments
    for ckey in ('COMMENTS', 'Comments', 'comments', 'FacilityComment', 'facilityComment'):
        if ckey in fd and fd[ckey]:
            facility_comment = str(fd[ckey]).strip()
            break

    # Visit summaries / recent capacity
    # Look for a list under 'VisitSummary', 'visits', 'Visits', 'VisitSummaries'
    visits = None
    for vkey in ('VisitSummary', 'VisitSummaries', 'Visit', 'Visits', 'visits', 'visitSummary'):
        if vkey in fd and isinstance(fd[vkey], list) and len(fd[vkey]) > 0:
            visits = fd[vkey]
            break

    if visits:
        # Try to find most recent by date field if available
        def parse_date(d):
            try:
                from datetime import datetime
                # handle common ISO-like or /Date() patterns
                if isinstance(d, (int, float)):
                    return datetime.fromtimestamp(d)
                if isinstance(d, str):
                    # remove /Date(...)\/ format
                    m = re.search(r'\d{4}-\d{2}-\d{2}', d)
                    if m:
                        return datetime.fromisoformat(m.group(0))
                    # fallback parse
                    return datetime.fromisoformat(d.split('T')[0])
            except Exception:
                return None
            return None

        best = None
        best_date = None
        for v in visits:
            # capacity could be in v['Capacity'] or in text
            cap = None
            for cap_key in ('Capacity', 'capacity', 'MaxCapacity'):
                if cap_key in v and v[cap_key]:
                    cap = str(v[cap_key]).strip()
                    break
            if not cap:
                # try to scan fields for 'Capacity:' text
                txt = json.dumps(v)
                m = re.search(r'Capacity[:\s]+([0-9,]+)', txt, re.I)
                if m:
                    cap = m.group(1)
            # parse date
            v_date = None
            for dkey in ('VisitDate', 'visitDate', 'Date', 'date', 'Created', 'created'):
                if dkey in v and v[dkey]:
                    v_date = parse_date(v[dkey])
                    if v_date:
                        break
            # Choose most recent date
            if v_date and cap:
                if best_date is None or v_date > best_date:
                    best = (cap, v_date)
                    best_date = v_date
            elif cap and best is None:
                best = (cap, None)
        if best:
            recent_capacity = best[0]

    # Fallback: scan fd for capacity anywhere
    if not recent_capacity:
        txt = json.dumps(fd)
        m = re.search(r'Capacity[:\s]+([0-9,]+)', txt, re.I)
        if m:
            recent_capacity = m.group(1)

    # Fetch reports via FacilityReports API
    reports = []
    reports_api = f"https://www.ccld.dss.ca.gov/transparencyapi/api/FacilityReports/{facility_id}"
    # try to fetch list of reports
    rdata = None
    for attempt in range(1, 3):
        try:
            logger.info('Requesting reports list %s (attempt %d)', reports_api, attempt)
            if use_requests:
                rr = requests.get(reports_api, timeout=15, headers={'User-Agent': 'sfusd-public-scraper/1.0'})
                rr.raise_for_status()
                rdata = rr.json()
            else:
                with urllib.request.urlopen(reports_api, timeout=15) as resp:
                    rdata = json.load(resp)
            break
        except Exception as e:
            logger.warning('FacilityReports list request (attempt %d) failed for %s: %s', attempt, facility_id, e)
            time.sleep(0.5 * attempt)
    if rdata and isinstance(rdata, dict) and 'REPORTARRAY' in rdata and isinstance(rdata['REPORTARRAY'], list):
        consecutive_empty = 0
        empty_cutoff = 3  # stop after this many consecutive empty/non-JSON responses
        for idx, rpt in enumerate(rdata['REPORTARRAY']):
            if consecutive_empty >= empty_cutoff:
                logger.info('Stopping report fetch early after %d consecutive empty responses for %s', empty_cutoff, facility_id)
                break
            if not isinstance(rpt, dict):
                logger.debug('Skipping non-dict report entry at index %d for %s', idx, facility_id)
                continue
            rep_date = rpt.get('REPORTDATE') or rpt.get('ReportDate') or rpt.get('reportdate')
            rep_title = rpt.get('REPORTTITLE') or rpt.get('ReportTitle') or rpt.get('reporttitle')
            report_url = f"https://www.ccld.dss.ca.gov/transparencyapi/api/FacilityReports?facNum={facility_id}&inx={idx}"
            # fetch the report content: prefer cached HTML file, otherwise JSON-first network fetch with HTML save fallback
            rep_content = None
            report_capacity = None
            report_dir = Path('data/ccld-reports') / str(facility_id)
            cache_path = report_dir / f'report-{idx}.html'

            # Use cached report if present to avoid re-fetching
            if cache_path.exists():
                try:
                    rep_content = cache_path.read_text(encoding='utf-8', errors='replace')
                    logger.info('Loaded cached report for %s idx %d from %s', facility_id, idx, cache_path)
                except Exception as e:
                    logger.warning('Failed to read cached report for %s idx %d: %s', facility_id, idx, e)

            # If no cached content, fetch from network (try JSON first, then text/HTML)
            if rep_content is None:
                for attempt in range(1, 3):
                    try:
                        logger.info('Requesting report %s (attempt %d)', report_url, attempt)
                        if use_requests:
                            rr = requests.get(report_url, timeout=15, headers={'User-Agent': 'sfusd-public-scraper/1.0'})
                            rr.raise_for_status()
                            # try JSON first
                            try:
                                rfull = rr.json()
                                json_parsed = True
                            except ValueError:
                                json_parsed = False
                                rtext = rr.text
                        else:
                            with urllib.request.urlopen(report_url, timeout=15) as resp:
                                raw = resp.read()
                                rtext = raw.decode('utf-8', errors='replace')
                            try:
                                rfull = json.loads(rtext)
                                json_parsed = True
                            except ValueError:
                                json_parsed = False
                        if json_parsed:
                            if isinstance(rfull, dict):
                                for k in ('REPORT', 'Report', 'report', 'REPORTTEXT', 'ReportText', 'reportText'):
                                    if k in rfull and rfull[k]:
                                        rep_content = str(rfull[k])
                                        break
                                if not rep_content:
                                    rep_content = json.dumps(rfull)
                            else:
                                rep_content = str(rfull)
                        else:
                            # non-JSON response: save HTML/text body and use as rep_content if non-empty
                            if rtext and rtext.strip():
                                rep_content = rtext
                                try:
                                    report_dir.mkdir(parents=True, exist_ok=True)
                                    fpath = report_dir / f'report-{idx}.html'
                                    with open(fpath, 'w', encoding='utf-8') as fh:
                                        fh.write(rtext)
                                    logger.info('Saved non-JSON report for %s idx %d to %s', facility_id, idx, fpath)
                                except Exception as e2:
                                    logger.warning('Failed to save report HTML for %s idx %d: %s', facility_id, idx, e2)
                            else:
                                rep_content = None
                        break
                    except Exception as e:
                        logger.warning('FacilityReports fetch (attempt %d) failed for %s idx %d: %s', attempt, facility_id, idx, e)
                        time.sleep(0.5 * attempt)
            # record consecutive empty responses to allow early cutoff
            if not rep_content:
                consecutive_empty += 1
            else:
                consecutive_empty = 0
            # extract capacity from report content
            if rep_content:
                m = re.search(r'Capacity[:\s]+([0-9,]+)', rep_content, re.I)
                if m:
                    report_capacity = m.group(1)
                    # prefer capacity from report if we don't have a visit-derived capacity
                    if not recent_capacity:
                        recent_capacity = report_capacity
            reports.append({
                'index': idx,
                'report_date': rep_date,
                'report_title': rep_title,
                'report_url': report_url,
                'report_capacity': report_capacity,
                'report_saved_path': str(cache_path) if cache_path.exists() else '',
                'report_text_snippet': (rep_content[:500] + '...') if rep_content and len(rep_content) > 500 else rep_content
            })

    return {"facility_name": facility_name, "facility_comment": facility_comment, "recent_capacity": recent_capacity, "detail_url": base, "reports": reports} 


def read_ids_from_csv(path):
    """Read facility IDs from a CSV file. Tries to find a 'facility_id' header otherwise scans all cells for numeric IDs."""
    ids = set()
    try:
        with open(path, newline='', encoding='utf-8') as fh:
            reader = csv.reader(fh)
            headers = next(reader, None)
            fid_idx = None
            if headers:
                for i, h in enumerate(headers):
                    if h and 'facility' in h.lower() and 'id' in h.lower():
                        fid_idx = i
                        break
            if fid_idx is not None:
                for row in reader:
                    if len(row) > fid_idx:
                        m = re.search(r'([0-9]{3,10})', row[fid_idx])
                        if m:
                            ids.add(m.group(1))
            else:
                for row in reader:
                    for cell in row:
                        m = re.search(r'([0-9]{3,10})', cell or '')
                        if m:
                            ids.add(m.group(1))
    except FileNotFoundError:
        logger.error('IDs CSV not found: %s', path)
    return ids


def write_ids_csv(id_map_or_set, out_path):
    with open(out_path, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.writer(fh)
        writer.writerow(['zipcodes', 'facility_id'])
        if isinstance(id_map_or_set, dict):
            for fid, zips in sorted(id_map_or_set.items()):
                writer.writerow([','.join(sorted(zips)), fid])
        else:
            for fid in sorted(id_map_or_set):
                writer.writerow(['', fid])


def run_all(zipcodes: List[str], out: Path, headless: bool = True, ids_only: bool = False, from_csv: Optional[str] = None, ids_out: Path = Path('data/ccld-ids.csv')):
    out.parent.mkdir(parents=True, exist_ok=True)
    ids_out.parent.mkdir(parents=True, exist_ok=True)
    header = ["zipcodes", "facility_name", "facility_id", "facility_comment", "recent_capacity", "detail_url", "report_count"]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()

        # If a CSV of IDs is supplied, skip collection phase
        if from_csv:
            ids_set = read_ids_from_csv(from_csv)
            id_map = {fid: set() for fid in ids_set}
            logger.info('Loaded %d IDs from %s', len(ids_set), from_csv)
        else:
            # First pass: collect unique facility IDs and record origin ZIPs
            id_map = {}  # facility_id -> set of zipcodes
            for z in zipcodes:
                ids = scrape_for_zip(page, z)
                logger.info("Found %d facility IDs for %s", len(ids), z)
                for fid in ids:
                    if not fid:
                        continue
                    id_map.setdefault(fid, set()).add(z)
                # Polite pause between ZIPs
                time.sleep(1)
            ids_set = set(id_map.keys())
            logger.info("Total unique facility IDs: %d", len(id_map))

        # Write collected IDs CSV
        write_ids_csv(id_map if isinstance(id_map, dict) else ids_set, ids_out)

        if ids_only:
            logger.info("IDs-only mode: wrote %d IDs to %s", len(id_map if isinstance(id_map, dict) else ids_set), ids_out)
            browser.close()
            return

        # Second pass: visit FacDetail/[ID] for each facility and record details
        with out.open('w', newline='', encoding='utf-8') as fh:
            writer = csv.writer(fh)
            writer.writerow(header)
            for fid in sorted(id_map.keys() if isinstance(id_map, dict) else list(ids_set)):
                # Prefer API-based detail fetch
                detail = fetch_detail_via_api(fid)
                if not detail:
                    # Fallback to HTML scraping if API fails
                    logger.info('API detail missing for %s, falling back to page scraping', fid)
                    detail = extract_detail_by_id(page, fid)
                writer.writerow([
                    ','.join(sorted(id_map.get(fid, set()) if isinstance(id_map, dict) else [])),
                    detail.get('facility_name') or '',
                    fid,
                    detail.get('facility_comment') or '',
                    detail.get('recent_capacity') or '',
                    detail.get('detail_url') or '',
                    len(detail.get('reports', [])) if detail else 0
                ])
                fh.flush()

        browser.close()


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--zipcodes', help='Comma-separated ZIP codes to run (default: SF zips)')
    ap.add_argument('--out', default='data/ccld-sf.csv', help='CSV output file for facility details')
    ap.add_argument('--ids-out', default='data/ccld-ids.csv', help='CSV output file for collected IDs (zipcodes,facility_id)')
    ap.add_argument('--ids-only', action='store_true', help='Only collect facility IDs from search results and write to --ids-out')
    ap.add_argument('--from-csv', help='Path to CSV containing facility IDs to spider (skips collection phase)')
    ap.add_argument('--no-headless', dest='headless', action='store_false', help='Run browser visible')
    ap.add_argument('--headless', dest='headless', action='store_true', help='Run browser headless (default)')
    ap.set_defaults(headless=True)
    return ap.parse_args()


def main():
    args = parse_args()
    zips = [z.strip() for z in args.zipcodes.split(',')] if args.zipcodes else SF_ZIPCODES
    out = Path(args.out)
    ids_out = Path(args.ids_out)
    run_all(zips, out, headless=args.headless, ids_only=args.ids_only, from_csv=args.from_csv, ids_out=ids_out)


if __name__ == '__main__':
    main()
