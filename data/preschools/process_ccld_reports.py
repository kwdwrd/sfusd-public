#!/usr/bin/env python3
"""Process saved CCLD FacilityReports HTML to extract capacities and merge into details CSV.

Usage:
  python scripts/process_ccld_reports.py --reports-dir data/ccld-reports --out data/ccld-reports-capacities.csv
  python scripts/process_ccld_reports.py --reports-dir data/ccld-reports --out data/ccld-reports-capacities.csv --merge data/ccld-details-full.csv --write-updated

The script attempts to use BeautifulSoup if available for reliable HTML parsing, otherwise falls back to regex scanning.
"""

import argparse
import csv
import logging
import re
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

try:
    from bs4 import BeautifulSoup
    HAVE_BS4 = True
except Exception:
    HAVE_BS4 = False

CAPACITY_RE = re.compile(r'Capacity[:\s]*([0-9,]+)', re.I)
GENERIC_NUM_RE = re.compile(r'([0-9]{1,3}(?:,[0-9]{3})*)')
# HTML-aware patterns that try to match numeric content inside tags following labels (avoids matching numbers in attributes like font size)
CAPACITY_HTML_RE = re.compile(r'CAPACITY[^>]{0,200}>(?:[^>]*>)*\s*([0-9]{1,4}(?:,[0-9]{3})*)', re.I)
ENROLLED_HTML_RE = re.compile(r'TOTAL\s+ENROLLED(?:\s+CHILDREN)?[^>]{0,200}>(?:[^>]*>)*\s*([0-9]{1,4}(?:,[0-9]{3})*)', re.I)


def parse_capacity_from_html(html: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Return (capacity, total_enrolled, method, snippet, report_date).

    - capacity, total_enrolled: digits-only strings (commas removed)
    - method: one of 'licensed', 'max', 'label', 'table', 'regex', 'enrolled', 'html-capacity', etc.
    - snippet: short surrounding HTML/text used for the match
    - report_date: ISO date string (YYYY-MM-DD) when found, otherwise None

    Methods: 'licensed', 'max', 'label', 'table', 'regex', 'enrolled'
    """
    # Use BeautifulSoup when available for structured lookups
    total_enrolled = None
    report_date = None

    def _find_date_in_text(txt: str) -> Optional[str]:
        if not txt:
            return None
        mdate = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', txt)
        if mdate:
            try:
                return datetime.strptime(mdate.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
            except Exception:
                return mdate.group(1)
        return None

    if HAVE_BS4:
        soup = BeautifulSoup(html, 'html.parser')
        whole = soup.get_text(' ', strip=True)

        # 0) Prefer explicit 'LICENSED' or 'MAXIMUM' capacity phrases
        # m = re.search(r'LICENSED\s+(?:FOR|TO\s+SERVE)[:\s]*([0-9]{1,3}(?:,[0-9]{3})*)', whole, re.I)
        # if m:
        #     print( 'in clause 1' )
        #     cap = m.group(1).replace(',', '')
        #     report_date = _find_date_in_text(whole)
        #     return cap, None, 'licensed', whole[m.start():m.start()+200], report_date
        # m = re.search(r'(?:MAXIMUM\s+CAPACITY|MAX\.|MAXIMUM)[:\s]*([0-9]{1,3}(?:,[0-9]{3})*)', whole, re.I)
        # if m:
        #     print( 'in clause 2' )
        #     cap = m.group(1).replace(',', '')
        #     report_date = _find_date_in_text(whole)
        #     return cap, None, 'max', whole[m.start():m.start()+200], report_date

        # 0b) Look for explicit 'TOTAL ENROLLED' phrases
        me = re.search( r'CAPACITY[:\s]*([0-9]{1,4}(?:,[0-9]{3})*)', whole, re.I )
        if me:
            cap = me.group(1).replace(',', '')

        me = re.search(r'TOTAL\s+ENROLLED(?:\s+CHILDREN)?[:\s]*([0-9]{1,4}(?:,[0-9]{3})*)', whole, re.I)
        if me:
            total_enrolled = me.group(1).replace(',', '')

        me = re.search(r'DATE[:\s]*(\d{2})\/(\d{2})\/(\d{4})', whole, re.I )
        # keep looking for a licensed/max capacity first; but if none, we may return enrolled later
        if me:
            date_groups = me.groups()
            report_date = f'{date_groups[2]}-{date_groups[0]}-{date_groups[1]}'
        return ( cap, total_enrolled, 'regex', whole[me.start():me.start()+200], report_date ) if cap else ( None, total_enrolled, 'regex-enrolled', whole[me.start():me.start()+200], report_date )

        # 1) Look for table rows where first cell contains capacity-like labels or enrolled labels
        for tr in soup.find_all('tr'):
            tds = tr.find_all(['td', 'th'])
            if len(tds) >= 2:
                left = tds[0].get_text(' ', strip=True).lower()
                right = tds[1].get_text(' ', strip=True)
                if any(k in left for k in ('capacity', 'licensed', 'maximum', 'licensed for', 'licensed to serve', 'maximum capacity')):
                    m = re.search(r'([0-9]{1,4}(?:,[0-9]{3})*)', right)
                    if m:
                        cap = m.group(1).replace(',', '')
                        # try to find DATE in this row
                        report_date = None
                        try:
                            report_date = _find_date_in_text(tr.get_text(' ', strip=True))
                        except Exception:
                            report_date = None
                        return cap, total_enrolled, 'table', right[:200], report_date
                if 'total enrolled' in left or 'total enrolled children' in left or 'total enrolled' in left:
                    m2 = re.search(r'([0-9]{1,4}(?:,[0-9]{3})*)', right)
                    if m2:
                        en = m2.group(1).replace(',', '')
                        report_date = None
                        try:
                            report_date = _find_date_in_text(tr.get_text(' ', strip=True))
                        except Exception:
                            report_date = None
                        return None, en, 'table-enrolled', right[:200], report_date

        # 2) Search for labeled text nodes containing 'capacity' or 'enrolled'
        for txt in soup.find_all(text=re.compile(r'(capacity|licensed|total enrolled|enrolled)', re.I)):
            parent = txt.parent
            full = parent.get_text(' ', strip=True)

            # prefer explicit 'capacity' nearby
            m = re.search(r'capacity[:\s]*([0-9]{1,4}(?:,[0-9]{3})*)', full, re.I)
            if m:
                cap = m.group(1).replace(',', '')
                report_date = _find_date_in_text(parent.find_parent('tr').get_text(' ', strip=True) if parent.find_parent('tr') else parent.get_text(' ', strip=True))
                return cap, total_enrolled, 'label', full[:200], report_date

            # explicit 'total enrolled'
            me2 = re.search(r'total\s+enrolled(?:\s+children)?[:\s]*([0-9]{1,4}(?:,[0-9]{3})*)', full, re.I)
            if me2:
                en = me2.group(1).replace(',', '')
                report_date = _find_date_in_text(parent.find_parent('tr').get_text(' ', strip=True) if parent.find_parent('tr') else parent.get_text(' ', strip=True))
                return None, en, 'label-enrolled', full[:200], report_date

            # sibling checks
            sib = parent.find_next_sibling()
            if sib:
                stext = sib.get_text(' ', strip=True)
                m3 = re.search(r'([0-9]{1,4}(?:,[0-9]{3})*)', stext)
                if m3:
                    num = m3.group(1).replace(',', '')
                    # heuristics: if text mentions 'enroll' treat as enrolled
                    if re.search(r'enroll', parent.get_text(' ', strip=True), re.I) or re.search(r'enroll', stext, re.I):
                        report_date = _find_date_in_text(parent.find_parent('tr').get_text(' ', strip=True) if parent.find_parent('tr') else parent.get_text(' ', strip=True))
                        return None, num, 'label-sibling-enrolled', stext[:200], report_date
                    else:
                        report_date = _find_date_in_text(parent.find_parent('tr').get_text(' ', strip=True) if parent.find_parent('tr') else parent.get_text(' ', strip=True))
                        return num, total_enrolled, 'label-sibling', stext[:200], report_date

        # 3) fallback: search entire document text for our existing capacity regex or total enrolled patterns
        m = CAPACITY_RE.search(whole)
        if m:
            cap = m.group(1).replace(',', '')
            report_date = _find_date_in_text(whole)
            return cap, total_enrolled, 'regex', whole[m.start():m.start()+200], report_date
        # try nearby 'capacity' keyword
        m2 = re.search(r'capacity.{0,30}([0-9]{1,4}(?:,[0-9]{3})*)', whole, re.I)
        if m2:
            cap = m2.group(1).replace(',', '')
            report_date = _find_date_in_text(whole)
            return cap, total_enrolled, 'regex-near', whole[m2.start():m2.start()+200], report_date
        # total enrolled fallback
        if total_enrolled:
            report_date = _find_date_in_text(whole)
            return None, total_enrolled, 'regex-enrolled', whole[me.start():me.start()+200], report_date

    # If BeautifulSoup not available or above failed, do regex scan on raw HTML
    # Prefer HTML-aware patterns that look for numeric content inside tags following label text
    m_html = CAPACITY_HTML_RE.search(html)
    if m_html:
        cap = m_html.group(1).replace(',', '')
        snippet = html[m_html.start():m_html.start()+400]
        report_date = _find_date_in_text(snippet)
        return cap, None, 'html-capacity', snippet, report_date
    me_html = ENROLLED_HTML_RE.search(html)
    if me_html:
        enrolled = me_html.group(1).replace(',', '')
        snippet = html[me_html.start():me_html.start()+400]
        report_date = _find_date_in_text(snippet)
        return None, enrolled, 'html-enrolled', snippet, report_date

    # Fallback to older, less strict regexes
    m = re.search(r'LICENSED\s+(?:FOR|TO\s+SERVE)[:\s]*([0-9]{1,4}(?:,[0-9]{3})*)', html, re.I)
    if m:
        snippet = html[m.start():m.start()+400]
        report_date = _find_date_in_text(snippet)
        return m.group(1).replace(',', ''), None, 'licensed', snippet, report_date
    m = re.search(r'(?:MAXIMUM\s+CAPACITY|MAX\.|MAXIMUM)[:\s]*([0-9]{1,4}(?:,[0-9]{3})*)', html, re.I)
    if m:
        snippet = html[m.start():m.start()+400]
        report_date = _find_date_in_text(snippet)
        return m.group(1).replace(',', ''), None, 'max', snippet, report_date

    # prefer total enrolled phrases
    me = re.search(r'TOTAL\s+ENROLLED(?:\s+CHILDREN)?[:\s]*([0-9]{1,4}(?:,[0-9]{3})*)', html, re.I)
    if me:
        snippet = html[me.start():me.start()+400]
        report_date = _find_date_in_text(snippet)
        return None, me.group(1).replace(',', ''), 'enrolled', snippet, report_date

    m = CAPACITY_RE.search(html)
    if m:
        cap = m.group(1).replace(',', '')
        snippet = html[m.start():m.start()+200]
        report_date = _find_date_in_text(snippet)
        return cap, None, 'regex', snippet, report_date
    m2 = re.search(r'capacity.{0,30}([0-9]{1,4}(?:,[0-9]{3})*)', html, re.I)
    if m2:
        cap = m2.group(1).replace(',', '')
        snippet = html[m2.start():m2.start()+200]
        report_date = _find_date_in_text(snippet)
        return cap, None, 'regex-near', snippet, report_date

    return None, None, None, None, None


def scan_reports(reports_dir: Path, out_csv: Path) -> dict:
    """Scan report HTML files and write capacities to out_csv. Return mapping facility_id -> first-found capacity.
    """
    rows = []
    capacities = {}
    for facil_dir in sorted(reports_dir.iterdir()):
        if not facil_dir.is_dir():
            continue
        facility_id = facil_dir.name

        for report_file in sorted(facil_dir.glob('report-*.html')):
            try:
                html = report_file.read_text(encoding='utf-8', errors='replace')
            except Exception as e:
                logger.warning('Failed to read %s: %s', report_file, e)
                continue
            cap, enrolled, method, snippet, report_date = parse_capacity_from_html(html)
            rows.append({
                'facility_id': facility_id,
                'report_path': str(report_file),
                'report_date': report_date or '',
                'capacity': cap or '',
                'total_enrolled': enrolled or '',
                'method': method or '',
                'snippet': (snippet or '').replace('\n', ' ')
            })
            if facility_id not in capacities and (cap or enrolled):
                capacities[facility_id] = {'capacity': cap, 'total_enrolled': enrolled, 'report_date': report_date}
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open('w', newline='', encoding='utf-8') as fh:
        w = csv.DictWriter(fh, fieldnames=['facility_id', 'report_path', 'report_date', 'capacity', 'total_enrolled', 'method', 'snippet'])
        w.writeheader()
        for r in rows:
            w.writerow(r)
    logger.info('Wrote %d report rows with capacities to %s (facilities with capacity/enrollment info: %d)', len(rows), out_csv, len(capacities))
    return capacities


def merge_into_details(details_csv: Path, capacities_map: dict, out_csv: Optional[Path] = None, write_updated: bool = False, force: bool = False):
    """Merge first-found capacities into details CSV where recent_capacity is empty.

    If write_updated is True, overwrite details_csv with updated contents (after making a backup).
    Returns count of updated rows.
    """
    updated = 0
    rows = []

    with details_csv.open(newline='', encoding='utf-8') as fh:
        r = csv.DictReader(fh)
        rows = list(r)
        headers = r.fieldnames

    # ensure we have columns for recent_capacity, total_enrolled and recent_report_date
    if 'recent_capacity' not in headers:
        headers = headers + ['recent_capacity']
    if 'total_enrolled' not in headers:
        headers = headers + ['total_enrolled']
    if 'recent_report_date' not in headers:
        headers = headers + ['recent_report_date']

    for row in rows:
        fid = row.get('facility_id') or row.get('facility') or row.get('facility_id')
        if not fid:
            continue
        entry = capacities_map.get(fid) if isinstance(capacities_map.get(fid), dict) else None
        # Backwards compatible: if capacities_map stored strings (old behavior), handle it
        if entry is None and fid in capacities_map and not isinstance(capacities_map[fid], dict):
            entry = {'capacity': capacities_map[fid], 'total_enrolled': None}

        if entry:
            cap = entry.get('capacity')
            enrolled = entry.get('total_enrolled')
            # Determine whether to overwrite recent_capacity (if force=True, or if empty)
            cur_cap = row.get('recent_capacity')
            if force or (not cur_cap or not cur_cap.strip()):
                # prefer explicit capacity, else use total_enrolled
                if cap:
                    row['recent_capacity'] = cap
                    row['recent_report_date'] = entry.get('report_date') or ''
                    updated += 1
                elif enrolled:
                    row['recent_capacity'] = enrolled
                    row['recent_report_date'] = entry.get('report_date') or ''
                    updated += 1
            # populate total_enrolled column if empty or if forcing
            cur_enrolled = row.get('total_enrolled')
            if enrolled and (force or not cur_enrolled or not cur_enrolled.strip()):
                row['total_enrolled'] = enrolled

    if write_updated:
        bak = details_csv.with_suffix(details_csv.suffix + '.bak')
        details_csv.replace(bak)
        with details_csv.open('w', newline='', encoding='utf-8') as fh:
            w = csv.DictWriter(fh, fieldnames=headers)
            w.writeheader()
            for r in rows:
                w.writerow(r)
        logger.info('Merged capacities into %s (backup at %s) - updated %d rows', details_csv, bak, updated)
    else:
        logger.info('Would update %d rows in %s (run with --write-updated to apply)', updated, details_csv)

    return updated


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--reports-dir', default='data/ccld-reports', help='Directory with saved report HTML by facility id')
    ap.add_argument('--out', default='data/ccld-reports-capacities.csv', help='Output CSV of per-report capacities')
    ap.add_argument('--merge', help='Path to details CSV to merge capacities into')
    ap.add_argument('--write-updated', action='store_true', help='If set, update the details CSV in place (backup is created)')
    ap.add_argument('--force', action='store_true', help='If set, overwrite existing recent_capacity values with extracted values')
    args = ap.parse_args()

    reports_dir = Path(args.reports_dir)
    out_csv = Path(args.out)

    if not reports_dir.exists():
        logger.error('Reports directory not found: %s', reports_dir)
        return

    if not HAVE_BS4:
        logger.warning('BeautifulSoup not available; HTML parsing will fall back to regex heuristics. To improve results, pip install beautifulsoup4')

    capacities = scan_reports(reports_dir, out_csv)

    if args.merge and Path(args.merge).exists():
        merge_into_details(Path(args.merge), capacities, out_csv, write_updated=args.write_updated, force=args.force)


if __name__ == '__main__':
    main()
