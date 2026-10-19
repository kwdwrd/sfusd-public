#!/usr/bin/env python3
"""
SFUSD BoardDocs contract scraper — Step 1.

Fetches meeting agendas from BoardDocs, identifies contract-related items,
and writes contracts_raw.csv. Also caches raw HTML under cache/ for Step 2.

Usage:
    python scraper.py                  # scrapes 2023, 2024, 2025
    python scraper.py 2022 2023 2024   # scrapes specific years
"""

import csv
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ── BoardDocs endpoints ────────────────────────────────────────────────────────

BASE_URL = "https://go.boarddocs.com/ca/sfusd/Board.nsf"
COMMITTEE_ID = "A4EP6J588C05"  # SFUSD Board of Education committee

HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "sec-ch-ua": '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "x-requested-with": "XMLHttpRequest",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/113.0.0.0 Safari/537.36"
    ),
    "Referer": f"{BASE_URL}/Public",
}

# ── Contract signal patterns (matched against item subject line) ───────────────

CONTRACT_SUBJECT_RE = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bcontracts?\b",                          # "Contracts, Orders for Service…" / "Ratification of…Contracts"
        r"\borders?\s+for\s+service\b",
        r"\bwork\s+orders?\b",
        r"\bmodification\s+(?:#\s*\d+\s+to|to\s+(?:master\s+)?(?:agreement|contract))",
        r"\bmaster\s+(?:contract|agreement|service\s+agreement)\b",
        r"\bmemorandum\s+of\s+understanding\b",
        r"\bMOU\b",
        r"\bpurchase\s+order\b",
        r"\bindependent\s+contractor\b",
        r"\bprofessional\s+(?:services?|consulting)\s+agreement\b",
        r"\bsoftware\s+licen[sc]e\b",
        r"\bsubscription\s+agreement\b",
        r"object\s*code\s*5800",
    ]
]

AMOUNT_RE = re.compile(r"\$\s*([\d,]+(?:\.\d{1,2})?)")

# Vendor after em-dash or hyphen-dash at end of subject: "… – Bana Builders"
# Stops before a trailing date suffix like " - 10/14/2025"
VENDOR_DASH_RE = re.compile(r"[–—-]\s*([A-Z][A-Za-z0-9\s,\.&\/\-\+\']{2,60}?)(?:\s*-\s*\d|\s*$)")

# "Master Contract/Agreement with <Vendor> [and SFUSD|for|,|- date]"
VENDOR_MASTER_RE = re.compile(
    r"(?:Master\s+(?:Contract|Agreement|Service\s+Agreement)|MOU)\s+with\s+"
    r"([A-Z][A-Za-z0-9\s,\.&\/\-\+\']{2,80}?)"
    r"(?:\s+and\s+SFUSD|\s+for\s+|\s*,|\s*-\s*\d|\s*$)",
    re.IGNORECASE,)

VENDOR_WITH_RE = re.compile(
    r"(?:with|to|from|by|vendor[:\s]+|contractor[:\s]+|agreement\s+with)\s+"
    r"([A-Z][A-Za-z0-9\s,\.&\-\']{2,60}?)"
    r"(?:\s+(?:for|to provide|in the amount|without|pursuant|and |,|\.)|$)",
    re.IGNORECASE,
)

FUNDING_RE = re.compile(
    r"((?:Title\s+[IVX]+|ESSER\s*(?:I+|III)?|LCFF|General\s+Fund|"
    r"Proposition\s+[A-Z\d]+|Measure\s+[A-Z]+|federal\s+funds?|state\s+funds?|"
    r"[A-Z][A-Za-z\s]+\s+Grant|Prop(?:osition)?\s+[A-Z\d]+\s+(?:Bond|Fund)[^,.\n]{0,50})"
    r"[^,.\n]{0,80})",
    re.IGNORECASE,
)

PDF_LINK_RE = re.compile(r'href=["\']([^"\']*\.pdf[^"\']*)["\']', re.IGNORECASE)

# ── Output paths ───────────────────────────────────────────────────────────────

DIR = Path(__file__).parent
CACHE_DIR = DIR / "cache"
RAW_CSV = DIR / "contracts_raw.csv"

FIELDNAMES = [
    "date", "meeting_id", "item_title",
    "vendor_name", "amount_usd", "funding_source",
    "item_description", "pdf_urls",
]

# ── API calls ──────────────────────────────────────────────────────────────────

def get_all_meetings() -> list[dict]:
    url = f"{BASE_URL}/BD-GetMeetingsList?open"
    resp = requests.post(url, headers=HEADERS, data=f"current_committee_id={COMMITTEE_ID}", timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_agenda_html(meeting_id: str) -> str:
    cache_path = CACHE_DIR / f"{meeting_id}.html"
    if cache_path.exists():
        return cache_path.read_text(encoding="utf-8")

    url = f"{BASE_URL}/PRINT-AgendaDetailed"
    body = f"id={meeting_id}&current_committee_id={COMMITTEE_ID}"
    resp = requests.post(url, headers=HEADERS, data=body, timeout=60)
    resp.raise_for_status()
    html = resp.text

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(html, encoding="utf-8")
    return html

# ── Extraction helpers ─────────────────────────────────────────────────────────

def is_contract_subject(subject: str) -> bool:
    return any(p.search(subject) for p in CONTRACT_SUBJECT_RE)


def get_item_fields(item_el) -> dict[str, str]:
    """Extract all dl/dt/dd structured fields from a div.item element."""
    fields: dict[str, str] = {}
    for dl in item_el.find_all("dl", class_="row"):
        dt = dl.find("dt")
        dd = dl.find("dd")
        if dt and dd:
            fields[dt.get_text(strip=True)] = dd.get_text(strip=True)
    return fields


_DATE_RE = re.compile(r"^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d", re.IGNORECASE)

def _valid_vendor(s: str) -> bool:
    s = s.strip()
    return bool(s) and s[0].isupper() and not _DATE_RE.match(s)

def extract_vendor(subject: str, recommended_action: str) -> str:
    # 1. After the last em-dash/hyphen: "… – Bana Builders"
    m = VENDOR_DASH_RE.search(subject)
    if m and _valid_vendor(m.group(1)):
        return m.group(1).strip()
    # 2. "Master Contract/MOU with <Vendor> [and SFUSD|for|,]"
    m = VENDOR_MASTER_RE.search(subject)
    if m and _valid_vendor(m.group(1)):
        return m.group(1).strip()
    # 3. "with/from/by <Vendor>" in recommended action
    m = VENDOR_WITH_RE.search(recommended_action)
    if m and _valid_vendor(m.group(1)):
        return m.group(1).strip()
    return ""


def extract_amount(dollar_amount_field: str, full_text: str) -> str:
    # Prefer the structured Dollar Amount field
    if dollar_amount_field:
        m = AMOUNT_RE.search(dollar_amount_field)
        if m:
            try:
                return f"${float(m.group(1).replace(',', '')):,.2f}"
            except ValueError:
                pass
    # Fall back to largest dollar value in full text
    amounts = []
    for raw in AMOUNT_RE.findall(full_text):
        try:
            amounts.append(float(raw.replace(",", "")))
        except ValueError:
            pass
    return f"${max(amounts):,.2f}" if amounts else ""


def extract_funding(budget_source: str, full_text: str) -> str:
    # Prefer the structured Budget Source field
    if budget_source:
        return budget_source[:200]
    m = FUNDING_RE.search(full_text)
    return m.group(1).strip() if m else ""


def extract_pdf_urls(html_fragment: str) -> list[str]:
    urls = []
    for href in PDF_LINK_RE.findall(html_fragment):
        if href.startswith("http"):
            urls.append(href)
        else:
            urls.append(urljoin(BASE_URL + "/", href.lstrip("/")))
    return list(dict.fromkeys(urls))  # deduplicate, preserve order

# ── Batch ratification attachment parsing ──────────────────────────────────────

# Matches consent-calendar batch items that list contracts by attachment
BATCH_SUBJECT_RE = re.compile(
    r"Ratification of the attached|"
    r"Contracts\s+(?:over|under)\s+\$|"
    r"Approve\s+Fiscal\s+Year.*Contracts\s+(?:over|under)",
    re.IGNORECASE,
)

# Filename format: [optional contract#] Vendor (Dept/School) $amount [REQ/PO junk]
FILENAME_RE = re.compile(
    r"^(?:\d+\s*)?"                          # optional leading contract number
    r"([^($]+)\s*"                           # vendor name — stops before ( or $
    r"(?:\(([^)]+)\)\s*)?"                  # optional (dept/school)
    r".*?\$\s*([\d,]+(?:\.\d{1,2})?)",      # $ amount
)

from urllib.parse import unquote as _unquote

def parse_attachment_filename(filename: str) -> dict | None:
    name = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE).strip()
    m = FILENAME_RE.match(name)
    if not m:
        return None
    vendor = m.group(1).strip().rstrip("–—-").strip()
    dept   = m.group(2).strip() if m.group(2) else ""
    try:
        amount = f"${float(m.group(3).replace(',', '')):,.2f}"
    except ValueError:
        amount = ""
    return {"vendor_name": vendor, "funding_source": dept, "amount_usd": amount}


def expand_batch_item(item_el, meeting_id: str, date_str: str, subject: str) -> list[dict]:
    """Return one record per PDF attachment for a batch ratification item."""
    records = []
    for a in item_el.find_all("a", href=True):
        href = a["href"]
        if "/$file/" not in href and "$file" not in href:
            continue
        raw_filename = href.split("$file/")[-1]
        filename = _unquote(raw_filename)
        parsed = parse_attachment_filename(filename)
        if not parsed:
            continue
        url = href if href.startswith("http") else urljoin(BASE_URL + "/", href.lstrip("/"))
        records.append({
            "date": date_str,
            "meeting_id": meeting_id,
            "item_title": subject[:250],
            "vendor_name": parsed["vendor_name"],
            "amount_usd": parsed["amount_usd"],
            "funding_source": parsed["funding_source"],
            "item_description": filename[:300],
            "pdf_urls": url,
        })
    return records

# ── Agenda parsing ─────────────────────────────────────────────────────────────

def parse_agenda(meeting_id: str, date_str: str, html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    records = []

    for item in soup.find_all("div", class_="item"):
        fields = get_item_fields(item)
        subject = fields.get("Subject", "")

        if not subject or not is_contract_subject(subject):
            continue
        # Skip header/separator rows like "-- MOU Consent Items --"
        if re.match(r"^-{2,}", subject.lstrip("0123456789. ")):
            continue

        # Batch ratification items: expand into one row per attachment
        if BATCH_SUBJECT_RE.search(subject):
            expanded = expand_batch_item(item, meeting_id, date_str, subject)
            if expanded:
                records.extend(expanded)
                continue

        full_text = item.get_text(separator=" ", strip=True)
        recommended_action = fields.get("Recommended Action", "")

        records.append({
            "date": date_str,
            "meeting_id": meeting_id,
            "item_title": subject[:250],
            "vendor_name": extract_vendor(subject, recommended_action),
            "amount_usd": extract_amount(fields.get("Dollar Amount", ""), full_text),
            "funding_source": extract_funding(fields.get("Budget Source", ""), full_text),
            "item_description": full_text[:600],
            "pdf_urls": "|".join(extract_pdf_urls(str(item))),
        })

    return records

# ── Date normalization ─────────────────────────────────────────────────────────

DATE_FORMATS = ("%Y%m%d", "%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%B %d, %Y")

def normalize_date(raw) -> str:
    s = str(raw)[:8] if raw else ""  # numberdate is YYYYMMDD (8 chars)
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return s

# ── Main scraping loop ─────────────────────────────────────────────────────────

def scrape_meetings(years: list[int]) -> list[dict]:
    print("Fetching meeting list...")
    try:
        all_meetings = get_all_meetings()
    except Exception as e:
        print(f"  ERROR: {e}")
        return []

    # Filter to requested years using numberdate (YYYYMMDD)
    year_set = set(str(y) for y in years)
    meetings = [m for m in all_meetings if str(m.get("numberdate", ""))[:4] in year_set]
    print(f"  {len(meetings)} meetings in {sorted(year_set)} (of {len(all_meetings)} total)")

    all_records = []
    for mtg in meetings:
        mid = mtg.get("unique", "")
        date_str = normalize_date(mtg.get("numberdate", ""))

        name = mtg.get("name", "")
        print(f"  {mid} ({date_str}) {name}...", end=" ", flush=True)
        try:
            html = fetch_agenda_html(mid)
            records = parse_agenda(mid, date_str, html)
            print(f"{len(records)} contract item(s)")
            all_records.extend(records)
            time.sleep(0.4)
        except requests.HTTPError as e:
            print(f"HTTP {e.response.status_code} — skipping")
        except Exception as e:
            print(f"error ({e}) — skipping")

    return all_records


def main():
    years = [int(y) for y in sys.argv[1:]] if len(sys.argv) > 1 else [2023, 2024, 2025]

    RAW_CSV.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    for year in years:
        records = scrape_meetings([year])
        out = DIR / f"contracts_{year}.csv"
        with open(out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(records)
        print(f"  {len(records)} rows → {out}")
        total += len(records)

    print(f"\nDone. {total} rows total.")


if __name__ == "__main__":
    main()
