#!/usr/bin/env python3
"""
SFUSD BoardDocs PDF extractor — Step 2.

Reads contracts_raw.csv (produced by scraper.py) and cached HTML under cache/.
For each meeting with contract items it:
  1. Scans cached HTML for PDF attachment links not already captured by the scraper.
  2. Downloads all PDFs into pdfs/.
  3. Attempts table extraction (pdfplumber); falls back to raw text.
  4. Emits one row per extracted vendor line, cross-referenced to the original
     scraped record.

Output: contracts_full.csv

Usage:
    python pdf_extractor.py
"""

import csv
import re
import time
from pathlib import Path
from urllib.parse import urljoin

import pdfplumber
import requests
from bs4 import BeautifulSoup

# ── Paths ──────────────────────────────────────────────────────────────────────

DIR = Path(__file__).parent
RAW_CSV = DIR / "contracts_raw.csv"
FULL_CSV = DIR / "contracts_full.csv"
CACHE_DIR = DIR / "cache"
PDF_DIR = DIR / "pdfs"

BASE_URL = "https://go.boarddocs.com/ca/sfusd/Board.nsf"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": f"{BASE_URL}/Public",
}

FULL_FIELDNAMES = [
    # from scraper
    "date", "meeting_id", "item_title",
    "vendor_name", "amount_usd", "funding_source", "item_description",
    # from PDF
    "pdf_source", "pdf_vendor", "pdf_amount", "pdf_funding", "pdf_description",
]

# ── PDF link patterns ──────────────────────────────────────────────────────────

PDF_HREF_RE = re.compile(r'href=["\']([^"\']*\.pdf[^"\']*)["\']', re.IGNORECASE)
# BoardDocs also serves attachments through /$FILE/ paths without .pdf extension
FILE_HREF_RE = re.compile(r'href=["\']([^"\']*\$FILE[^"\']+)["\']', re.IGNORECASE)

AMOUNT_RE = re.compile(r"\$[\d,]+(?:\.\d{1,2})?")

# ── Helpers ────────────────────────────────────────────────────────────────────

def load_raw() -> list[dict]:
    if not RAW_CSV.exists():
        raise FileNotFoundError(f"{RAW_CSV} not found — run scraper.py first.")
    with open(RAW_CSV, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def cached_html(meeting_id: str) -> str | None:
    p = CACHE_DIR / f"{meeting_id}.html"
    return p.read_text(encoding="utf-8") if p.exists() else None


def find_pdf_links(html: str) -> list[str]:
    urls = []
    for pattern in (PDF_HREF_RE, FILE_HREF_RE):
        for href in pattern.findall(html):
            if href.startswith("http"):
                urls.append(href)
            else:
                urls.append(urljoin(BASE_URL + "/", href.lstrip("/")))
    return list(dict.fromkeys(urls))


def pipe_urls(raw_value: str) -> list[str]:
    return [u for u in raw_value.split("|") if u.strip()]


def download_pdf(url: str, dest: Path) -> bool:
    if dest.exists():
        return True
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60, stream=True)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")
        if "pdf" not in ct.lower() and not url.lower().endswith(".pdf"):
            # Peek at first bytes
            chunk = next(resp.iter_content(8), b"")
            if not chunk.startswith(b"%PDF"):
                return False
            dest.write_bytes(chunk + b"".join(resp.iter_content(65536)))
            return True
        dest.write_bytes(resp.content)
        time.sleep(0.3)
        return True
    except Exception as e:
        print(f"    download error: {e}")
        return False

# ── PDF parsing ────────────────────────────────────────────────────────────────

# Maps fuzzy column header keywords to canonical field names
COLUMN_KEYWORDS = {
    "vendor":      ("vendor", "contractor", "company", "payee", "name", "organization"),
    "amount":      ("amount", "cost", "total", "dollar", "expenditure", "fee", "$"),
    "funding":     ("fund", "source", "grant", "program", "object", "account"),
    "description": ("description", "service", "scope", "purpose", "work", "detail"),
}


def map_columns(header_row: list) -> dict[str, int]:
    col_map = {}
    for i, cell in enumerate(header_row):
        h = str(cell).lower().strip() if cell else ""
        for canonical, keywords in COLUMN_KEYWORDS.items():
            if canonical not in col_map and any(kw in h for kw in keywords):
                col_map[canonical] = i
    return col_map


def rows_from_table(table: list[list]) -> list[dict]:
    if not table or len(table) < 2:
        return []

    col_map = map_columns(table[0])
    if not col_map:
        return []

    rows = []
    for row in table[1:]:
        if not row or all(c is None or str(c).strip() == "" for c in row):
            continue

        def cell(key: str) -> str:
            idx = col_map.get(key)
            if idx is None or idx >= len(row):
                return ""
            return str(row[idx] or "").strip()

        vendor = cell("vendor")
        amount = cell("amount")
        if not vendor and not amount:
            continue

        rows.append({
            "pdf_vendor": vendor,
            "pdf_amount": amount,
            "pdf_funding": cell("funding"),
            "pdf_description": cell("description"),
        })
    return rows


def extract_from_pdf(path: Path) -> list[dict]:
    """Try table extraction first; fall back to text scanning."""
    rows = []
    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                for table in (page.extract_tables() or []):
                    rows.extend(rows_from_table(table))
    except Exception as e:
        print(f"    table extract error: {e}")

    if rows:
        return rows

    # Fallback: raw text — pull any lines that look like dollar-amount entries
    try:
        with pdfplumber.open(path) as pdf:
            text = "\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception:
        return []

    for line in text.splitlines():
        if AMOUNT_RE.search(line) and len(line) > 10:
            rows.append({
                "pdf_vendor": "",
                "pdf_amount": " ".join(AMOUNT_RE.findall(line)),
                "pdf_funding": "",
                "pdf_description": line.strip()[:300],
            })

    return rows

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    raw_records = load_raw()
    print(f"Loaded {len(raw_records)} rows from contracts_raw.csv")

    # Group by meeting_id
    by_meeting: dict[str, list[dict]] = {}
    for r in raw_records:
        by_meeting.setdefault(r["meeting_id"], []).append(r)

    PDF_DIR.mkdir(parents=True, exist_ok=True)
    all_enriched: list[dict] = []

    for mid, records in by_meeting.items():
        print(f"\nMeeting {mid} ({records[0]['date']}):")

        # Collect PDF URLs: from raw CSV + any found in cached HTML
        pdf_urls: list[str] = []
        for r in records:
            pdf_urls.extend(pipe_urls(r.get("pdf_urls", "")))

        html = cached_html(mid)
        if html:
            extra = find_pdf_links(html)
            for u in extra:
                if u not in pdf_urls:
                    pdf_urls.append(u)
        else:
            print("  (no cached HTML — using URLs from raw CSV only)")

        pdf_urls = list(dict.fromkeys(pdf_urls))  # deduplicate
        print(f"  {len(pdf_urls)} PDF(s) to process")

        # Download + parse every PDF for this meeting
        meeting_pdf_rows: list[tuple[str, dict]] = []  # (source_url, row)
        for i, url in enumerate(pdf_urls):
            dest = PDF_DIR / f"{mid}_{i}.pdf"
            print(f"  [{i+1}/{len(pdf_urls)}] {url[:90]}")
            if not download_pdf(url, dest):
                print("    skipped (not a PDF or download failed)")
                continue
            extracted = extract_from_pdf(dest)
            print(f"    → {len(extracted)} row(s) extracted")
            for row in extracted:
                meeting_pdf_rows.append((url, row))

        # Cross-reference: emit one enriched output row per (scraped item × pdf row)
        # If no PDF rows, emit the scraped records unchanged with empty PDF columns.
        blank_pdf = {
            "pdf_source": "", "pdf_vendor": "",
            "pdf_amount": "", "pdf_funding": "", "pdf_description": "",
        }

        for r in records:
            base = {k: r.get(k, "") for k in FULL_FIELDNAMES if k in r}
            if meeting_pdf_rows:
                for src_url, pdf_row in meeting_pdf_rows:
                    all_enriched.append({**base, "pdf_source": src_url, **pdf_row})
            else:
                all_enriched.append({**base, **blank_pdf})

    with open(FULL_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FULL_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_enriched)

    print(f"\nDone. {len(all_enriched)} rows → {FULL_CSV}")


if __name__ == "__main__":
    main()
