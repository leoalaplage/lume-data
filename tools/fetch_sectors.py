#!/usr/bin/env python3
"""
Fill tools/sectors.json with the sector of every constituent that appears in the
generated dataset but isn't classified yet. Source: stockanalysis.com stock
overview API (its infoTable carries a "Sector" row).

Run order:  fetch_holdings.py  ->  build_holdings.py  ->  fetch_sectors.py  ->  build_holdings.py
(The second build re-attaches the freshly discovered sectors.)
"""
import json
import os
import time
import urllib.request

ROOT = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(ROOT)
SECTORS_PATH = os.path.join(ROOT, "sectors.json")
# App repo bundles it under Lume/Data; the standalone data repo has it at root.
_APP_DATA = os.path.join(REPO, "Lume", "Data", "etf_holdings.json")
DATA_PATH = _APP_DATA if os.path.isfile(_APP_DATA) else os.path.join(REPO, "etf_holdings.json")
API = "https://stockanalysis.com/api/symbol/s/{sym}/overview"

# stockanalysis sector labels -> Lume's labels (match etf_holdings sectors)
NORMALIZE = {
    "Technology": "Technology",
    "Communication Services": "Communication",
    "Consumer Discretionary": "Consumer Discretionary",
    "Consumer Staples": "Consumer Staples",
    "Financials": "Financials",
    "Financial": "Financials",
    "Healthcare": "Health Care",
    "Health Care": "Health Care",
    "Industrials": "Industrials",
    "Energy": "Energy",
    "Utilities": "Utilities",
    "Materials": "Materials",
    "Basic Materials": "Materials",
    "Real Estate": "Real Estate",
}


def fetch_sector(sym):
    url = API.format(sym=sym.replace(".", "-"))
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Lume data pipeline)"})
    with urllib.request.urlopen(req, timeout=25) as resp:
        data = json.load(resp).get("data", {})
    for row in data.get("infoTable", []) or []:
        if row.get("t") == "Sector":
            return NORMALIZE.get(row.get("v", "").strip(), row.get("v", "").strip())
    return None


def main():
    sectors = json.load(open(SECTORS_PATH, encoding="utf-8"))
    data = json.load(open(DATA_PATH, encoding="utf-8"))
    tickers = sorted({h["symbol"] for fund in data["funds"] for h in fund["holdings"]})
    todo = [t for t in tickers if t not in sectors]
    print(f"{len(todo)} tickers to classify (of {len(tickers)} total).")

    found, failed = 0, []
    for i, t in enumerate(todo, 1):
        try:
            sector = fetch_sector(t)
        except Exception as e:  # noqa: BLE001
            failed.append(t)
            continue
        if sector:
            sectors[t] = sector
            found += 1
        else:
            failed.append(t)
        if i % 25 == 0:
            print(f"  {i}/{len(todo)}…")
        time.sleep(0.15)

    # Preserve the leading comment key if present.
    with open(SECTORS_PATH, "w", encoding="utf-8") as f:
        json.dump(sectors, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(f"Classified {found}, still unknown {len(failed)}.")
    if failed:
        print("  Unknown:", ", ".join(failed))


if __name__ == "__main__":
    main()
