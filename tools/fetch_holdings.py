#!/usr/bin/env python3
"""
Fetch top holdings for the largest US equity ETFs and write:
  tools/holdings_raw/<SYM>.tsv   (symbol<TAB>name<TAB>weight%)
  tools/etf_manifest.json        (the funds that returned usable equity holdings)

Source: stockanalysis.com public holdings API (JSON). Runs at build time only —
the app never touches the network; it reads the generated Lume/Data/etf_holdings.json.

Bond, commodity and single-crypto ETFs are filtered out automatically: their
"holdings" aren't single stocks, so they can't be looked through. A fund is kept
only if enough of its holdings are clean equity tickers.

Usage:  python3 tools/fetch_holdings.py   then   python3 tools/build_holdings.py
"""
import json
import os
import re
import time
import urllib.request

ROOT = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(ROOT, "holdings_raw")
API = "https://stockanalysis.com/api/symbol/e/{sym}/holdings"
TOP_N = 20
AS_OF = "2026-05-31"  # data vintage; refresh when re-pulling
TICKER_RE = re.compile(r"^[A-Z][A-Z.\-]{0,5}$")

# The largest US-listed equity ETFs by AUM, grouped by what they represent.
# Bond/commodity/crypto giants (BND, AGG, GLD, IBIT, ...) are deliberately absent:
# they don't decompose into single stocks. International funds are excluded for
# now (foreign-exchange tickers need a normalization step).
ETFS = [
    # Broad US market / S&P 500
    ("VOO", "Vanguard S&P 500 ETF", "US Large Blend"),
    ("SPY", "SPDR S&P 500 ETF Trust", "US Large Blend"),
    ("IVV", "iShares Core S&P 500 ETF", "US Large Blend"),
    ("VTI", "Vanguard Total Stock Market ETF", "US Total Market"),
    ("ITOT", "iShares Core S&P Total US Stock Market", "US Total Market"),
    ("SCHB", "Schwab US Broad Market ETF", "US Total Market"),
    ("SCHX", "Schwab US Large-Cap ETF", "US Large Blend"),
    ("VV", "Vanguard Large-Cap ETF", "US Large Blend"),
    ("SPLG", "SPDR Portfolio S&P 500 ETF", "US Large Blend"),
    ("IWB", "iShares Russell 1000 ETF", "US Large Blend"),
    ("IWV", "iShares Russell 3000 ETF", "US Total Market"),
    ("SCHK", "Schwab 1000 Index ETF", "US Large Blend"),
    ("OEF", "iShares S&P 100 ETF", "US Mega Cap"),
    ("MGC", "Vanguard Mega Cap ETF", "US Mega Cap"),
    # Large growth
    ("QQQ", "Invesco QQQ Trust", "US Large Growth"),
    ("QQQM", "Invesco NASDAQ 100 ETF", "US Large Growth"),
    ("VUG", "Vanguard Growth ETF", "US Large Growth"),
    ("IWF", "iShares Russell 1000 Growth ETF", "US Large Growth"),
    ("SCHG", "Schwab US Large-Cap Growth ETF", "US Large Growth"),
    ("SPYG", "SPDR Portfolio S&P 500 Growth ETF", "US Large Growth"),
    ("MGK", "Vanguard Mega Cap Growth ETF", "US Large Growth"),
    ("IVW", "iShares S&P 500 Growth ETF", "US Large Growth"),
    ("VONG", "Vanguard Russell 1000 Growth ETF", "US Large Growth"),
    ("IWY", "iShares Russell Top 200 Growth ETF", "US Large Growth"),
    # Large value
    ("VTV", "Vanguard Value ETF", "US Large Value"),
    ("IWD", "iShares Russell 1000 Value ETF", "US Large Value"),
    ("SCHV", "Schwab US Large-Cap Value ETF", "US Large Value"),
    ("SPYV", "SPDR Portfolio S&P 500 Value ETF", "US Large Value"),
    ("VONV", "Vanguard Russell 1000 Value ETF", "US Large Value"),
    ("IVE", "iShares S&P 500 Value ETF", "US Large Value"),
    # Factor / smart beta
    ("RSP", "Invesco S&P 500 Equal Weight ETF", "US Large Blend"),
    ("QUAL", "iShares MSCI USA Quality Factor ETF", "US Factor"),
    ("USMV", "iShares MSCI USA Min Vol Factor ETF", "US Factor"),
    ("MTUM", "iShares MSCI USA Momentum Factor ETF", "US Factor"),
    ("VLUE", "iShares MSCI USA Value Factor ETF", "US Factor"),
    ("SPHQ", "Invesco S&P 500 Quality ETF", "US Factor"),
    ("DGRW", "WisdomTree US Quality Dividend Growth", "US Dividend"),
    ("COWZ", "Pacer US Cash Cows 100 ETF", "US Factor"),
    ("SCHD", "Schwab US Dividend Equity ETF", "US Dividend"),
    ("VIG", "Vanguard Dividend Appreciation ETF", "US Dividend"),
    ("VYM", "Vanguard High Dividend Yield ETF", "US Dividend"),
    ("DVY", "iShares Select Dividend ETF", "US Dividend"),
    ("SDY", "SPDR S&P Dividend ETF", "US Dividend"),
    ("NOBL", "ProShares S&P 500 Dividend Aristocrats", "US Dividend"),
    ("HDV", "iShares Core High Dividend ETF", "US Dividend"),
    ("JEPI", "JPMorgan Equity Premium Income ETF", "US Equity Income"),
    ("JEPQ", "JPMorgan Nasdaq Equity Premium Income", "US Equity Income"),
    ("DIVO", "Amplify CWP Enhanced Dividend Income", "US Equity Income"),
    ("SPHD", "Invesco S&P 500 High Div Low Vol ETF", "US Dividend"),
    # Mid cap
    ("IJH", "iShares Core S&P Mid-Cap ETF", "US Mid Cap"),
    ("VO", "Vanguard Mid-Cap ETF", "US Mid Cap"),
    ("MDY", "SPDR S&P MidCap 400 ETF", "US Mid Cap"),
    ("IWR", "iShares Russell Mid-Cap ETF", "US Mid Cap"),
    ("SCHM", "Schwab US Mid-Cap ETF", "US Mid Cap"),
    ("VOE", "Vanguard Mid-Cap Value ETF", "US Mid Value"),
    ("VOT", "Vanguard Mid-Cap Growth ETF", "US Mid Growth"),
    ("IJK", "iShares S&P Mid-Cap 400 Growth ETF", "US Mid Growth"),
    # Small cap
    ("IJR", "iShares Core S&P Small-Cap ETF", "US Small Cap"),
    ("VB", "Vanguard Small-Cap ETF", "US Small Cap"),
    ("IWM", "iShares Russell 2000 ETF", "US Small Cap"),
    ("VBR", "Vanguard Small-Cap Value ETF", "US Small Value"),
    ("VBK", "Vanguard Small-Cap Growth ETF", "US Small Growth"),
    ("SCHA", "Schwab US Small-Cap ETF", "US Small Cap"),
    ("VTWO", "Vanguard Russell 2000 ETF", "US Small Cap"),
    ("IWN", "iShares Russell 2000 Value ETF", "US Small Value"),
    ("IWO", "iShares Russell 2000 Growth ETF", "US Small Growth"),
    ("AVUV", "Avantis US Small Cap Value ETF", "US Small Value"),
    # Sector — Technology & semis
    ("XLK", "Technology Select Sector SPDR", "Sector: Technology"),
    ("VGT", "Vanguard Information Technology ETF", "Sector: Technology"),
    ("IYW", "iShares US Technology ETF", "Sector: Technology"),
    ("FTEC", "Fidelity MSCI Information Technology ETF", "Sector: Technology"),
    ("SMH", "VanEck Semiconductor ETF", "Sector: Semiconductors"),
    ("SOXX", "iShares Semiconductor ETF", "Sector: Semiconductors"),
    ("FDN", "First Trust Dow Jones Internet ETF", "Sector: Internet"),
    ("SKYY", "First Trust Cloud Computing ETF", "Sector: Cloud"),
    # Sector — Financials
    ("XLF", "Financial Select Sector SPDR", "Sector: Financials"),
    ("VFH", "Vanguard Financials ETF", "Sector: Financials"),
    ("KRE", "SPDR S&P Regional Banking ETF", "Sector: Banks"),
    ("KBE", "SPDR S&P Bank ETF", "Sector: Banks"),
    # Sector — Health care
    ("XLV", "Health Care Select Sector SPDR", "Sector: Health Care"),
    ("VHT", "Vanguard Health Care ETF", "Sector: Health Care"),
    ("IBB", "iShares Biotechnology ETF", "Sector: Biotech"),
    ("XBI", "SPDR S&P Biotech ETF", "Sector: Biotech"),
    # Sector — Energy
    ("XLE", "Energy Select Sector SPDR", "Sector: Energy"),
    ("VDE", "Vanguard Energy ETF", "Sector: Energy"),
    ("XOP", "SPDR S&P Oil & Gas Expl & Prod ETF", "Sector: Energy"),
    ("AMLP", "Alerian MLP ETF", "Sector: Energy"),
    # Sector — Consumer
    ("XLY", "Consumer Discretionary Select SPDR", "Sector: Cons. Disc."),
    ("VCR", "Vanguard Consumer Discretionary ETF", "Sector: Cons. Disc."),
    ("XLP", "Consumer Staples Select Sector SPDR", "Sector: Cons. Staples"),
    ("VDC", "Vanguard Consumer Staples ETF", "Sector: Cons. Staples"),
    # Sector — Industrials & materials
    ("XLI", "Industrial Select Sector SPDR", "Sector: Industrials"),
    ("VIS", "Vanguard Industrials ETF", "Sector: Industrials"),
    ("XLB", "Materials Select Sector SPDR", "Sector: Materials"),
    ("VAW", "Vanguard Materials ETF", "Sector: Materials"),
    ("ITB", "iShares US Home Construction ETF", "Sector: Homebuilders"),
    ("JETS", "US Global Jets ETF", "Sector: Airlines"),
    # Sector — Utilities, real estate, communication
    ("XLU", "Utilities Select Sector SPDR", "Sector: Utilities"),
    ("VPU", "Vanguard Utilities ETF", "Sector: Utilities"),
    ("XLRE", "Real Estate Select Sector SPDR", "Sector: Real Estate"),
    ("VNQ", "Vanguard Real Estate ETF", "Sector: Real Estate"),
    ("XLC", "Communication Services Select SPDR", "Sector: Communication"),
    ("VOX", "Vanguard Communication Services ETF", "Sector: Communication"),
    # Thematic / other
    ("ARKK", "ARK Innovation ETF", "Thematic: Innovation"),
    ("MOAT", "VanEck Morningstar Wide Moat ETF", "US Factor"),
    ("DSI", "iShares MSCI KLD 400 Social ETF", "US ESG"),
    ("ESGU", "iShares ESG Aware MSCI USA ETF", "US ESG"),
    ("SUSA", "iShares MSCI USA ESG Select ETF", "US ESG"),
    ("PAVE", "Global X US Infrastructure Dev ETF", "Thematic: Infrastructure"),
    ("XT", "iShares Exponential Technologies ETF", "Thematic: Innovation"),
    ("BOTZ", "Global X Robotics & AI ETF", "Thematic: AI/Robotics"),
    ("IGV", "iShares Expanded Tech-Software ETF", "Sector: Software"),
    ("VONE", "Vanguard Russell 1000 ETF", "US Large Blend"),
    ("SPTM", "SPDR Portfolio S&P 1500 Composite", "US Total Market"),
    ("FXAIX", "Fidelity 500 Index (proxy)", "US Large Blend"),
]


def fetch(sym):
    url = API.format(sym=sym)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Lume data pipeline)"})
    with urllib.request.urlopen(req, timeout=25) as resp:
        payload = json.load(resp)
    holdings = payload.get("data", {}).get("holdings", [])
    rows = []
    for h in holdings:
        ticker = str(h.get("s", "")).lstrip("$").strip().upper()
        name = str(h.get("n", "")).strip()
        weight = str(h.get("as", "")).strip()
        if not TICKER_RE.match(ticker):
            continue  # drops CUSIPs, cash lines, foreign codes → filters bond/cmdty funds
        if not weight or weight in ("n/a", "-"):
            continue  # some funds report only share counts, no weights → unusable
        rows.append((ticker, name, weight))
    return rows


def main():
    os.makedirs(RAW_DIR, exist_ok=True)
    kept, skipped = [], []
    for sym, name, category in ETFS:
        try:
            rows = fetch(sym)
        except Exception as e:  # noqa: BLE001
            skipped.append((sym, f"error: {e}"))
            continue
        if len(rows) < 5:
            skipped.append((sym, f"only {len(rows)} equity holdings (likely not an equity fund)"))
            continue
        with open(os.path.join(RAW_DIR, f"{sym}.tsv"), "w", encoding="utf-8") as f:
            for ticker, hname, weight in rows[:TOP_N]:
                f.write(f"{ticker}\t{hname}\t{weight}\n")
        kept.append({"symbol": sym, "name": name, "category": category})
        time.sleep(0.25)

    manifest = {
        "_comment": ("Generated by fetch_holdings.py from stockanalysis.com. Funds here "
                     "returned usable single-stock holdings; bond/commodity/international "
                     "funds are filtered out. Re-run fetch_holdings.py then build_holdings.py."),
        "asOf": AS_OF,
        "topN": 15,
        "funds": kept,
    }
    with open(os.path.join(ROOT, "etf_manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"Kept {len(kept)} equity ETFs.")
    if skipped:
        print(f"Skipped {len(skipped)}:")
        for sym, why in skipped:
            print(f"  {sym}: {why}")


if __name__ == "__main__":
    main()
