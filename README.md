# lume-data

Public ETF look-through dataset for the **Lume** app.

`etf_holdings.json` holds the top holdings (with weights and sectors) of the
largest US equity ETFs. The Lume app downloads this file at runtime to decompose
funds into their underlying companies. Only public reference data lives here — no
user data, ever.

## How it works

- `tools/fetch_holdings.py` — pulls top holdings for ~110 large US equity ETFs
  from a public source; filters out bond/commodity/international funds.
- `tools/fetch_sectors.py` — classifies each constituent by sector.
- `tools/build_holdings.py` — assembles `etf_holdings.json`.
- `.github/workflows/refresh.yml` — re-runs the pipeline on the 1st of each month
  and commits the refreshed dataset.

## Refresh manually

```sh
python3 tools/fetch_holdings.py
python3 tools/build_holdings.py
python3 tools/fetch_sectors.py
python3 tools/build_holdings.py
```

## Consumed by the app at

`https://raw.githubusercontent.com/<owner>/lume-data/main/etf_holdings.json`

Data is a periodic snapshot from a public aggregator; verify against issuer
filings before relying on it for anything beyond illustration.
