#!/usr/bin/env python3
"""
Build the on-device stock-quality dataset from SEC EDGAR (public-domain XBRL) plus
a monthly price snapshot (Stooq) for the valuation metrics.

Pipeline
  1. S&P 500 constituents (symbol, GICS sector, CIK) from a public CSV.
  2. Per company: pull data.sec.gov XBRL companyfacts, build annual (FY, 10-K)
     series for each line item using fallback tag lists, and derive 22 metrics.
  3. Pull an EOD price from Stooq to compute market-cap / EV metrics.
  4. Write stock_quality.json (raw metric values only — scoring is a later step).

Nothing here runs at app runtime; the app reads the generated JSON, so the
scanner stays fully on-device. Run:  python3 tools/build_quality.py [--limit N] [SYM ...]
"""
import bisect
import csv
import io
import json
import os
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import date, timedelta

ROOT = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(ROOT)
_APP_DATA = os.path.join(REPO, "Lume", "Data")
OUT = os.path.join(_APP_DATA, "stock_quality.json") if os.path.isdir(_APP_DATA) \
    else os.path.join(REPO, "stock_quality.json")

UA = "Lume research leoalaplage@gmail.com"
SP500_CSV = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
AS_OF = date.today().isoformat()

# ── XBRL concept fallback lists (us-gaap unless noted) ──────────────────────
REVENUE = ["RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues",
           "RevenueFromContractWithCustomerIncludingAssessedTax", "SalesRevenueNet",
           "RegulatedAndUnregulatedOperatingRevenue", "RevenuesNetOfInterestExpense"]
COGS = ["CostOfGoodsAndServicesSold", "CostOfRevenue", "CostOfGoodsSold"]
GROSS = ["GrossProfit"]
EBIT = ["OperatingIncomeLoss"]
NET_INCOME = ["NetIncomeLoss", "ProfitLoss"]
INTEREST = ["InterestExpense", "InterestExpenseDebt", "InterestAndDebtExpense"]
TAX = ["IncomeTaxExpenseBenefit"]
PRETAX = ["IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
          "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments"]
SBC = ["ShareBasedCompensation", "ShareBasedCompensationExpense"]
DA = ["DepreciationDepletionAndAmortization", "DepreciationAmortizationAndAccretionNet",
      "DepreciationAndAmortization"]
OCF = ["NetCashProvidedByUsedInOperatingActivities",
       "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"]
CAPEX = ["PaymentsToAcquirePropertyPlantAndEquipment", "PaymentsToAcquireProductiveAssets",
         "PaymentsForCapitalImprovements", "PaymentsToAcquireOtherPropertyPlantAndEquipment",
         "PaymentsToAcquireMachineryAndEquipment", "PaymentsToAcquireEquipmentOnLease"]
ASSETS = ["Assets"]
CUR_ASSETS = ["AssetsCurrent"]
CUR_LIAB = ["LiabilitiesCurrent"]
EQUITY = ["StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"]
CASH = ["CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"]
LTD_NC = ["LongTermDebtNoncurrent", "LongTermDebt"]
LTD_C = ["LongTermDebtCurrent", "DebtCurrent"]
STD = ["ShortTermBorrowings", "ShortTermDebt"]
DILUTED_SH = ["WeightedAverageNumberOfDilutedSharesOutstanding",
              "WeightedAverageNumberOfDilutedSharesOutstandingAdjustment"]

# Capital-light sectors: banks, insurers and asset managers report little or no
# PP&E capex, so a missing capex line means ~0, not "unknown". Treating FCF = OCF
# there recovers the FCF-family metrics (margin, yield, conversion, CAGRs) instead
# of cascading nine metrics to N/A off one absent tag. REITs are deliberately left
# out — their cash generation is FFO/AFFO, which FCF≈OCF would misrepresent.
CAPITAL_LIGHT_SECTORS = {"Financials"}

# Banks are the exception inside Financials: their operating cash flow swings with
# loans/deposits and is NOT free cash flow (JPM's OCF is wildly negative some years),
# and their "operating margin" is a net-interest artifact. A true bank both TAKES
# DEPOSITS and reports operating interest income — requiring BOTH cleanly separates
# banks (JPM, BAC, COF, NTRS, GS, MS, AXP) from payment networks (V, MA), exchanges
# (ICE, CME, SPGI) and insurers/asset managers, which report at most one of them.
BANK_DEPOSIT_TAG = "Deposits"
BANK_INTEREST_TAG = "InterestAndDividendIncomeOperating"


# ── Scoring model ───────────────────────────────────────────────────────────
# Direction of each metric: +1 = higher is better, -1 = lower is better.
METRIC_DIR = {
    "grossMargin": 1, "grossMargin5yAvg": 1, "operatingMargin": 1, "roic5yAvg": 1,
    "fcfMargin": 1, "fcfMargin5yAvg": 1, "fcfToNetIncome": 1, "capexCoverage": 1,
    "netDebtToEbitda": -1, "ebitToInterest": 1, "ltDebtToAssets": -1, "currentRatio": 1,
    "revenue5yCagr": 1, "leveredFcf5yCagr": 1, "fcfPerShareCagr5y": 1, "fcfPerShareCagr10y": 1,
    "sharesOut5yCagr": -1, "sharesOut10yCagr": -1, "sbcToRevenue": -1,
    "fcfYield": 1, "evToEbit": -1, "evToFcf": -1,
    "earningsStability": 1,
}
# Global weight of each pillar in the overall score. This is a QUALITY screen, so
# Quality dominates and Valuation is only a light tiebreaker — being expensive does
# not make a wide-moat compounder low-quality (it would have buried MSFT/V/MA).
PILLAR_WEIGHTS = {"quality": 0.60, "health": 0.20, "valuation": 0.05, "growth": 0.15}

# Every metric is ranked on ONE absolute scale — its percentile against the whole
# S&P 500, never sector-relative. The goal is the best companies outright, so a weak
# sector must not hand its leaders a free pass (a fertilizer maker shouldn't score
# like a software compounder just because its peers are worse). 0.0 = fully universe.
PILLAR_SECTOR_BLEND = {"quality": 0.0, "health": 0.0, "valuation": 0.0, "growth": 0.0}

# Metric weights within each pillar (sum ~1.0). The two "forward" metrics the
# user's spec lists (Forward P/FCF, Revenue Forward 3Y CAGR) aren't available from
# EDGAR, so they're dropped and their weight redistributed while keeping the
# explicit sub-weights (ROIC = 25% of Quality, FCF Yield = 40% of Valuation).
# Earnings stability (durability) takes 15% of Quality; the 9 profitability/dilution
# metrics share the remaining 60%.
_Q_OTHER = 0.60 / 9
PILLAR_METRICS = {
    "quality": {
        "roic5yAvg": 0.25, "earningsStability": 0.15,
        "grossMargin": _Q_OTHER, "grossMargin5yAvg": _Q_OTHER, "operatingMargin": _Q_OTHER,
        "fcfMargin": _Q_OTHER, "fcfMargin5yAvg": _Q_OTHER, "fcfToNetIncome": _Q_OTHER,
        "sharesOut5yCagr": _Q_OTHER, "sharesOut10yCagr": _Q_OTHER, "sbcToRevenue": _Q_OTHER,
    },
    "health": {
        "netDebtToEbitda": 0.20, "currentRatio": 0.20, "ebitToInterest": 0.20,
        "ltDebtToAssets": 0.20, "capexCoverage": 0.20,
    },
    "valuation": {  # Forward P/FCF dropped; its 20% goes to EV/EBIT & EV/FCF, FCF Yield stays 40%
        "fcfYield": 0.40, "evToEbit": 0.30, "evToFcf": 0.30,
    },
    "growth": {  # Revenue Forward 3Y CAGR dropped; remaining four share equally
        "revenue5yCagr": 0.25, "leveredFcf5yCagr": 0.25,
        "fcfPerShareCagr5y": 0.25, "fcfPerShareCagr10y": 0.25,
    },
}
MIN_SECTOR_PEERS = 8  # below this, rank against the whole universe for that metric

# Coverage floors, so a grade never rests on too little data (a pillar computed
# from one lucky metric told us nothing — see a bank's low LT-debt/assets).
MIN_PILLAR_COVERAGE = 0.45   # a pillar needs ~half its metric weight present, else N/A
                             # (0.45 not 0.50: the common "all quality but ROIC" set sums
                             #  to exactly 6×(0.75/9)=0.4999… and must not fall on the wrong
                             #  side of a float boundary)
MIN_OVERALL_COVERAGE = 0.5   # a graded stock needs pillars covering >=50% of total weight
MIN_PILLARS_FOR_GRADE = 2    # …and at least two pillars, so no grade rests on one thin pillar


def _percentile(sorted_vals, x):
    """Mid-rank percentile of x within a pre-sorted list (0..100)."""
    n = len(sorted_vals)
    if n == 0:
        return 50.0
    lo = bisect.bisect_left(sorted_vals, x)
    hi = bisect.bisect_right(sorted_vals, x)
    return (lo + 0.5 * (hi - lo)) / n * 100.0


def _grade_from_percentile(p):
    # Graded on a curve vs the S&P 500: A ~ top 12%, F ~ bottom 12%.
    return "A" if p >= 88 else "B" if p >= 68 else "C" if p >= 32 else "D" if p >= 12 else "F"


def score_universe(stocks):
    """Attach percentile sub-scores (sector-relative), pillar scores, an overall
    0–100 and an A–F grade to each stock. Missing metrics are simply excluded and
    the pillar / overall weights renormalise over what's present."""
    sector_vals = defaultdict(lambda: defaultdict(list))
    universe_vals = defaultdict(list)
    for s in stocks:
        for k, v in s["metrics"].items():
            if v is not None:
                sector_vals[k][s["sector"]].append(v)
                universe_vals[k].append(v)
    for k in universe_vals:
        universe_vals[k].sort()
    for k in sector_vals:
        for sec in sector_vals[k]:
            sector_vals[k][sec].sort()

    # metric -> its pillar's sector/universe blend factor
    metric_blend = {k: PILLAR_SECTOR_BLEND[pillar]
                    for pillar, weights in PILLAR_METRICS.items() for k in weights}

    for s in stocks:
        subs = {}
        for k, v in s["metrics"].items():
            if v is None:
                continue
            peers = sector_vals[k].get(s["sector"], [])
            sector_p = _percentile(peers if len(peers) >= MIN_SECTOR_PEERS else universe_vals[k], v)
            universe_p = _percentile(universe_vals[k], v)
            a = metric_blend.get(k, 1.0)
            p = a * sector_p + (1.0 - a) * universe_p
            subs[k] = round(100.0 - p if METRIC_DIR[k] < 0 else p, 1)

        # Weighted pillar scores, renormalising over the metrics actually present —
        # but only if enough of the pillar's weight is covered. A pillar resting on
        # a single lucky metric is dropped (N/A) rather than trusted.
        pillars = {}
        for name, weights in PILLAR_METRICS.items():
            total = sum(weights.values())
            num = den = 0.0
            for k, w in weights.items():
                if k in subs:
                    num += w * subs[k]
                    den += w
            pillars[name] = round(num / den, 1) if den >= MIN_PILLAR_COVERAGE * total else None

        # Overall: weighted pillars, renormalising over present pillars. Requires at
        # least two pillars covering half the total weight, so nothing is graded on a
        # single thin pillar (which was inflating sparse banks/insurers/REITs).
        present = [n for n in PILLAR_WEIGHTS if pillars[n] is not None]
        den = sum(PILLAR_WEIGHTS[n] for n in present)
        overall = round(sum(PILLAR_WEIGHTS[n] * pillars[n] for n in present) / den) \
            if (len(present) >= MIN_PILLARS_FOR_GRADE and den >= MIN_OVERALL_COVERAGE) else None

        s["subScores"] = subs
        s["pillars"] = pillars
        s["overall"] = overall

    # Grade on a curve: rank each overall against the scored universe.
    overalls = sorted(s["overall"] for s in stocks if s["overall"] is not None)
    for s in stocks:
        s["grade"] = None if s["overall"] is None else \
            _grade_from_percentile(_percentile(overalls, s["overall"]))
    return stocks


def fetch(url, tries=3):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read()
        except Exception:
            if i == tries - 1:
                return None
            time.sleep(1.0 + i)
    return None


def _parse(d):
    return date.fromisoformat(d)


def annual(usgaap, concepts, unit, instant):
    """FY 10-K values as {year: value}, MERGED across the fallback tag list.
    Companies migrate tags over time (e.g. Revenues -> ASC-606 revenue), so no
    single concept spans all years — earlier concepts in the list win per year,
    latest-filed wins within a concept (handles restatements)."""
    out = {}  # year -> value (already resolved, earliest concept wins)
    for c in concepts:
        node = usgaap.get(c)
        if not node:
            continue
        cur = {}  # this concept: year -> (val, filed)
        for e in node.get("units", {}).get(unit, []):
            if not str(e.get("form", "")).startswith("10-K") or e.get("fp") != "FY":
                continue
            end, val = e.get("end"), e.get("val")
            if not end or val is None:
                continue
            if not instant:
                start = e.get("start")
                if not start:
                    continue
                days = (_parse(end) - _parse(start)).days
                if days < 340 or days > 380:
                    continue
            yr = int(end[:4])
            filed = e.get("filed", "")
            if yr not in cur or filed > cur[yr][1]:
                cur[yr] = (val, filed)
        for yr, (val, _) in cur.items():
            if yr not in out:
                out[yr] = val
    return out


def desplit(series):
    """Normalise a share-count series for stock splits so a CAGR reflects real
    dilution/buybacks, not split mechanics. Raw XBRL share counts are as-reported,
    so a 4:1 split shows up as a 4x jump — detect clean multiples and rescale the
    older years to the newest year's basis."""
    if len(series) < 2:
        return dict(series)
    yrs = sorted(series.keys(), reverse=True)  # newest first
    adj = {yrs[0]: series[yrs[0]]}
    cum = 1.0
    for i in range(1, len(yrs)):
        newer, older = series[yrs[i - 1]], series[yrs[i]]
        r = (newer / older) if older else 1.0
        factor = 1.0
        for s in (10, 7, 5, 4, 3, 2, 1.5):
            if abs(r / s - 1) < 0.12:      # forward split (share count jumped up)
                factor = s; break
            if abs(r * s - 1) < 0.12:      # reverse split (share count dropped)
                factor = 1.0 / s; break
        cum *= factor
        adj[yrs[i]] = older * cum
    return adj


def latest_end_date(usgaap, concepts, unit="USD"):
    """Most recent period-end date seen for a concept (ISO string), or None."""
    best = None
    for c in concepts:
        node = usgaap.get(c)
        if not node:
            continue
        for e in node.get("units", {}).get(unit, []):
            en = e.get("end")
            if en and (best is None or en > best):
                best = en
    return best


def ttm_flow(usgaap, concepts):
    """Trailing-twelve-months value for a flow (income / cash-flow) concept.
    Uses FY + latest year-to-date − prior-year year-to-date, so it works even
    though Q4 is rarely filed as a discrete quarter. Falls back to the latest full
    fiscal year when no quarterly data is available."""
    # Pick the fallback concept with the most recent data (companies migrate
    # tags over time, so an earlier concept may be stale — same trap as annual()).
    best_rows, best_end = None, None
    for c in concepts:
        node = usgaap.get(c)
        if not node:
            continue
        rows = []
        for e in node.get("units", {}).get("USD", []):
            form = e.get("form", "")
            if not (form.startswith("10-K") or form.startswith("10-Q")):
                continue
            start, end, val = e.get("start"), e.get("end"), e.get("val")
            if not start or not end or val is None:
                continue
            rows.append((_parse(start), _parse(end), (_parse(end) - _parse(start)).days, val, e.get("filed", "")))
        if not rows:
            continue
        mx = max(r[1] for r in rows)
        if best_end is None or mx > best_end:
            best_end, best_rows = mx, rows

    if best_rows:
        rows = best_rows
        latest_end = best_end

        def pick(end_date, dmin, dmax):
            cands = [(f, v) for (st, en, d, v, f) in rows if en == end_date and dmin <= d <= dmax]
            return max(cands, key=lambda x: x[0])[1] if cands else None

        # A full ~12-month period ending at the latest date is already TTM.
        direct = pick(latest_end, 340, 380)
        if direct is not None:
            return direct

        # Latest year-to-date (longest sub-annual period ending at latest_end).
        ytd = [(d, v) for (st, en, d, v, f) in rows if en == latest_end and 60 <= d < 340]
        fy = [(en, v) for (st, en, d, v, f) in rows if 340 <= d <= 380]
        if not ytd:
            return max(fy, key=lambda x: x[0])[1] if fy else None
        ytd_days, ytd_cur = max(ytd, key=lambda x: x[0])

        # Prior-year year-to-date of the same length, ending ~1 year earlier.
        try:
            target = latest_end.replace(year=latest_end.year - 1)
        except ValueError:
            target = latest_end - timedelta(days=365)
        prior, best = None, 999
        for (st, en, d, v, f) in rows:
            if abs(d - ytd_days) <= 12:
                diff = abs((en - target).days)
                if diff <= 25 and diff < best:
                    best, prior = diff, v
        if not fy or prior is None:
            return None
        return max(fy, key=lambda x: x[0])[1] + ytd_cur - prior
    return None


def latest_instant(usgaap, concepts, unit):
    """Most recently reported value for a balance-sheet / cover-page concept."""
    best = None  # (end, val)
    for c in concepts:
        node = usgaap.get(c)
        if not node:
            continue
        for e in node.get("units", {}).get(unit, []):
            end, val = e.get("end"), e.get("val")
            if not end or val is None:
                continue
            if best is None or end > best[0]:
                best = (end, val)
        if best:
            return best[1]
    return None


def g(series, year):
    return series.get(year)


def safe_div(a, b):
    if a is None or b in (None, 0):
        return None
    return a / b


def cagr(series, years):
    yrs = sorted(series.keys())
    if len(yrs) < 2:
        return None
    last = yrs[-1]
    first = last - years
    if first not in series:
        # allow the nearest available start within one year
        cand = [y for y in yrs if y <= first]
        if not cand:
            return None
        first = cand[-1]
    span = last - first
    a, b = series[first], series[last]
    if span <= 0 or a is None or b is None or a <= 0 or b <= 0:
        return None
    return (b / a) ** (1.0 / span) - 1.0


def mean_last(series, n):
    yrs = sorted(series.keys())[-n:]
    vals = [series[y] for y in yrs if series[y] is not None]
    return sum(vals) / len(vals) if vals else None


def _dispersion(series):
    """Spread of a profitability series, in the metric's own units (fractional
    margin / ROIC). Uses mean-absolute-deviation, which — unlike a coefficient of
    variation — stays well-behaved when the series crosses zero (a cyclical dipping
    to a loss). Higher = more volatile. Needs >=4 years to be meaningful."""
    vals = [v for v in series.values() if v is not None]
    n = len(vals)
    if n < 4:
        return None
    mean = sum(vals) / n
    return sum(abs(v - mean) for v in vals) / n


def earnings_stability(*series_list):
    """A 0..1 durability score: 1 = rock-steady margins/returns (a compounder),
    →0 = wildly swinging (a cyclical). Averages the dispersion of each supplied
    profitability series and maps it through 1/(1+k·disp). k=8 makes a ~6pt average
    swing score ~0.68 and a ~25pt swing (deep cyclical) score ~0.33."""
    disps = [d for d in (_dispersion(s) for s in series_list) if d is not None]
    if not disps:
        return None
    return 1.0 / (1.0 + 8.0 * (sum(disps) / len(disps)))


def build_metrics(usgaap, price, sector=None):
    rev = annual(usgaap, REVENUE, "USD", False)
    if len(rev) < 2:
        return None  # can't do much without a revenue history
    # In capital-light sectors an absent capex line means ~0 spend, not unknown —
    # but not for banks, whose OCF isn't FCF (see the bank tags above).
    is_bank = BANK_DEPOSIT_TAG in usgaap and BANK_INTEREST_TAG in usgaap
    capex_light = sector in CAPITAL_LIGHT_SECTORS and not is_bank
    cogs = annual(usgaap, COGS, "USD", False)
    gross = annual(usgaap, GROSS, "USD", False)
    ebit = annual(usgaap, EBIT, "USD", False)
    ni = annual(usgaap, NET_INCOME, "USD", False)
    interest = annual(usgaap, INTEREST, "USD", False)
    tax = annual(usgaap, TAX, "USD", False)
    pretax = annual(usgaap, PRETAX, "USD", False)
    # Reconstruct EBIT where OperatingIncomeLoss isn't tagged: pretax + interest.
    for y in set(pretax) | set(interest):
        if y not in ebit and pretax.get(y) is not None and interest.get(y) is not None:
            ebit[y] = pretax[y] + interest[y]
    sbc = annual(usgaap, SBC, "USD", False)
    da = annual(usgaap, DA, "USD", False)
    ocf = annual(usgaap, OCF, "USD", False)
    capex = annual(usgaap, CAPEX, "USD", False)
    assets = annual(usgaap, ASSETS, "USD", True)
    cur_a = annual(usgaap, CUR_ASSETS, "USD", True)
    cur_l = annual(usgaap, CUR_LIAB, "USD", True)
    equity = annual(usgaap, EQUITY, "USD", True)
    cash = annual(usgaap, CASH, "USD", True)
    ltd_nc = annual(usgaap, LTD_NC, "USD", True)
    ltd_c = annual(usgaap, LTD_C, "USD", True)
    std = annual(usgaap, STD, "USD", True)
    shares = desplit(annual(usgaap, DILUTED_SH, "shares", False))

    years = sorted(rev.keys())
    y0 = years[-1]

    def gross_profit(y):
        if y in gross:
            return gross[y]
        if y in rev and y in cogs:
            return rev[y] - cogs[y]
        return None

    def total_debt(y):
        parts = [ltd_nc.get(y), ltd_c.get(y), std.get(y)]
        vals = [p for p in parts if p is not None]
        return sum(vals) if vals else None

    def fcf(y):
        o = ocf.get(y)
        if o is None:
            return None
        c = capex.get(y)
        if c is None:
            if not capex_light:
                return None
            c = 0.0  # capital-light: no capex line ⇒ ~0 spend
        return o - c

    def roic(y):
        e = ebit.get(y)
        td, eq, ca = total_debt(y), equity.get(y), cash.get(y)
        if e is None or eq is None:
            return None
        ic = (td or 0) + eq - (ca or 0)
        if ic <= 0:
            return None
        t = safe_div(tax.get(y), pretax.get(y))
        t = min(max(t, 0.0), 0.5) if t is not None else 0.21
        return e * (1 - t) / ic

    # per-year derived series (for averages)
    gm_series = {y: safe_div(gross_profit(y), rev.get(y)) for y in years}
    gm_series = {y: v for y, v in gm_series.items() if v is not None}
    fcfm_series = {y: safe_div(fcf(y), rev.get(y)) for y in years}
    fcfm_series = {y: v for y, v in fcfm_series.items() if v is not None}
    roic_series = {y: roic(y) for y in years}
    roic_series = {y: v for y, v in roic_series.items() if v is not None}
    om_series = {y: safe_div(ebit.get(y), rev.get(y)) for y in years}
    om_series = {y: v for y, v in om_series.items() if v is not None}
    fcf_series = {y: fcf(y) for y in years if fcf(y) is not None}
    fps_series = {y: safe_div(fcf(y), shares.get(y)) for y in years}
    fps_series = {y: v for y, v in fps_series.items() if v is not None}

    # ── Point-in-time metrics: TTM flows + latest-quarter balance sheet ──────
    rev_t = ttm_flow(usgaap, REVENUE)
    cogs_t = ttm_flow(usgaap, COGS)
    gross_t = ttm_flow(usgaap, GROSS)
    ebit_t = ttm_flow(usgaap, EBIT)
    ni_t = ttm_flow(usgaap, NET_INCOME)
    interest_t = ttm_flow(usgaap, INTEREST)
    tax_t = ttm_flow(usgaap, TAX)
    pretax_t = ttm_flow(usgaap, PRETAX)
    sbc_t = ttm_flow(usgaap, SBC)
    da_t = ttm_flow(usgaap, DA)
    ocf_t = ttm_flow(usgaap, OCF)
    capex_t = ttm_flow(usgaap, CAPEX)
    if ebit_t is None and pretax_t is not None and interest_t is not None:
        ebit_t = pretax_t + interest_t

    assets_l = latest_instant(usgaap, ASSETS, "USD")
    cur_a_l = latest_instant(usgaap, CUR_ASSETS, "USD")
    cur_l_l = latest_instant(usgaap, CUR_LIAB, "USD")
    cash_l = latest_instant(usgaap, CASH, "USD")
    ltd_nc_l = latest_instant(usgaap, LTD_NC, "USD")
    ltd_c_l = latest_instant(usgaap, LTD_C, "USD")
    std_l = latest_instant(usgaap, STD, "USD")
    debt_parts = [x for x in (ltd_nc_l, ltd_c_l, std_l) if x is not None]
    total_debt_l = sum(debt_parts) if debt_parts else None
    net_debt_l = (total_debt_l - cash_l) if (total_debt_l is not None and cash_l is not None) else None

    gross_profit_t = gross_t if gross_t is not None else \
        ((rev_t - cogs_t) if (rev_t is not None and cogs_t is not None) else None)
    if capex_t is None and capex_light:
        capex_t = 0.0  # capital-light: FCF ≈ OCF (capexCoverage stays N/A via /0)
    fcf_t = None if (ocf_t is None or capex_t is None) else ocf_t - capex_t
    ebitda_t = None if (ebit_t is None or da_t is None) else ebit_t + da_t

    cur_shares = latest_instant(usgaap.get("_dei", {}), ["EntityCommonStockSharesOutstanding"], "shares")
    if cur_shares is None:
        cur_shares = shares.get(y0)
    market_cap = None if (price is None or cur_shares in (None, 0)) else price * cur_shares
    ev = (market_cap + net_debt_l) if (market_cap is not None and net_debt_l is not None) else None

    m = {
        # Point metrics — TTM / latest quarter (fresh at each 10-Q).
        "grossMargin": safe_div(gross_profit_t, rev_t),
        "operatingMargin": safe_div(ebit_t, rev_t),
        "fcfMargin": safe_div(fcf_t, rev_t),
        "fcfToNetIncome": safe_div(fcf_t, ni_t),
        "sbcToRevenue": safe_div(sbc_t, rev_t),
        "netDebtToEbitda": safe_div(net_debt_l, ebitda_t),
        "currentRatio": safe_div(cur_a_l, cur_l_l),
        "ebitToInterest": safe_div(ebit_t, interest_t),
        "ltDebtToAssets": safe_div(ltd_nc_l, assets_l),
        "capexCoverage": safe_div(ocf_t, capex_t),
        "fcfYield": safe_div(fcf_t, market_cap),
        "evToEbit": safe_div(ev, ebit_t),
        "evToFcf": safe_div(ev, fcf_t),
        # Series metrics — annual (need full fiscal years for averages / CAGRs).
        "grossMargin5yAvg": mean_last(gm_series, 5),
        "fcfMargin5yAvg": mean_last(fcfm_series, 5),
        "roic5yAvg": mean_last(roic_series, 5),
        "sharesOut5yCagr": cagr(shares, 5),
        "sharesOut10yCagr": cagr(shares, 10),
        "revenue5yCagr": cagr(rev, 5),
        "leveredFcf5yCagr": cagr(fcf_series, 5),
        "fcfPerShareCagr5y": cagr(fps_series, 5),
        "fcfPerShareCagr10y": cagr(fps_series, 10),
        # Durability: how steady operating margin, FCF margin and ROIC have been over
        # the years. Compounders hold near-flat, high margins; cyclicals swing — this
        # is what separates a wide-moat name from a fertilizer/gold miner at a peak.
        "earningsStability": earnings_stability(om_series, fcfm_series, roic_series),
    }
    # A margin above ~100% is a tag artifact (common for REITs/banks where the
    # gross/operating concepts aren't margin-meaningful) — drop it as N/A.
    for k in ("grossMargin", "grossMargin5yAvg", "operatingMargin", "fcfMargin", "fcfMargin5yAvg"):
        if m.get(k) is not None and m[k] > 1.05:
            m[k] = None
    # Banks have no meaningful gross/operating margin: their "operating income ÷
    # revenue" lands at 80–105% (net-interest accounting) and would rank them as
    # ultra-profitable against real businesses. Drop those two for banks entirely —
    # they're graded on ROIC, capital returns and health instead.
    if is_bank:
        m["grossMargin"] = m["grossMargin5yAvg"] = m["operatingMargin"] = None

    # round for a compact file
    for k, v in m.items():
        if v is not None:
            m[k] = round(v, 5)
    covered = sum(1 for v in m.values() if v is not None)
    # Base the "as of" on cash-flow / balance-sheet filings, which every company
    # files each quarter — revenue tags vary and can be stale for banks/utilities.
    as_of = latest_end_date(usgaap, OCF) or latest_end_date(usgaap, ASSETS) or latest_end_date(usgaap, REVENUE)
    return {
        "metrics": m, "fiscalYear": y0, "asOfPeriod": as_of, "coverage": covered, "price": price,
        # Raw TTM/latest blocks so the app can recompute price-based valuation
        # (FCF yield, EV/EBIT, EV/FCF) live against the current quote.
        "fcf": fcf_t, "ebit": ebit_t, "netDebt": net_debt_l, "shares": cur_shares,
    }


def snapshot_price(symbol):
    """EOD price for the valuation metrics: Yahoo primary, Stooq fallback."""
    y = symbol.upper().replace(".", "-")
    data = fetch(f"https://query1.finance.yahoo.com/v8/finance/chart/{y}?interval=1d&range=1d")
    if data:
        try:
            meta = json.loads(data)["chart"]["result"][0]["meta"]
            p = meta.get("regularMarketPrice")
            if p and p > 0:
                return float(p)
        except (KeyError, IndexError, TypeError, json.JSONDecodeError):
            pass
    s = symbol.lower().replace(".", "-")
    data = fetch(f"https://stooq.com/q/l/?s={s}.us&f=sd2t2ohlcv&h&e=csv")
    if data:
        lines = data.decode("utf-8", "ignore").splitlines()
        if len(lines) >= 2:
            cols = lines[1].split(",")
            try:
                c = float(cols[6])
                if c > 0:
                    return c
            except (ValueError, IndexError):
                pass
    return None


def load_universe(only):
    data = fetch(SP500_CSV)
    if not data:
        print("Could not fetch S&P 500 constituents.", file=sys.stderr)
        return []
    rows = list(csv.DictReader(io.StringIO(data.decode("utf-8"))))
    universe = []
    for r in rows:
        sym = r["Symbol"].strip().upper()
        cik = r.get("CIK", "").strip()
        if not cik:
            continue
        if only and sym not in only:
            continue
        universe.append((sym, r["Security"].strip(), r["GICS Sector"].strip(), int(cik)))
    return universe


def main():
    args = sys.argv[1:]
    limit = None
    only = set()
    i = 0
    while i < len(args):
        if args[i] == "--limit":
            limit = int(args[i + 1]); i += 2
        else:
            only.add(args[i].upper()); i += 1

    universe = load_universe(only)
    if limit:
        universe = universe[:limit]
    print(f"Building quality data for {len(universe)} companies…")

    out_stocks = []
    skipped = []
    for n, (sym, name, sector, cik) in enumerate(universe, 1):
        facts_raw = fetch(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json")
        time.sleep(0.13)  # stay well under SEC's 10 req/s
        if not facts_raw:
            skipped.append(sym); continue
        try:
            facts = json.loads(facts_raw).get("facts", {})
        except json.JSONDecodeError:
            skipped.append(sym); continue
        usgaap = dict(facts.get("us-gaap", {}))
        usgaap["_dei"] = facts.get("dei", {})  # stash for share count lookup
        price = snapshot_price(sym)
        result = build_metrics(usgaap, price, sector)
        if not result:
            skipped.append(sym); continue
        def _int(x):
            return int(round(x)) if x is not None else None
        out_stocks.append({
            "symbol": sym,
            "name": name,
            "sector": sector,
            "fiscalYear": result["fiscalYear"],
            "asOfPeriod": result["asOfPeriod"],
            "price": result["price"],
            "fcf": _int(result["fcf"]),
            "ebit": _int(result["ebit"]),
            "netDebt": _int(result["netDebt"]),
            "shares": _int(result["shares"]),
            "metrics": result["metrics"],
        })
        if n % 25 == 0:
            print(f"  …{n}/{len(universe)}")

    score_universe(out_stocks)
    payload = {
        "note": ("Per-company fundamentals and quality scores for the on-device scanner, "
                 "generated by tools/build_quality.py from SEC EDGAR (public domain) plus a "
                 "price snapshot for valuation metrics. Scores are sector-relative percentiles "
                 "across the S&P 500. Refresh monthly."),
        "asOf": AS_OF,
        "stocks": sorted(out_stocks, key=lambda s: s["symbol"]),
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"Wrote {len(out_stocks)} stocks -> {os.path.relpath(OUT, REPO)}")
    if skipped:
        print(f"  Skipped ({len(skipped)}): {', '.join(skipped[:30])}{'…' if len(skipped) > 30 else ''}")


if __name__ == "__main__":
    sys.exit(main())
