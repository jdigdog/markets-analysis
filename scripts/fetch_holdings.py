"""
Fetch/refresh holdings for all universes defined in universes_config.yml.

For ETFs with auto_fetch_holdings: fetches current holdings from issuer APIs.
  - Invesco ETFs (QQQ, etc.): uses Invesco's direct holdings endpoint (~100 holdings)
  - Other ETFs: falls back to yfinance top_holdings (top 10 only, last resort)
For watchlists: reads tickers from config and writes as equal-weight holdings.

Usage:
    python scripts/fetch_holdings.py
    python scripts/fetch_holdings.py --universe QQQ
"""
from __future__ import annotations

import argparse
import re

import pandas as pd
import requests

from utils import (
    load_config, get_universes, ensure_dirs,
    save_universe_holdings, save_universe_meta, logger,
)

# ── Constants ────────────────────────────────────────────────────────────────

# Invesco direct holdings API (returns full ~100-holding universe for QQQ, QQQ-adjacent ETFs)
INVESCO_HOLDINGS_URL = (
    "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/{ticker}/holdings/fund"
    "?idType=ticker&interval=daily&productType=ETF"
)

# Known Invesco ETF tickers that support the direct API
INVESCO_TICKERS = {
    "QQQ", "QQQM", "QQQJ", "RSP", "SPHD", "SPLV", "PGX", "PFF",
    "BKLN", "PDBC", "PPLT", "IAU", "DJP",
}

# Guardrail: refuse to overwrite with partial data
MIN_ETF_HOLDINGS = 50

# Valid equity ticker pattern (exclude futures like NQH6, options artifacts)
VALID_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


# ── Helpers ──────────────────────────────────────────────────────────────────

def _find_holdings_array(payload: object) -> tuple[list[dict], str]:
    """Locate a list-of-dicts holdings array anywhere in the JSON response."""
    if isinstance(payload, dict):
        for key in ("holdings", "fundHoldings", "portfolioHoldings", "data", "items", "results"):
            v = payload.get(key)
            if isinstance(v, list) and (not v or isinstance(v[0], dict)):
                return v, f"root.{key}"
    if isinstance(payload, list) and (not payload or isinstance(payload[0], dict)):
        return payload, "root(list)"
    return [], "not_found"


def _is_probably_equity_ticker(t: str) -> bool:
    """Filter out futures, options artifacts, and other non-equity symbols."""
    t = (t or "").strip().upper()
    if not t or "_" in t or "$" in t or " " in t:
        return False
    # Futures-like symbols (e.g. NQH6) contain digits — exclude for equity universes
    if any(ch.isdigit() for ch in t):
        return False
    return bool(VALID_TICKER_RE.match(t))


def _extract_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a raw holdings DataFrame to: Ticker, WeightPct, Name."""
    cols_lower = {c: str(c).strip().lower() for c in df.columns}

    # Find ticker column
    ticker_col = None
    for key in ("ticker", "symbol"):
        for c, cl in cols_lower.items():
            if cl == key:
                ticker_col = c
                break
        if ticker_col:
            break
    if ticker_col is None:
        for c, cl in cols_lower.items():
            if "ticker" in cl or "symbol" in cl:
                ticker_col = c
                break
    if ticker_col is None:
        raise RuntimeError(f"No ticker/symbol column found. Columns: {list(df.columns)[:20]}")

    # Find weight column (optional)
    weight_col = next(
        (c for c, cl in cols_lower.items()
         if any(k in cl for k in ("weight", "allocation", "percent", "percentage"))),
        None
    )

    # Find name column (optional)
    name_col = next((c for c, cl in cols_lower.items() if "name" in cl), None)

    out = pd.DataFrame()
    out["Ticker"] = (
        df[ticker_col]
        .apply(lambda x: "" if pd.isna(x) else str(x))
        .str.strip().str.upper()
    )

    # Filter to valid equity tickers
    before = len(out)
    out = out[out["Ticker"].apply(_is_probably_equity_ticker)].copy()
    removed = before - len(out)
    if removed:
        logger.info(f"  Removed {removed} non-equity/invalid tickers")

    # Parse WeightPct — detect fraction (0–1) vs percentage (0–100) by max value
    if weight_col is not None:
        w_raw = df.loc[out.index, weight_col]
        if pd.api.types.is_numeric_dtype(w_raw):
            w = pd.to_numeric(w_raw, errors="coerce")
        else:
            w = pd.to_numeric(
                w_raw.astype(str)
                .str.replace("%", "", regex=False)
                .str.replace(",", "", regex=False)
                .str.strip(),
                errors="coerce"
            )
        w_nonnull = w.dropna()
        if not w_nonnull.empty and float(w_nonnull.max()) <= 1.5:
            w = w * 100.0  # convert fraction to percent
        out["WeightPct"] = w.values
    else:
        out["WeightPct"] = pd.NA

    out["Name"] = (
        df.loc[out.index, name_col]
        .apply(lambda x: "" if pd.isna(x) else str(x))
        .str.strip()
        if name_col is not None else pd.NA
    )

    out = out.drop_duplicates(subset=["Ticker"]).reset_index(drop=True)
    if out["WeightPct"].notna().any():
        out = out.sort_values(by=["WeightPct", "Ticker"], ascending=[False, True])
    else:
        out = out.sort_values(by="Ticker", ascending=True)
    return out.reset_index(drop=True)[["Ticker", "WeightPct", "Name"]]


# ── ETF Fetchers ─────────────────────────────────────────────────────────────

def _fetch_invesco(ticker: str) -> pd.DataFrame:
    """
    Fetch full holdings from Invesco's direct API.
    Returns ~100 holdings for QQQ vs yfinance's top-10 limitation.
    """
    url = INVESCO_HOLDINGS_URL.format(ticker=ticker)
    headers = {
        "User-Agent": "market-data-library/1.0 (github actions)",
        "Accept": "application/json,text/plain,*/*",
    }
    r = requests.get(url, headers=headers, timeout=45)
    r.raise_for_status()

    holdings_list, path_hint = _find_holdings_array(r.json())
    if not holdings_list:
        raise RuntimeError(f"No holdings array in Invesco response (path={path_hint})")

    df = pd.DataFrame(holdings_list)
    if df.empty:
        raise RuntimeError(f"Invesco returned empty holdings DataFrame (path={path_hint})")

    out = _extract_columns(df)
    logger.info(f"  Invesco API: {len(out)} holdings (path={path_hint})")

    if len(out) < MIN_ETF_HOLDINGS:
        raise RuntimeError(
            f"Invesco returned only {len(out)} tickers (<{MIN_ETF_HOLDINGS}). "
            "Refusing to overwrite existing data."
        )
    return out


def _fetch_yfinance_fallback(ticker: str) -> pd.DataFrame:
    """
    Fallback to yfinance funds_data.top_holdings.
    WARNING: only returns top ~10 holdings. Use only when issuer API unavailable.
    """
    import yfinance as yf

    logger.warning(f"  Using yfinance fallback for {ticker} — only top ~10 holdings will be fetched")
    etf = yf.Ticker(ticker)
    holdings_data = []

    if hasattr(etf, "funds_data"):
        try:
            fd = etf.funds_data
            if hasattr(fd, "top_holdings") and fd.top_holdings is not None:
                for idx, row in fd.top_holdings.iterrows():
                    holdings_data.append({
                        "Ticker": str(idx).strip(),
                        "Name": row.get("Name", ""),
                        "WeightPct": round(float(row.get("Holding Percent", 0)) * 100, 4),
                    })
        except Exception as e:
            logger.warning(f"  yfinance funds_data failed: {e}")

    if not holdings_data:
        return pd.DataFrame(columns=["Ticker", "Name", "WeightPct"])

    df = pd.DataFrame(holdings_data)
    df["Ticker"] = df["Ticker"].str.replace(r"[^\w\-.]", "", regex=True)
    df = df[df["Ticker"].str.len() > 0]
    logger.info(f"  yfinance fallback: {len(df)} holdings for {ticker}")
    return df


def fetch_etf_holdings(ticker: str) -> pd.DataFrame:
    """
    Fetch ETF holdings, preferring issuer APIs over yfinance.
    - Invesco ETFs: uses direct API (full ~100-holding universe)
    - Others: falls back to yfinance top_holdings
    """
    logger.info(f"Fetching holdings for ETF: {ticker}")

    if ticker.upper() in INVESCO_TICKERS:
        try:
            return _fetch_invesco(ticker.upper())
        except Exception as e:
            logger.warning(f"  Invesco API failed for {ticker}: {e}. Falling back to yfinance.")

    return _fetch_yfinance_fallback(ticker)


def build_watchlist_holdings(universe: dict) -> pd.DataFrame:
    """Build equal-weight holdings from a watchlist config entry."""
    tickers = universe.get("tickers", [])
    if not tickers:
        return pd.DataFrame(columns=["Ticker", "Name", "WeightPct"])
    weight = round(100.0 / len(tickers), 4)
    return pd.DataFrame([{"Ticker": t, "Name": "", "WeightPct": weight} for t in tickers])


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Fetch/refresh universe holdings")
    parser.add_argument("--universe", type=str, default=None,
                        help="Process only this universe ID")
    args = parser.parse_args()

    ensure_dirs()
    config = load_config()
    universes = get_universes(config)

    for universe in universes:
        uid = universe["id"]

        if args.universe and uid != args.universe:
            continue

        logger.info(f"Processing universe: {uid} (type={universe['type']})")

        utype = universe.get("type", "watchlist")
        if utype in ("etf", "index") and universe.get("auto_fetch_holdings"):
            df = fetch_etf_holdings(uid)
        else:
            df = build_watchlist_holdings(universe)

        if df.empty:
            logger.warning(f"  No holdings for {uid}")
            continue

        save_universe_holdings(uid, df)
        save_universe_meta(uid, {
            "id": uid,
            "name": universe.get("name", uid),
            "type": utype,
            "benchmark": universe.get("benchmark", "SPY"),
            "description": universe.get("description", ""),
            "ticker_count": len(df),
        })

        logger.info(f"  Saved {len(df)} holdings for {uid}")


if __name__ == "__main__":
    main()
