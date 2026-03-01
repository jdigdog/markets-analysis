"""
Fetch/refresh holdings for all universes defined in universes_config.yml.

For ETFs with auto_fetch_holdings: scrapes current holdings.
For watchlists: reads tickers from config and writes as equal-weight holdings.

Usage:
    python scripts/fetch_holdings.py
    python scripts/fetch_holdings.py --universe QQQ
"""

import argparse
import pandas as pd

from utils import (
    load_config, get_universes, ensure_dirs,
    save_universe_holdings, save_universe_meta, logger,
)


def fetch_etf_holdings(ticker: str) -> pd.DataFrame:
    """
    Attempt to fetch ETF holdings. yfinance doesn't directly provide
    holdings for all ETFs, so we try multiple approaches.
    """
    import yfinance as yf

    logger.info(f"Fetching holdings for ETF: {ticker}")

    try:
        etf = yf.Ticker(ticker)

        # Try to get holdings from the fund's info
        # yfinance provides top holdings for some ETFs
        info = etf.info or {}
        holdings_data = []

        # Method 1: Check for fund holdings attribute
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
            except Exception:
                pass

        # Method 2: Fall back to basic approach
        if not holdings_data:
            logger.warning(
                f"  Could not auto-fetch holdings for {ticker}. "
                f"Consider adding a manual tickers list to the config, "
                f"or place a holdings.parquet file in data/universes/{ticker}/"
            )
            return pd.DataFrame(columns=["Ticker", "Name", "WeightPct"])

        df = pd.DataFrame(holdings_data)
        # Clean ticker symbols
        df["Ticker"] = df["Ticker"].str.replace(r"[^\w\-.]", "", regex=True)
        df = df[df["Ticker"].str.len() > 0]

        logger.info(f"  Found {len(df)} holdings for {ticker}")
        return df

    except Exception as e:
        logger.error(f"  Error fetching holdings for {ticker}: {e}")
        return pd.DataFrame(columns=["Ticker", "Name", "WeightPct"])


def build_watchlist_holdings(universe: dict) -> pd.DataFrame:
    """Build equal-weight holdings from a watchlist config entry."""
    tickers = universe.get("tickers", [])
    if not tickers:
        return pd.DataFrame(columns=["Ticker", "Name", "WeightPct"])

    weight = round(100.0 / len(tickers), 4)
    records = [{"Ticker": t, "Name": "", "WeightPct": weight} for t in tickers]
    return pd.DataFrame(records)


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

        # Build or fetch holdings
        utype = universe.get("type", "watchlist")
        if utype in ("etf", "index") and universe.get("auto_fetch_holdings"):
            df = fetch_etf_holdings(uid)
        else:
            df = build_watchlist_holdings(universe)

        if df.empty:
            logger.warning(f"  No holdings for {uid}")
            continue

        # Save holdings and meta
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
