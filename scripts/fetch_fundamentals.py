"""
Fetch fundamental data for ALL known tickers across all universes.

Collects: PE ratio, forward PE, EPS, revenue, market cap, sector,
industry, 52-week high/low, analyst target, and recommendation.

Usage:
    python scripts/fetch_fundamentals.py
    python scripts/fetch_fundamentals.py --tickers AAPL MSFT
"""

import argparse
import yfinance as yf
import pandas as pd

from utils import (
    load_config, get_all_tickers, ensure_dirs,
    SECURITIES_DIR, write_parquet, logger, today_str,
)


FUNDAMENTAL_FIELDS = {
    "trailingPE": "PE",
    "forwardPE": "ForwardPE",
    "trailingEps": "EPS",
    "forwardEps": "ForwardEPS",
    "totalRevenue": "Revenue",
    "marketCap": "MarketCap",
    "sector": "Sector",
    "industry": "Industry",
    "fiftyTwoWeekHigh": "High52W",
    "fiftyTwoWeekLow": "Low52W",
    "targetMeanPrice": "AnalystTarget",
    "recommendationKey": "Recommendation",
    "dividendYield": "DividendYield",
    "beta": "Beta",
    "shortName": "Name",
}


def fetch_fundamentals(tickers: list[str]) -> pd.DataFrame:
    """Fetch fundamental snapshot for each ticker."""
    logger.info(f"Fetching fundamentals for {len(tickers)} tickers")
    records = []

    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).info
            if not info or info.get("quoteType") is None:
                logger.warning(f"  {ticker}: no info available")
                continue

            row = {"Ticker": ticker, "FetchDate": today_str()}
            for yf_key, col_name in FUNDAMENTAL_FIELDS.items():
                val = info.get(yf_key)
                if val is not None:
                    row[col_name] = val
            records.append(row)

        except Exception as e:
            logger.warning(f"  {ticker}: {e}")
            continue

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    logger.info(f"Fetched fundamentals for {len(df)} tickers")
    return df


def main():
    parser = argparse.ArgumentParser(description="Fetch fundamentals for all tracked tickers")
    parser.add_argument("--tickers", nargs="*", default=None)
    args = parser.parse_args()

    ensure_dirs()
    config = load_config()
    tickers = args.tickers or get_all_tickers(config)

    if not tickers:
        logger.warning("No tickers to fetch")
        return

    df = fetch_fundamentals(tickers)
    if df.empty:
        logger.info("No fundamentals fetched")
        return

    write_parquet(df, SECURITIES_DIR / "fundamentals.parquet")


if __name__ == "__main__":
    main()
