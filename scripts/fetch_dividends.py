"""
Fetch dividend history for ALL known tickers across all universes.

Usage:
    python scripts/fetch_dividends.py
    python scripts/fetch_dividends.py --tickers AAPL MSFT
"""

import argparse
from datetime import datetime, timedelta, timezone
import yfinance as yf
import pandas as pd

from utils import (
    load_config, get_all_tickers, ensure_dirs,
    SECURITIES_DIR, read_parquet, write_parquet, logger,
)


def fetch_dividends(tickers: list[str], years: int = 2) -> pd.DataFrame:
    """Fetch dividend history for a list of tickers."""
    logger.info(f"Fetching dividends for {len(tickers)} tickers ({years}y lookback)")
    records = []

    for ticker in tickers:
        try:
            t = yf.Ticker(ticker)
            divs = t.dividends
            if divs is None or divs.empty:
                continue

            cutoff = pd.Timestamp(datetime.now(timezone.utc) - timedelta(days=years * 365), tz="UTC")
            divs = divs[divs.index >= cutoff]

            for date, amount in divs.items():
                records.append({
                    "Date": date.date() if hasattr(date, "date") else date,
                    "Ticker": ticker,
                    "Dividend": round(float(amount), 6),
                })
        except Exception as e:
            logger.warning(f"  {ticker}: {e}")
            continue

    if not records:
        return pd.DataFrame(columns=["Date", "Ticker", "Dividend"])

    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset=["Date", "Ticker"], keep="last")
    df = df.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    logger.info(f"Fetched {len(df)} dividend records for {df['Ticker'].nunique()} tickers")
    return df


def main():
    parser = argparse.ArgumentParser(description="Fetch dividends for all tracked tickers")
    parser.add_argument("--tickers", nargs="*", default=None)
    args = parser.parse_args()

    ensure_dirs()
    config = load_config()
    settings = config.get("settings", {})
    years = settings.get("price_history_years", 2)

    tickers = args.tickers or get_all_tickers(config)
    if not tickers:
        logger.warning("No tickers to fetch")
        return

    dividends_path = SECURITIES_DIR / "dividends.parquet"
    existing = read_parquet(dividends_path)

    new_divs = fetch_dividends(tickers, years)
    if new_divs.empty:
        logger.info("No new dividends")
        return

    if not existing.empty:
        existing["Date"] = pd.to_datetime(existing["Date"]).dt.date
        combined = pd.concat([existing, new_divs], ignore_index=True)
        combined = combined.drop_duplicates(subset=["Date", "Ticker"], keep="last")
        combined = combined.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    else:
        combined = new_divs

    write_parquet(combined, dividends_path)


if __name__ == "__main__":
    main()
