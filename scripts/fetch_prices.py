"""
Fetch daily closing prices for ALL known tickers across all universes.

Reads the combined ticker list from universes_config.yml + holdings files,
then fetches/appends to the shared securities/prices.parquet.

Usage:
    python scripts/fetch_prices.py                  # incremental update
    python scripts/fetch_prices.py --backfill 730   # backfill 2 years
"""

import argparse
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

from utils import (
    load_config, get_all_tickers, ensure_dirs,
    SECURITIES_DIR, read_parquet, write_parquet, logger,
)


def fetch_prices(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Fetch closing prices for a list of tickers over a date range."""
    logger.info(f"Fetching prices for {len(tickers)} tickers: {start} → {end}")

    # yfinance handles batches; chunk to avoid timeouts on very large lists
    chunk_size = 50
    frames = []

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        logger.info(f"  Chunk {i // chunk_size + 1}: {len(chunk)} tickers")
        try:
            data = yf.download(
                chunk,
                start=start,
                end=end,
                group_by="ticker",
                auto_adjust=True,
                threads=True,
                progress=False,
            )
            if data.empty:
                logger.warning(f"  No data returned for chunk")
                continue

            # Reshape: multi-level columns → long format
            if len(chunk) == 1:
                # Single ticker: columns are just OHLCV
                df = data[["Close"]].copy()
                df["Ticker"] = chunk[0]
                df = df.reset_index()
                df = df.rename(columns={"Date": "Date", "Close": "Close"})
            else:
                # Multiple tickers: multi-level columns (Ticker, Field)
                records = []
                for ticker in chunk:
                    if ticker in data.columns.get_level_values(0):
                        col = data[ticker]["Close"].dropna()
                        for date, close in col.items():
                            records.append({
                                "Date": date,
                                "Ticker": ticker,
                                "Close": round(float(close), 4),
                            })
                df = pd.DataFrame(records)

            if not df.empty:
                frames.append(df)

        except Exception as e:
            logger.error(f"  Error fetching chunk: {e}")
            continue

    if not frames:
        logger.warning("No price data fetched")
        return pd.DataFrame(columns=["Date", "Ticker", "Close"])

    result = pd.concat(frames, ignore_index=True)
    result["Date"] = pd.to_datetime(result["Date"]).dt.date
    result = result.drop_duplicates(subset=["Date", "Ticker"], keep="last")
    result = result.sort_values(["Ticker", "Date"]).reset_index(drop=True)

    logger.info(f"Fetched {len(result)} price records for {result['Ticker'].nunique()} tickers")
    return result


def main():
    parser = argparse.ArgumentParser(description="Fetch prices for all tracked tickers")
    parser.add_argument("--backfill", type=int, default=0,
                        help="Number of days to backfill (0 = incremental)")
    parser.add_argument("--tickers", nargs="*", default=None,
                        help="Override: fetch only these tickers")
    args = parser.parse_args()

    ensure_dirs()
    config = load_config()

    # Determine tickers
    if args.tickers:
        tickers = args.tickers
    else:
        tickers = get_all_tickers(config)

    if not tickers:
        logger.warning("No tickers to fetch. Check universes_config.yml and holdings files.")
        return

    # Determine date range
    prices_path = SECURITIES_DIR / "prices.parquet"
    existing = read_parquet(prices_path)

    if args.backfill > 0:
        start = (datetime.utcnow() - timedelta(days=args.backfill)).strftime("%Y-%m-%d")
    elif not existing.empty:
        # Incremental: fetch from last known date
        last_date = pd.to_datetime(existing["Date"]).max()
        start = (last_date - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        # First run: default to 2 years
        years = config.get("settings", {}).get("price_history_years", 2)
        start = (datetime.utcnow() - timedelta(days=years * 365)).strftime("%Y-%m-%d")

    end = datetime.utcnow().strftime("%Y-%m-%d")

    # Fetch
    new_prices = fetch_prices(tickers, start, end)
    if new_prices.empty:
        logger.info("No new prices to save")
        return

    # Merge with existing
    if not existing.empty:
        existing["Date"] = pd.to_datetime(existing["Date"]).dt.date
        combined = pd.concat([existing, new_prices], ignore_index=True)
        combined = combined.drop_duplicates(subset=["Date", "Ticker"], keep="last")
        combined = combined.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    else:
        combined = new_prices

    write_parquet(combined, prices_path)
    logger.info(f"Total: {len(combined)} records, {combined['Ticker'].nunique()} tickers")


if __name__ == "__main__":
    main()
