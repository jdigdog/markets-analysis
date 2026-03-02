"""
Generate JSON data feeds for the static site.

Reads from data/securities/ and data/universes/, writes to
data/reports/data/. These JSON files are consumed client-side.

Usage:
    python scripts/generate_feeds.py
"""

import json
import pandas as pd
from pathlib import Path

from utils import (
    load_config, get_universes, get_all_tickers, ensure_dirs,
    UNIVERSES_DIR, SECURITIES_DIR, SITE_DATA_DIR,
    read_parquet, write_json, logger,
)


def generate_universes_index(config):
    universes = get_universes(config)
    index = []
    for u in universes:
        uid = u["id"]
        meta_path = UNIVERSES_DIR / uid / "meta.json"
        holdings_path = UNIVERSES_DIR / uid / "holdings.parquet"
        meta = {}
        if meta_path.exists():
            with open(meta_path) as f:
                meta = json.load(f)
        ticker_count = 0
        if holdings_path.exists():
            ticker_count = len(pd.read_parquet(holdings_path))
        index.append({
            "id": uid,
            "name": meta.get("name", u.get("name", uid)),
            "type": meta.get("type", u.get("type", "watchlist")),
            "benchmark": meta.get("benchmark", u.get("benchmark", "SPY")),
            "description": meta.get("description", u.get("description", "")),
            "ticker_count": ticker_count,
        })
    write_json(index, SITE_DATA_DIR / "universes.json")


def generate_holdings_feeds(config):
    # Non-equity placeholders Invesco includes (cash lines, currency hedges, etc.)
    EXCLUDE_TICKERS = {"USD", "EUR", "GBP", "JPY", "CASH", "OTHER", "XTSLA"}

    for u in get_universes(config):
        uid = u["id"]
        hp = UNIVERSES_DIR / uid / "holdings.parquet"
        if not hp.exists():
            continue
        df = pd.read_parquet(hp)

        # Drop known non-equity placeholders
        df = df[~df["Ticker"].isin(EXCLUDE_TICKERS)].copy()

        fund = read_parquet(SECURITIES_DIR / "fundamentals.parquet")
        if not fund.empty:
            cols = [c for c in ["Ticker", "Sector", "Industry", "MarketCap", "PE", "Name"] if c in fund.columns]
            df = df.merge(fund[cols], on="Ticker", how="left", suffixes=("", "_fund"))
            if "Name_fund" in df.columns:
                df["Name"] = df["Name"].fillna(df["Name_fund"])
                df = df.drop(columns=["Name_fund"])

        # FIX: NaN is not valid JSON — browser JSON.parse() throws SyntaxError.
        # Must cast to object dtype first; float columns silently re-coerce None→NaN.
        df = df.astype(object).where(pd.notnull(df), other=None)

        write_json(df.to_dict(orient="records"), SITE_DATA_DIR / f"{uid}_holdings.json")


def generate_prices_feed():
    prices = read_parquet(SECURITIES_DIR / "prices.parquet")
    if prices.empty:
        logger.warning("No price data"); return
    prices["Date"] = pd.to_datetime(prices["Date"])
    for label, days in [("1y", 365), ("2y", 730)]:
        cutoff = prices["Date"].max() - pd.Timedelta(days=days)
        subset = prices[prices["Date"] >= cutoff]
        pivot = subset.pivot_table(index="Date", columns="Ticker", values="Close").sort_index()
        feed = {
            "dates": [d.strftime("%Y-%m-%d") for d in pivot.index],
            "tickers": {t: [round(v, 2) if pd.notna(v) else None for v in pivot[t]] for t in pivot.columns},
        }
        write_json(feed, SITE_DATA_DIR / f"prices_{label}.json", indent=None)


def _df_to_records(df: pd.DataFrame) -> list:
    """Convert DataFrame to JSON-safe records (NaN → null, not NaN literal).
    Must cast to object dtype first — float columns silently re-coerce None→NaN.
    """
    return df.astype(object).where(pd.notnull(df), other=None).to_dict(orient="records")


def generate_sentiment_feed():
    latest = read_parquet(SECURITIES_DIR / "sentiment" / "latest.parquet")
    history = read_parquet(SECURITIES_DIR / "sentiment" / "history.parquet")
    feed = {"latest": [], "history": []}
    if not latest.empty:
        feed["latest"] = _df_to_records(latest)
    if not history.empty:
        history["Date"] = pd.to_datetime(history["Date"])
        cutoff = history["Date"].max() - pd.Timedelta(weeks=8)
        feed["history"] = _df_to_records(history[history["Date"] >= cutoff])
    write_json(feed, SITE_DATA_DIR / "sentiment.json")


def generate_fundamentals_feed():
    fund = read_parquet(SECURITIES_DIR / "fundamentals.parquet")
    write_json(_df_to_records(fund) if not fund.empty else [], SITE_DATA_DIR / "fundamentals.json")


def main():
    ensure_dirs()
    config = load_config()
    logger.info("Generating JSON data feeds...")
    generate_universes_index(config)
    generate_holdings_feeds(config)
    generate_prices_feed()
    generate_sentiment_feed()
    generate_fundamentals_feed()
    logger.info("All feeds generated")


if __name__ == "__main__":
    main()
