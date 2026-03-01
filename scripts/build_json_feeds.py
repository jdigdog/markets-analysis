"""
Generate JSON data feeds consumed by the frontend site.

Reads from shared parquet files and produces compact JSON in
data/reports/data/ that HTML pages fetch at runtime.

Usage:
    python scripts/build_json_feeds.py
"""

import json
from datetime import datetime, timedelta

import pandas as pd

from utils import (
    load_config, get_universes, ensure_dirs,
    UNIVERSES_DIR, SECURITIES_DIR, SITE_DATA_DIR,  # FIX: was REPORTS_DATA_DIR (doesn't exist)
    read_parquet,                                   # FIX: was read_parquet_safe (doesn't exist)
    logger,                                         # FIX: was log (doesn't exist)
)


def write_json(data, filename):
    path = SITE_DATA_DIR / filename
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"), default=str)
    size_kb = path.stat().st_size / 1024
    logger.info(f"Wrote {filename} ({size_kb:.1f} KB)")  # FIX: was log(..., "ok")


def build_universes_index(config):
    universes = []
    for u in get_universes(config):
        uid = u["id"]
        holdings_path = UNIVERSES_DIR / uid / "holdings.parquet"
        ticker_count = 0
        if holdings_path.exists():
            df = pd.read_parquet(holdings_path)
            ticker_count = len(df)
        universes.append({
            "id": uid,
            "name": u.get("name", uid),
            "type": u.get("type", "watchlist"),
            "benchmark": u.get("benchmark", "SPY"),   # FIX: was missing — used by universe.html
            "description": u.get("description", ""),
            "ticker_count": ticker_count,
        })
    write_json(universes, "universes.json")


def build_holdings_feeds(config):
    for u in get_universes(config):
        uid = u["id"]
        path = UNIVERSES_DIR / uid / "holdings.parquet"
        if not path.exists():
            logger.warning(f"No holdings for {uid}, skipping")  # FIX: was log(..., "warn")
            continue
        df = pd.read_parquet(path)
        write_json(df.to_dict(orient="records"), f"{uid}_holdings.json")


def build_prices_feed():
    """
    FIX: The old implementation produced {ticker: {date: price}}.
    The frontend (app.js) expects {dates: [...], tickers: {ticker: [values]}}.
    Also added prices_2y.json — ticker.html fetches it but it was never generated.
    """
    df = read_parquet(SECURITIES_DIR / "prices.parquet")  # FIX: was read_parquet_safe
    if df.empty:
        logger.warning("No price data found; writing empty feeds")
        write_json({"dates": [], "tickers": {}}, "prices_1y.json")
        write_json({"dates": [], "tickers": {}}, "prices_2y.json")
        return

    df["Date"] = pd.to_datetime(df["Date"])

    for label, days in [("1y", 365), ("2y", 730)]:  # FIX: added 2y — ticker.html needs it
        cutoff = datetime.now() - timedelta(days=days)
        subset = df[df["Date"] >= cutoff].copy()
        subset["Date"] = subset["Date"].dt.strftime("%Y-%m-%d")

        # FIX: pivot to the {dates, tickers} shape the frontend expects
        pivot = subset.pivot_table(
            index="Date", columns="Ticker", values="Close", aggfunc="last"
        ).sort_index()

        feed = {
            "dates": list(pivot.index),
            "tickers": {
                ticker: [round(v, 2) if pd.notna(v) else None for v in pivot[ticker]]
                for ticker in pivot.columns
            },
        }
        write_json(feed, f"prices_{label}.json")


def build_sentiment_feed():
    """
    FIX: The old implementation made history a dict keyed by ticker.
    The frontend iterates sentiment.history as a flat array of {Ticker, Date, Score, ...} records.
    """
    latest = read_parquet(SECURITIES_DIR / "sentiment" / "latest.parquet")   # FIX: was read_parquet_safe
    history = read_parquet(SECURITIES_DIR / "sentiment" / "history.parquet") # FIX: was read_parquet_safe
    feed = {"latest": [], "history": []}  # FIX: history is a list, not a dict

    if not latest.empty:
        feed["latest"] = latest.to_dict(orient="records")

    if not history.empty:
        history["Date"] = pd.to_datetime(history["Date"])
        eight_weeks_ago = datetime.now() - timedelta(weeks=8)
        recent = history[history["Date"] >= eight_weeks_ago].copy()
        recent["Date"] = recent["Date"].dt.strftime("%Y-%m-%d")
        # FIX: return as flat list of records, not grouped dict
        feed["history"] = recent.to_dict(orient="records")

    write_json(feed, "sentiment.json")


def build_fundamentals_feed():
    df = read_parquet(SECURITIES_DIR / "fundamentals.parquet")  # FIX: was read_parquet_safe
    if df.empty:
        write_json([], "fundamentals.json")
        return
    df = df.where(pd.notna(df), None)
    write_json(df.to_dict(orient="records"), "fundamentals.json")


def main():
    ensure_dirs()
    config = load_config()
    logger.info("Building JSON data feeds")  # FIX: was log(...)
    build_universes_index(config)
    build_holdings_feeds(config)
    build_prices_feed()
    build_sentiment_feed()
    build_fundamentals_feed()
    logger.info("All feeds generated")  # FIX: was log(..., "ok")


if __name__ == "__main__":
    main()
