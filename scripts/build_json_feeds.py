"""
Generate JSON data feeds consumed by the frontend site.

Reads from shared parquet files and produces compact JSON in
data/reports/data/ that HTML pages fetch at runtime.

Usage:
    python scripts/build_json_feeds.py
"""

import json
import sys
from datetime import datetime, timedelta

import pandas as pd

from utils import (
    load_config, get_universes, get_all_tickers, ensure_dirs,
    UNIVERSES_DIR, SECURITIES_DIR, REPORTS_DATA_DIR,
    read_parquet_safe, log,
)


def write_json(data, filename):
    path = REPORTS_DATA_DIR / filename
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"), default=str)
    log(f"Wrote {filename} ({path.stat().st_size / 1024:.1f} KB)", "ok")


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
            "description": u.get("description", ""),
            "ticker_count": ticker_count,
        })
    write_json(universes, "universes.json")


def build_holdings_feeds(config):
    for u in get_universes(config):
        uid = u["id"]
        path = UNIVERSES_DIR / uid / "holdings.parquet"
        if not path.exists():
            log(f"No holdings for {uid}, skipping", "warn")
            continue
        df = pd.read_parquet(path)
        write_json(df.to_dict(orient="records"), f"{uid}_holdings.json")


def build_prices_feed():
    df = read_parquet_safe(SECURITIES_DIR / "prices.parquet")
    if df.empty:
        write_json({}, "prices_1y.json")
        return
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    df["Date"] = pd.to_datetime(df["Date"])
    df = df[df["Date"] >= one_year_ago].copy()
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    result = {}
    for ticker, group in df.groupby("Ticker"):
        result[ticker] = dict(zip(group["Date"], group["Close"].round(2)))
    write_json(result, "prices_1y.json")


def build_sentiment_feed():
    latest = read_parquet_safe(SECURITIES_DIR / "sentiment" / "latest.parquet")
    history = read_parquet_safe(SECURITIES_DIR / "sentiment" / "history.parquet")
    feed = {"latest": [], "history": {}}
    if not latest.empty:
        feed["latest"] = latest.to_dict(orient="records")
    if not history.empty:
        history["Date"] = pd.to_datetime(history["Date"])
        eight_weeks_ago = datetime.now() - timedelta(weeks=8)
        recent = history[history["Date"] >= eight_weeks_ago].copy()
        recent["Date"] = recent["Date"].dt.strftime("%Y-%m-%d")
        for ticker, group in recent.groupby("Ticker"):
            feed["history"][ticker] = group[["Date", "Score", "Label"]].to_dict(orient="records")
    write_json(feed, "sentiment.json")


def build_fundamentals_feed():
    df = read_parquet_safe(SECURITIES_DIR / "fundamentals.parquet")
    if df.empty:
        write_json([], "fundamentals.json")
        return
    df = df.where(pd.notna(df), None)
    write_json(df.to_dict(orient="records"), "fundamentals.json")


def main():
    ensure_dirs()
    config = load_config()
    log("Building JSON data feeds")
    build_universes_index(config)
    build_holdings_feeds(config)
    build_prices_feed()
    build_sentiment_feed()
    build_fundamentals_feed()
    log("All feeds generated", "ok")


if __name__ == "__main__":
    main()
