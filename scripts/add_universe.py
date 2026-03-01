"""
Onboard a new universe into the platform.

This is the key workflow script. It:
1. Fetches holdings (if ETF/index) or reads from config (if watchlist)
2. Identifies tickers not already in securities/prices.parquet
3. Backfills price history for new tickers
4. Fetches fundamentals for new tickers
5. Saves universe metadata

Usage:
    python scripts/add_universe.py QQQ          # onboard QQQ
    python scripts/add_universe.py SPY          # onboard SPY
    python scripts/add_universe.py --all        # onboard all from config
"""

import argparse
import pandas as pd
from datetime import datetime, timedelta

from utils import (
    load_config, get_universes, get_settings, ensure_dirs,
    SECURITIES_DIR, read_parquet, logger,
)
from fetch_holdings import fetch_etf_holdings, build_watchlist_holdings
from fetch_prices import fetch_prices
from fetch_fundamentals import fetch_fundamentals
from utils import save_universe_holdings, save_universe_meta, write_parquet


def onboard_universe(universe: dict, config: dict):
    """Full onboarding pipeline for a single universe."""
    uid = universe["id"]
    utype = universe.get("type", "watchlist")
    settings = get_settings(config)
    years = settings.get("price_history_years", 2)

    logger.info(f"{'=' * 60}")
    logger.info(f"Onboarding universe: {uid} (type={utype})")
    logger.info(f"{'=' * 60}")

    # ── Step 1: Get holdings ──
    if utype in ("etf", "index") and universe.get("auto_fetch_holdings"):
        holdings = fetch_etf_holdings(uid)
    else:
        holdings = build_watchlist_holdings(universe)

    if holdings.empty:
        logger.error(f"No holdings found for {uid}. Aborting.")
        return

    save_universe_holdings(uid, holdings)
    save_universe_meta(uid, {
        "id": uid,
        "name": universe.get("name", uid),
        "type": utype,
        "benchmark": universe.get("benchmark", "SPY"),
        "description": universe.get("description", ""),
        "ticker_count": len(holdings),
    })
    logger.info(f"  Step 1 complete: {len(holdings)} holdings saved")

    # ── Step 2: Identify new tickers ──
    new_tickers = holdings["Ticker"].tolist()
    # Include the benchmark
    benchmark = universe.get("benchmark")
    if benchmark and benchmark not in new_tickers:
        new_tickers.append(benchmark)

    prices_path = SECURITIES_DIR / "prices.parquet"
    existing_prices = read_parquet(prices_path)

    if not existing_prices.empty:
        known_tickers = set(existing_prices["Ticker"].unique())
        new_only = [t for t in new_tickers if t not in known_tickers]
        logger.info(f"  Step 2: {len(new_only)} new tickers (of {len(new_tickers)} total)")
    else:
        new_only = new_tickers
        logger.info(f"  Step 2: {len(new_only)} tickers (first run, all are new)")

    # ── Step 3: Backfill prices ──
    if new_only:
        start = (datetime.utcnow() - timedelta(days=years * 365)).strftime("%Y-%m-%d")
        end = datetime.utcnow().strftime("%Y-%m-%d")
        new_prices = fetch_prices(new_only, start, end)

        if not new_prices.empty:
            if not existing_prices.empty:
                existing_prices["Date"] = pd.to_datetime(existing_prices["Date"]).dt.date
                combined = pd.concat([existing_prices, new_prices], ignore_index=True)
                combined = combined.drop_duplicates(subset=["Date", "Ticker"], keep="last")
                combined = combined.sort_values(["Ticker", "Date"]).reset_index(drop=True)
            else:
                combined = new_prices
            write_parquet(combined, prices_path)
            logger.info(f"  Step 3 complete: backfilled {len(new_prices)} price records")
        else:
            logger.warning(f"  Step 3: no prices fetched for new tickers")
    else:
        logger.info(f"  Step 3: skipped (no new tickers to backfill)")

    # ── Step 4: Fetch fundamentals for new tickers ──
    if new_only:
        fund_df = fetch_fundamentals(new_only)
        if not fund_df.empty:
            fund_path = SECURITIES_DIR / "fundamentals.parquet"
            existing_fund = read_parquet(fund_path)
            if not existing_fund.empty:
                combined_fund = pd.concat([existing_fund, fund_df], ignore_index=True)
                combined_fund = combined_fund.drop_duplicates(subset=["Ticker"], keep="last")
            else:
                combined_fund = fund_df
            write_parquet(combined_fund, fund_path)
            logger.info(f"  Step 4 complete: fundamentals for {len(fund_df)} tickers")

    logger.info(f"Universe {uid} onboarded successfully!")


def main():
    parser = argparse.ArgumentParser(description="Onboard a new universe")
    parser.add_argument("universe_id", nargs="?", default=None,
                        help="Universe ID to onboard (e.g., SPY)")
    parser.add_argument("--all", action="store_true",
                        help="Onboard ALL universes from config")
    args = parser.parse_args()

    ensure_dirs()
    config = load_config()
    universes = get_universes(config)

    if args.all:
        for universe in universes:
            onboard_universe(universe, config)
    elif args.universe_id:
        match = [u for u in universes if u["id"] == args.universe_id]
        if not match:
            logger.error(
                f"Universe '{args.universe_id}' not found in config. "
                f"Available: {[u['id'] for u in universes]}"
            )
            return
        onboard_universe(match[0], config)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
