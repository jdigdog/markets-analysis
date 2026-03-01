"""
Fetch and score sentiment for ALL known tickers using Tavily + Claude.

This is the most API-heavy (and expensive) step. It:
1. Searches recent news for each ticker via Tavily
2. Sends headlines/snippets to Claude for sentiment scoring
3. Saves results to securities/sentiment/latest.parquet and appends to history

Environment variables required:
    TAVILY_API_KEY     — Tavily search API key
    ANTHROPIC_API_KEY  — Anthropic API key

Usage:
    python scripts/fetch_sentiment.py
    python scripts/fetch_sentiment.py --tickers AAPL MSFT
    python scripts/fetch_sentiment.py --universe QQQ
"""

import os
import json
import argparse
import pandas as pd
from datetime import datetime

from utils import (
    load_config, get_all_tickers, ensure_dirs,
    SECURITIES_DIR, UNIVERSES_DIR,
    read_parquet, write_parquet, logger, today_str,
)

TAVILY_API_KEY = os.environ.get("TAVILY_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")


def search_news(ticker: str, max_results: int = 5) -> list[dict]:
    """Search recent news for a ticker using Tavily."""
    if not TAVILY_API_KEY:
        logger.warning("TAVILY_API_KEY not set — skipping news search")
        return []

    try:
        from tavily import TavilyClient
        client = TavilyClient(api_key=TAVILY_API_KEY)
        response = client.search(
            query=f"{ticker} stock news",
            search_depth="basic",
            max_results=max_results,
            include_answer=False,
        )
        results = response.get("results", [])
        return [
            {
                "title": r.get("title", ""),
                "snippet": r.get("content", "")[:300],
                "url": r.get("url", ""),
            }
            for r in results
        ]
    except Exception as e:
        logger.warning(f"  Tavily search failed for {ticker}: {e}")
        return []


def score_sentiment(ticker: str, articles: list[dict]) -> dict:
    """Use Claude to score sentiment from news articles."""
    if not ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY not set — skipping sentiment scoring")
        return {}

    if not articles:
        return {}

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

        headlines = "\n".join(
            f"- {a['title']}: {a['snippet'][:150]}" for a in articles
        )

        prompt = f"""Analyze the sentiment of these recent news articles about {ticker} stock.

{headlines}

Respond with ONLY a JSON object (no markdown, no explanation):
{{
    "score": <float from -1.0 (very bearish) to 1.0 (very bullish)>,
    "label": "<one of: very_bearish, bearish, neutral, bullish, very_bullish>",
    "summary": "<one sentence summary of overall sentiment>",
    "article_count": {len(articles)}
}}"""

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )

        text = response.content[0].text.strip()
        # Strip markdown fences if present
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        return json.loads(text)

    except Exception as e:
        logger.warning(f"  Claude scoring failed for {ticker}: {e}")
        return {}


def main():
    parser = argparse.ArgumentParser(description="Fetch and score sentiment")
    parser.add_argument("--tickers", nargs="*", default=None)
    parser.add_argument("--universe", type=str, default=None,
                        help="Process only tickers in this universe")
    args = parser.parse_args()

    ensure_dirs()
    config = load_config()

    # Determine tickers
    if args.tickers:
        tickers = args.tickers
    elif args.universe:
        holdings_path = UNIVERSES_DIR / args.universe / "holdings.parquet"
        if holdings_path.exists():
            df = pd.read_parquet(holdings_path)
            tickers = df["Ticker"].tolist()
        else:
            logger.error(f"Holdings not found for {args.universe}")
            return
    else:
        tickers = get_all_tickers(config)

    if not tickers:
        logger.warning("No tickers to process")
        return

    logger.info(f"Scoring sentiment for {len(tickers)} tickers")
    records = []
    date = today_str()

    for ticker in tickers:
        logger.info(f"  Processing {ticker}...")
        articles = search_news(ticker)
        if not articles:
            logger.info(f"    No articles found")
            continue

        result = score_sentiment(ticker, articles)
        if not result:
            continue

        records.append({
            "Date": date,
            "Ticker": ticker,
            "Score": result.get("score", 0.0),
            "Label": result.get("label", "neutral"),
            "Summary": result.get("summary", ""),
            "ArticleCount": result.get("article_count", 0),
        })

    if not records:
        logger.info("No sentiment data collected")
        return

    latest = pd.DataFrame(records)
    sentiment_dir = SECURITIES_DIR / "sentiment"

    # Save latest snapshot
    write_parquet(latest, sentiment_dir / "latest.parquet")

    # Append to rolling history
    history_path = sentiment_dir / "history.parquet"
    history = read_parquet(history_path)
    if not history.empty:
        combined = pd.concat([history, latest], ignore_index=True)
        combined = combined.drop_duplicates(subset=["Date", "Ticker"], keep="last")
        combined = combined.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    else:
        combined = latest
    write_parquet(combined, history_path)

    logger.info(f"Sentiment complete: {len(records)} tickers scored")


if __name__ == "__main__":
    main()
