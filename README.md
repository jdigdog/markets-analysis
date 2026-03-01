# Markets Analysis

A universe-agnostic market intelligence platform that tracks ETFs, indexes, and custom watchlists with AI-powered sentiment scoring, professional charting, and automated daily data pipelines.

## Architecture

```
markets-analysis/
├── .github/workflows/          # Modular CI/CD pipelines
│   ├── fetch_data.yml          # Daily: prices, dividends, fundamentals
│   ├── fetch_sentiment.yml     # Daily: Tavily + Claude sentiment scoring
│   ├── fetch_holdings.yml      # Weekly: refresh universe compositions
│   ├── rebuild_reports.yml     # On-demand: regenerate JSON feeds
│   └── add_universe.yml        # Manual: onboard new ETF or watchlist
│
├── data/
│   ├── universes/              # Per-universe holdings + metadata
│   │   ├── QQQ/
│   │   ├── SPY/
│   │   └── TECH_WATCHLIST/
│   ├── securities/             # Shared ticker-level data
│   │   ├── prices.parquet
│   │   ├── dividends.parquet
│   │   ├── fundamentals.parquet
│   │   └── sentiment/
│   └── reports/data/           # JSON feeds consumed by the site
│
├── scripts/                    # Python pipeline scripts
│   ├── utils.py                # Shared config, paths, I/O
│   ├── fetch_prices.py
│   ├── fetch_dividends.py
│   ├── fetch_fundamentals.py
│   ├── fetch_sentiment.py
│   ├── fetch_holdings.py
│   ├── add_universe.py         # Full onboarding pipeline
│   └── generate_feeds.py       # Parquet → JSON for the site
│
├── site/                       # Static site (GitHub Pages)
│   ├── index.html              # Homepage / universe selector
│   ├── universe.html           # Reusable universe detail page
│   ├── ticker.html             # Individual stock deep-dive
│   ├── sentiment.html          # Sentiment heatmap dashboard
│   ├── compare.html            # Cross-universe comparison
│   ├── styles.css              # Design system
│   └── app.js                  # Shared JS: data, charts, UI
│
├── universes_config.yml        # Single source of truth
└── requirements.txt
```

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Onboard all universes from config (fetches holdings + backfills 2Y prices)
python scripts/add_universe.py --all

# 3. Generate JSON feeds for the site
python scripts/generate_feeds.py

# 4. Open site/index.html in a browser (or serve via GitHub Pages)
```

## Configuration

Everything is controlled by `universes_config.yml`:

```yaml
universes:
  - id: QQQ
    type: etf
    auto_fetch_holdings: true

  - id: TECH_WATCHLIST
    type: watchlist
    tickers: [TSM, MSTR, COIN, PLTR, ARM]
```

**To add a new universe:** edit the config, then run:
```bash
python scripts/add_universe.py NEW_UNIVERSE_ID
```

Or trigger the `add_universe.yml` workflow from GitHub Actions.

## Workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `fetch_data.yml` | Daily (weekdays 6AM UTC) | Prices, dividends, fundamentals |
| `fetch_sentiment.yml` | Daily (weekdays 8AM UTC) | AI sentiment scoring |
| `fetch_holdings.yml` | Weekly (Sundays 10AM UTC) | Refresh ETF compositions |
| `rebuild_reports.yml` | Manual / called by others | Regenerate JSON feeds |
| `add_universe.yml` | Manual with input | Onboard new universe |

## Secrets Required

Set these in your GitHub repo settings under **Settings → Secrets → Actions**:

| Secret | Required For |
|--------|-------------|
| `TAVILY_API_KEY` | Sentiment news search |
| `ANTHROPIC_API_KEY` | Sentiment scoring via Claude |

## Charting

The site uses:
- **[TradingView Lightweight Charts](https://tradingview.github.io/lightweight-charts/)** — price and performance charts
- **[Apache ECharts](https://echarts.apache.org/)** — sentiment heatmap and dashboards

## Key Design Decisions

- **Universe-agnostic data model:** Securities (prices, fundamentals, sentiment) are stored once per ticker, shared across all universes.
- **Config-driven pipeline:** Adding a new ETF or watchlist requires zero code changes — just edit `universes_config.yml`.
- **Hybrid static site:** HTML shells + JSON feeds. Fully GitHub Pages compatible, no build step for the frontend.
- **Modular workflows:** Each data task runs independently with its own schedule and trigger.

## License

Private / personal use.
