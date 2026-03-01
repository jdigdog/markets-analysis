"""
Shared utilities for the Market Intelligence Platform.

Handles config loading, path resolution, and data I/O so that
every script works with the same universe-agnostic data model.
"""

import os
import json
import yaml
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# ── Paths ─────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "universes_config.yml"
DATA_DIR = ROOT / "data"
UNIVERSES_DIR = DATA_DIR / "universes"
SECURITIES_DIR = DATA_DIR / "securities"
REPORTS_DIR = DATA_DIR / "reports"
SITE_DATA_DIR = REPORTS_DIR / "data"

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("markets")


def load_config() -> dict:
    """Load and validate universes_config.yml."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found: {CONFIG_PATH}")
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    if "universes" not in config:
        raise ValueError("Config missing 'universes' key")
    return config


def get_universes(config: dict = None) -> list[dict]:
    """Return list of universe definitions from config."""
    if config is None:
        config = load_config()
    return config["universes"]


def get_settings(config: dict = None) -> dict:
    """Return global settings from config."""
    if config is None:
        config = load_config()
    return config.get("settings", {})


def get_all_tickers(config: dict = None) -> list[str]:
    """
    Build the combined, deduplicated ticker list across ALL universes.
    For ETFs with auto_fetch_holdings, reads from their holdings.parquet.
    For watchlists, reads the tickers list directly from config.
    """
    if config is None:
        config = load_config()

    tickers = set()
    for universe in config["universes"]:
        uid = universe["id"]
        utype = universe.get("type", "watchlist")

        if utype in ("etf", "index") and universe.get("auto_fetch_holdings"):
            holdings_path = UNIVERSES_DIR / uid / "holdings.parquet"
            if holdings_path.exists():
                df = pd.read_parquet(holdings_path)
                tickers.update(df["Ticker"].tolist())
            else:
                logger.warning(f"Holdings not found for {uid}: {holdings_path}")
        elif "tickers" in universe:
            tickers.update(universe["tickers"])

        # Also add the benchmark ticker itself (e.g., QQQ, SPY)
        benchmark = universe.get("benchmark")
        if benchmark:
            tickers.add(benchmark)

    return sorted(tickers)


def ensure_dirs():
    """Create all required data directories if they don't exist."""
    for d in [
        UNIVERSES_DIR,
        SECURITIES_DIR,
        SECURITIES_DIR / "sentiment",
        REPORTS_DIR,
        SITE_DATA_DIR,
    ]:
        d.mkdir(parents=True, exist_ok=True)


def read_parquet(path: Path) -> pd.DataFrame:
    """Read a parquet file, returning empty DataFrame if it doesn't exist."""
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def write_parquet(df: pd.DataFrame, path: Path):
    """Write DataFrame to parquet, creating parent dirs as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    logger.info(f"Wrote {len(df)} rows → {path}")


def write_json(data, path: Path, indent: int = 2):
    """Write data to JSON, creating parent dirs as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=indent, default=str)
    logger.info(f"Wrote JSON → {path}")


def universe_dir(universe_id: str) -> Path:
    """Return the data directory for a specific universe."""
    return UNIVERSES_DIR / universe_id


def save_universe_meta(universe_id: str, meta: dict):
    """Save meta.json for a universe."""
    d = universe_dir(universe_id)
    d.mkdir(parents=True, exist_ok=True)
    write_json(meta, d / "meta.json")


def save_universe_holdings(universe_id: str, df: pd.DataFrame):
    """Save holdings.parquet for a universe."""
    d = universe_dir(universe_id)
    d.mkdir(parents=True, exist_ok=True)
    write_parquet(df, d / "holdings.parquet")


def today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")
