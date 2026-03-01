/**
 * Market Intelligence Platform — Shared Application Logic
 */

const DATA_BASE = "./data";

const DataStore = {
  _cache: {},
  async fetch(filename) {
    if (this._cache[filename]) return this._cache[filename];
    try {
      const res = await fetch(`${DATA_BASE}/${filename}`);
      if (!res.ok) throw new Error(`${res.status}`);
      const data = await res.json();
      this._cache[filename] = data;
      return data;
    } catch (err) { console.error(`Failed to load ${filename}:`, err); return null; }
  },
  async getUniverses()    { return this.fetch("universes.json"); },
  async getHoldings(id)   { return this.fetch(`${id}_holdings.json`); },
  async getPrices()       { return this.fetch("prices_1y.json"); },
  async getSentiment()    { return this.fetch("sentiment.json"); },
  async getFundamentals() { return this.fetch("fundamentals.json"); },
};

const State = {
  get(key) { return new URLSearchParams(window.location.search).get(key); },
  set(key, value) {
    const p = new URLSearchParams(window.location.search);
    p.set(key, value);
    history.replaceState(null, "", `?${p.toString()}`);
  },
};

const Charts = {
  createPriceChart(container, priceData, options = {}) {
    if (typeof LightweightCharts === "undefined") { container.innerHTML = '<p class="empty-state">Charts not loaded</p>'; return null; }
    const chart = LightweightCharts.createChart(container, {
      layout: { background: { type: "solid", color: "#161b22" }, textColor: "#8b949e" },
      grid: { vertLines: { color: "#30363d" }, horzLines: { color: "#30363d" } },
      crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
      rightPriceScale: { borderColor: "#30363d" },
      timeScale: { borderColor: "#30363d", timeVisible: false },
      width: container.clientWidth, height: options.height || 400,
    });
    const series = chart.addAreaSeries({
      lineColor: options.color || "#58a6ff",
      topColor: (options.color || "#58a6ff") + "40",
      bottomColor: (options.color || "#58a6ff") + "05",
      lineWidth: 2,
    });
    const d = Object.entries(priceData).map(([date, close]) => ({ time: date, value: close })).sort((a, b) => a.time.localeCompare(b.time));
    series.setData(d);
    chart.timeScale().fitContent();
    new ResizeObserver(() => chart.applyOptions({ width: container.clientWidth })).observe(container);
    return chart;
  },

  createComparisonChart(container, allPrices, tickers) {
    if (typeof LightweightCharts === "undefined") { container.innerHTML = '<p class="empty-state">Charts not loaded</p>'; return null; }
    const colors = ["#58a6ff", "#3fb950", "#f85149", "#d29922", "#bc8cff", "#f778ba"];
    const chart = LightweightCharts.createChart(container, {
      layout: { background: { type: "solid", color: "#161b22" }, textColor: "#8b949e" },
      grid: { vertLines: { color: "#30363d" }, horzLines: { color: "#30363d" } },
      rightPriceScale: { borderColor: "#30363d", scaleMargins: { top: 0.1, bottom: 0.1 } },
      timeScale: { borderColor: "#30363d" },
      width: container.clientWidth, height: 450,
    });
    tickers.forEach((ticker, i) => {
      const pd = allPrices[ticker];
      if (!pd) return;
      const entries = Object.entries(pd).map(([d, c]) => ({ time: d, value: c })).sort((a, b) => a.time.localeCompare(b.time));
      if (!entries.length) return;
      const base = entries[0].value;
      const norm = entries.map(e => ({ time: e.time, value: ((e.value - base) / base) * 100 }));
      const s = chart.addLineSeries({ color: colors[i % colors.length], lineWidth: 2, title: ticker });
      s.setData(norm);
    });
    chart.timeScale().fitContent();
    new ResizeObserver(() => chart.applyOptions({ width: container.clientWidth })).observe(container);
    return chart;
  },
};

const UI = {
  formatNumber(n, d = 2) {
    if (n == null || isNaN(n)) return "—";
    if (Math.abs(n) >= 1e12) return (n / 1e12).toFixed(1) + "T";
    if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(1) + "B";
    if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(1) + "M";
    return n.toFixed(d);
  },
  formatPct(n) { if (n == null || isNaN(n)) return "—"; return `${n >= 0 ? "+" : ""}${(n * 100).toFixed(2)}%`; },
  sentimentClass(l) { if (!l) return "neutral"; if (l.includes("bullish")) return "positive"; if (l.includes("bearish")) return "negative"; return "neutral"; },
  sentimentBadge(l) { if (!l) return "badge-neutral"; if (l.includes("bullish")) return "badge-bullish"; if (l.includes("bearish")) return "badge-bearish"; return "badge-neutral"; },
  showLoading(c) { c.innerHTML = '<div class="loading">Loading</div>'; },
  showEmpty(c, m = "No data available") { c.innerHTML = `<div class="empty-state">${m}</div>`; },
  setActiveNav(page) { document.querySelectorAll(".nav-links a").forEach(a => a.classList.toggle("active", a.dataset.page === page)); },
};

function renderNav() {
  return `<nav class="nav"><div class="nav-inner"><a href="index.html" class="nav-brand"><span>◆</span> Markets</a><div class="nav-links"><a href="index.html" data-page="home">Overview</a><a href="universe.html?id=QQQ" data-page="universe">Universes</a><a href="sentiment.html" data-page="sentiment">Sentiment</a><a href="compare.html" data-page="compare">Compare</a></div></div></nav>`;
}
function renderFooter() {
  return `<footer class="footer">Market Intelligence Platform · Data updated daily via GitHub Actions</footer>`;
}

window.MIP = { DataStore, State, Charts, UI, renderNav, renderFooter };
