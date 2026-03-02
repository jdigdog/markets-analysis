/**
 * Market Intelligence Platform — Shared Application Module
 */
(function () {

var CONFIG = {
  dataDir: "../data/reports/data",
  defaultUniverse: "QQQ",
};

// ── Data Layer ───────────────────────────────────────────────
var DataStore = {
  cache: {},
  fetch: async function(filename) {
    if (this.cache[filename]) return this.cache[filename];
    try {
      var resp = await window.fetch(CONFIG.dataDir + "/" + filename);
      if (!resp.ok) throw new Error(resp.status);
      var data = await resp.json();
      this.cache[filename] = data;
      return data;
    } catch (err) {
      console.error("Failed to load " + filename + ":", err);
      return null;
    }
  },
  getUniverses: async function()      { return this.fetch("universes.json"); },
  getHoldings:  async function(uid)   { return this.fetch(uid + "_holdings.json"); },
  getPrices:    async function(p)     { return this.fetch("prices_" + (p || "1y") + ".json"); },
  getSentiment: async function()      { return this.fetch("sentiment.json"); },
  getFundamentals: async function()   { return this.fetch("fundamentals.json"); },
};

// ── Utilities ────────────────────────────────────────────────
function formatNumber(n, d) {
  d = d == null ? 2 : d;
  if (n == null || isNaN(n)) return "\u2014";
  if (Math.abs(n) >= 1e12) return (n / 1e12).toFixed(1) + "T";
  if (Math.abs(n) >= 1e9)  return (n / 1e9).toFixed(1) + "B";
  if (Math.abs(n) >= 1e6)  return (n / 1e6).toFixed(1) + "M";
  return n.toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d });
}
function formatPercent(n, d) {
  d = d == null ? 2 : d;
  if (n == null || isNaN(n)) return "\u2014";
  return (n > 0 ? "+" : "") + n.toFixed(d) + "%";
}
function sentimentColor(s) {
  if (s >= 0.3)  return "var(--accent-green)";
  if (s >= 0.1)  return "#66bb6a";
  if (s <= -0.3) return "var(--accent-red)";
  if (s <= -0.1) return "#ef5350";
  return "var(--text-muted)";
}
function sentimentBadgeClass(label) {
  if (!label) return "badge-muted";
  if (label.includes("bullish")) return "badge-green";
  if (label.includes("bearish")) return "badge-red";
  return "badge-muted";
}
function getUrlParam(key) {
  return new URLSearchParams(window.location.search).get(key);
}

// ── Charts ───────────────────────────────────────────────────
var Charts = {
  createPriceChart: function(containerId, _unused, opts) {
    opts = opts || {};
    var el = document.getElementById(containerId);
    if (!el || typeof LightweightCharts === "undefined") return null;
    el.innerHTML = "";
    var chart = LightweightCharts.createChart(el, {
      width: el.clientWidth,
      height: opts.height || 400,
      layout: {
        background: { color: "#1c2128" },
        textColor: "#8b949e",
        fontFamily: "'Inter', sans-serif",
      },
      grid: {
        vertLines: { color: "#21262d" },
        horzLines: { color: "#21262d" },
      },
      crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
      rightPriceScale: { borderColor: "#30363d" },
      timeScale: { borderColor: "#30363d", timeVisible: false },
    });
    new ResizeObserver(function() {
      chart.applyOptions({ width: el.clientWidth });
    }).observe(el);
    return chart;
  },

  addLineSeries: function(chart, data, opts) {
    opts = opts || {};
    // NOTE: "percent" is NOT a valid LightweightCharts v4 priceFormat type.
    // Use "custom" formatter for percent display.
    var priceFormat = opts.percent
      ? { type: "custom", formatter: function(v) { return (v >= 0 ? "+" : "") + Number(v).toFixed(2) + "%"; }, minMove: 0.01 }
      : { type: "price", precision: 2, minMove: 0.01 };

    var seriesOpts = Object.assign({
      color: opts.color || "#58a6ff",
      lineWidth: opts.lineWidth || 2,
      crosshairMarkerVisible: true,
      priceFormat: priceFormat,
    }, opts.seriesOptions || {});

    var s = chart.addLineSeries(seriesOpts);
    s.setData(data);
    return s;
  },

  normalisePerformance: function(dates, prices) {
    var r = [];
    var base = null;
    for (var i = 0; i < dates.length; i++) {
      if (prices[i] == null) continue;
      if (base === null) base = prices[i];
      r.push({ time: dates[i], value: ((prices[i] - base) / base) * 100 });
    }
    return r;
  },
};

// ── UI Components ────────────────────────────────────────────
var UI = {
  renderUniverseCards: async function(containerId) {
    var el = document.getElementById(containerId);
    if (!el) return;
    el.innerHTML = '<div class="loading">Loading universes...</div>';
    var universes = await DataStore.getUniverses();
    if (!universes) { el.innerHTML = '<div class="empty-state">No data</div>'; return; }
    el.innerHTML = universes.map(function(u) {
      return '<a href="universe.html?id=' + u.id + '" class="card" style="text-decoration:none">'
        + '<div class="card-header">'
        + '<div>'
        + '<div class="card-title">' + (u.name || u.id) + '</div>'
        + '<div class="card-subtitle">' + u.type.toUpperCase() + ' \u00B7 ' + u.ticker_count + ' holdings</div>'
        + '</div>'
        + '<span class="badge badge-blue">' + u.id + '</span>'
        + '</div>'
        + '<div class="card-body">' + (u.description || "") + '</div>'
        + '</a>';
    }).join("");
  },

  renderHoldingsTable: function(containerId, holdings) {
    var el = document.getElementById(containerId);
    if (!el) return;
    if (!holdings || !holdings.length) {
      el.innerHTML = '<div class="empty-state">No holdings</div>';
      return;
    }
    var rows = holdings
      .sort(function(a, b) { return (b.WeightPct || 0) - (a.WeightPct || 0); })
      .slice(0, 50)
      .map(function(h) {
        return '<tr>'
          + '<td><a href="ticker.html?t=' + h.Ticker + '">' + h.Ticker + '</a></td>'
          + '<td>' + (h.Name || "\u2014") + '</td>'
          + '<td class="text-right text-mono">' + formatNumber(h.WeightPct) + '%</td>'
          + '<td>' + (h.Sector || "\u2014") + '</td>'
          + '<td class="text-right text-mono">' + (h.PE ? formatNumber(h.PE, 1) : "\u2014") + '</td>'
          + '<td class="text-right text-mono">' + (h.MarketCap ? formatNumber(h.MarketCap) : "\u2014") + '</td>'
          + '</tr>';
      }).join("");
    el.innerHTML = '<div class="table-wrap"><table>'
      + '<thead><tr><th>Ticker</th><th>Name</th><th class="text-right">Weight</th>'
      + '<th>Sector</th><th class="text-right">P/E</th><th class="text-right">Mkt Cap</th></tr></thead>'
      + '<tbody>' + rows + '</tbody></table></div>';
  },

  renderNav: function(activePage) {
    var nav = document.getElementById("nav");
    if (!nav) return;
    var pages = [
      { id: "home",      label: "Overview",      href: "index.html" },
      { id: "relative",  label: "Relative Perf", href: "relative.html" },
      { id: "sentiment", label: "Sentiment",      href: "sentiment.html" },
      { id: "compare",   label: "Compare",        href: "compare.html" },
    ];
    nav.innerHTML = '<nav class="nav"><div class="nav-inner">'
      + '<div class="nav-brand">'
      + '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">'
      + '<path d="M3 3v18h18"/><path d="m7 16 4-8 4 4 4-8"/>'
      + '</svg>Markets Analysis</div>'
      + '<ul class="nav-links">'
      + pages.map(function(p) {
          return '<li><a href="' + p.href + '" class="' + (activePage === p.id ? "active" : "") + '">' + p.label + '</a></li>';
        }).join("")
      + '</ul></div></nav>';
  },
};

// ── Exports ──────────────────────────────────────────────────
window.MIP = {
  CONFIG: CONFIG,
  DataStore: DataStore,
  Charts: Charts,
  UI: UI,
  formatNumber: formatNumber,
  formatPercent: formatPercent,
  sentimentColor: sentimentColor,
  sentimentBadgeClass: sentimentBadgeClass,
  getUrlParam: getUrlParam,
};

}()); // end IIFE
