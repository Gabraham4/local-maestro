#!/usr/bin/env python3
"""
Local Maestro — Interactive Web Server

Launches a local web server with an interactive setup page where you can:
- Pull strategies from your Composer portfolio, watchlist, or drafts
- Load from local backtest cache files
- Check/uncheck which strategies to include
- See which strategy limits the backtest window
- Pick time period presets (All Time, 3Y, 1Y, YTD, 6M, 3M, Custom)
- Run the analysis and view the interactive dashboard

Usage:
    python server.py                  # Start on default port 8080
    python server.py --port 9090      # Custom port
"""

import argparse
import base64
import json
import os
import re
import sys
import threading
import urllib.request
import webbrowser
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# Add lib to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "lib"))

from data_loader import load_composer_backtest_json, align_strategies, stitch_hybrid_backtest, stitch_hybrid_portfolio
from analytics import PortfolioAnalyzer, optimize_portfolios
from report import generate_html


# ── Credentials ──────────────────────────────────────────────────────

def load_credentials():
    """Load Composer API credentials from shared scripts/.env file."""
    search = Path(__file__).resolve().parent
    for _ in range(10):
        env_path = search / "scripts" / ".env"
        if env_path.exists():
            creds = {}
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, val = line.split("=", 1)
                        creds[key.strip()] = val.strip()
            return (creds.get("COMPOSER_API_KEY", ""),
                    creds.get("COMPOSER_API_SECRET", ""),
                    creds.get("COMPOSER_ACCOUNT_UUID", ""))
        search = search.parent
    return "", "", ""


API_KEY, API_SECRET, ACCOUNT_UUID = load_credentials()
ACCOUNT_UUID = ACCOUNT_UUID or os.getenv("COMPOSER_ACCOUNT_UUID", "")

# Auto-detect Rainboy backtester path
if not os.environ.get("RAINBOY_BACKTEST_PATH"):
    for candidate in [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Rainboy CLI Backtester", "backtest.sh"),
        os.path.expanduser("~/Rainboy CLI Backtester/backtest.sh"),
    ]:
        if os.path.exists(candidate):
            os.environ["RAINBOY_BACKTEST_PATH"] = os.path.abspath(candidate)
            break

BACKTEST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backtest_data")
REPORTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
os.makedirs(BACKTEST_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)


# ── Composer API Helpers ─────────────────────────────────────────────

def composer_api_get(host, path):
    """Make authenticated GET request to a Composer API host."""
    url = f"https://{host}{path}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-api-key-id": API_KEY,
        "Authorization": f"Bearer {API_SECRET}",
        "x-origin": "public-api",
    }
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _parse_symphony_list(data, source):
    """Extract symphony list from API response."""
    items = data.get("symphonies", data if isinstance(data, list) else [])
    symphonies = []
    for s in items:
        symphonies.append({
            "id": s.get("id", s.get("symphony_id", "")),
            "name": s.get("name", "Unknown"),
            "source": source,
        })
    return symphonies


def list_portfolio_symphonies():
    """List all symphonies in the user's active portfolio."""
    data = composer_api_get(
        "api.composer.trade",
        f"/api/v0.1/portfolio/accounts/{ACCOUNT_UUID}/symphony-stats-meta"
    )
    return _parse_symphony_list(data, "portfolio")


def fetch_symphony_code(symphony_id):
    """Fetch a symphony's strategy code tree (step/children JSON) from the Composer API."""
    try:
        data = composer_api_get(
            "backtest-api.composer.trade",
            f"/api/v1/symphonies/{symphony_id}/score?score_version=v1"
        )
        if data.get("step") == "root":
            return data
        print(f"  [fetch-code] Unexpected response for {symphony_id}: step={data.get('step')}")
        return None
    except Exception as e:
        print(f"  [fetch-code] Failed for {symphony_id}: {e}")
        return None


def list_watchlist_symphonies():
    """List all symphonies in the user's watchlist."""
    try:
        data = composer_api_get("backtest-api.composer.trade", "/api/v1/watchlist")
        return _parse_symphony_list(data, "watchlist")
    except Exception as e:
        print(f"  Watchlist fetch error: {e}")
        return []


def list_draft_symphonies():
    """List all draft symphonies."""
    try:
        data = composer_api_get("backtest-api.composer.trade", "/api/v1/user/symphonies/drafts")
        return _parse_symphony_list(data, "drafts")
    except Exception as e:
        print(f"  Drafts fetch error: {e}")
        return []


def fetch_backtest(symphony_id, force_refresh=False):
    """Fetch backtest data for a symphony. Caches for the current server session only."""
    cache_path = os.path.join(BACKTEST_DIR, f"{symphony_id}.json")

    # Use cache only if from this server session (less than 30 minutes old)
    # This avoids re-fetching the same strategy multiple times in one analysis
    # but ensures fresh data across sessions
    if not force_refresh and os.path.exists(cache_path):
        mtime = os.path.getmtime(cache_path)
        if (datetime.now().timestamp() - mtime) < 1800:  # 30 min
            with open(cache_path) as f:
                return json.load(f)

    print(f"  [fetch] Fetching backtest for {symphony_id}...")

    # Fetch from API
    body = json.dumps({
        "capital": 10000,
        "slippage_percent": 0.0001,
        "spread_markup": 0.002,
        "apply_reg_fee": True,
        "apply_taf_fee": True,
        "benchmark_tickers": ["SPY"],
    }).encode()

    # Try public endpoint first, then authenticated
    data = None
    last_error = None
    for url, hdrs in [
        (f"https://backtest-api.composer.trade/api/v1/public/symphonies/{symphony_id}/backtest",
         {"Content-Type": "application/json"}),
        (f"https://backtest-api.composer.trade/api/v1/symphonies/{symphony_id}/backtest",
         {"Content-Type": "application/json", "x-api-key-id": API_KEY,
          "Authorization": f"Bearer {API_SECRET}", "x-origin": "public-api"}),
    ]:
        req = urllib.request.Request(url, data=body, headers=hdrs, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read().decode())
            break
        except Exception as e:
            last_error = e
            err_body = ""
            if hasattr(e, 'read'):
                try:
                    err_body = e.read().decode()[:200]
                except Exception:
                    pass
            print(f"  [fetch] {symphony_id}: {type(e).__name__}: {e} {err_body}")
            continue

    if not data:
        print(f"  [fetch] FAILED {symphony_id}: {last_error}")
        return None

    print(f"  [fetch] OK {symphony_id}")
    cache = {"symphony_id": symphony_id, "cached_at": datetime.now().isoformat(), "backtest": data}
    with open(cache_path, "w") as f:
        json.dump(cache, f)
    return cache


def get_strategy_date_range(cache_data):
    """Extract date range from cached backtest data."""
    bt = cache_data.get("backtest", cache_data)
    dvm = bt.get("dvm_capital", {})
    sid = cache_data.get("symphony_id", "")
    capital = dvm.get(sid, next(iter(dvm.values()), {})) if dvm else {}
    if not capital:
        return None, None, 0
    days = sorted(capital.keys(), key=int)
    start = (datetime(1970, 1, 1) + timedelta(days=int(days[0]))).strftime("%Y-%m-%d")
    end = (datetime(1970, 1, 1) + timedelta(days=int(days[-1]))).strftime("%Y-%m-%d")
    return start, end, len(days)


# ── Setup Page HTML ──────────────────────────────────────────────────

SETUP_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Local Maestro - Setup</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #0f1923; color: #e0e6ed; min-height: 100vh;
}
.header {
    background: linear-gradient(135deg, #1a2332, #0f1923);
    border-bottom: 2px solid #2a3a4a; padding: 20px 32px;
}
.header h1 {
    font-size: 28px;
    background: linear-gradient(135deg, #f0c040, #e8a020);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}
.header .sub { color: #6a7a8a; font-size: 13px; margin-top: 4px; }
.container { max-width: 1200px; margin: 0 auto; padding: 24px 32px; }
h2 { color: #c0d0e0; font-size: 20px; margin: 20px 0 12px; }
h3 { color: #a0b0c0; font-size: 16px; margin: 16px 0 8px; }

/* Source tabs */
.source-tabs { display: flex; gap: 0; margin: 16px 0; }
.source-tab {
    padding: 10px 24px; cursor: pointer; color: #6a7a8a;
    font-weight: 600; font-size: 14px; border: 1px solid #2a3a4a;
    background: #1a2332; transition: all 0.2s; user-select: none;
}
.source-tab:first-child { border-radius: 6px 0 0 6px; }
.source-tab:last-child { border-radius: 0 6px 6px 0; }
.source-tab.active { color: #f0a030; background: #1e2d3d; border-color: #f0a030; }
.source-tab:hover:not(.active) { color: #a0b0c0; }

/* Strategy list */
.strategy-list {
    background: #1a2332; border: 1px solid #2a3a4a; border-radius: 8px;
    max-height: 500px; overflow-y: auto; margin: 12px 0;
}
.strat-row {
    display: grid; grid-template-columns: 40px 1fr 120px 120px 80px;
    align-items: center; padding: 10px 16px; border-bottom: 1px solid #1e2d3d;
    font-size: 13px; transition: background 0.1s;
}
.strat-row:hover { background: #1e2d3d; }
.strat-row.disabled { opacity: 0.4; }
.strat-row.limiting { background: #2a1a1a; }
.strat-row input[type="checkbox"] { width: 18px; height: 18px; cursor: pointer; }
.strat-name { color: #c0d0e0; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.strat-dates { color: #6a7a8a; font-size: 12px; }
.strat-days { color: #8899aa; text-align: right; }
.strat-tag {
    font-size: 10px; padding: 2px 6px; border-radius: 3px; text-align: center;
    background: #1e2d3d; color: #6a7a8a;
}
.strat-tag.limiting-tag { background: #3a1a1a; color: #e06040; }

.strat-header {
    display: grid; grid-template-columns: 40px 1fr 120px 120px 80px;
    padding: 8px 16px; background: #162030; font-size: 11px;
    color: #6a7a8a; font-weight: 600; text-transform: uppercase;
    border-bottom: 2px solid #2a3a4a; position: sticky; top: 0; z-index: 1;
}

/* Info bar */
.info-bar {
    background: #1e2d3d; border: 1px solid #2a3a4a; border-radius: 8px;
    padding: 16px 20px; margin: 16px 0; display: flex; gap: 32px; flex-wrap: wrap;
}
.info-item { }
.info-label { color: #6a7a8a; font-size: 11px; text-transform: uppercase; margin-bottom: 4px; }
.info-value { color: #e0e6ed; font-size: 16px; font-weight: 600; }
.info-value.warning { color: #e06040; }
.info-value.good { color: #40c060; }

/* Time period */
.period-row { display: flex; gap: 8px; margin: 16px 0; flex-wrap: wrap; }
.period-btn {
    padding: 8px 20px; cursor: pointer; border: 1px solid #2a3a4a;
    background: #1a2332; color: #8899aa; font-size: 13px; font-weight: 600;
    border-radius: 6px; transition: all 0.2s; user-select: none;
}
.period-btn:hover { color: #c0d0e0; border-color: #4a5a6a; }
.period-btn.active { color: #f0a030; border-color: #f0a030; background: #1e2d3d; }
.custom-dates {
    display: none; gap: 12px; align-items: center; margin: 8px 0;
}
.custom-dates.visible { display: flex; }
.custom-dates input {
    background: #1a2332; border: 1px solid #2a3a4a; color: #c0d0e0;
    padding: 8px 12px; border-radius: 4px; font-size: 13px;
}

/* Buttons */
.actions { margin: 24px 0; display: flex; gap: 12px; }
.btn {
    padding: 12px 32px; border: none; border-radius: 6px;
    font-size: 15px; font-weight: 700; cursor: pointer; transition: all 0.2s;
}
.btn-primary { background: linear-gradient(135deg, #f0a030, #e08020); color: #1a1a1a; }
.btn-primary:hover { transform: translateY(-1px); box-shadow: 0 4px 12px rgba(240,160,48,0.3); }
.btn-primary:disabled { opacity: 0.5; cursor: not-allowed; transform: none; box-shadow: none; }
.btn-secondary { background: #1e2d3d; color: #8899aa; border: 1px solid #2a3a4a; }
.btn-secondary:hover { color: #c0d0e0; }

/* Loading overlay */
.loading {
    display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0;
    background: rgba(15,25,35,0.9); z-index: 100;
    justify-content: center; align-items: center; flex-direction: column; gap: 16px;
}
.loading.active { display: flex; }
.loading .spinner {
    width: 48px; height: 48px; border: 4px solid #2a3a4a;
    border-top-color: #f0a030; border-radius: 50%; animation: spin 0.8s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }
.loading p { color: #8899aa; font-size: 14px; }

.select-actions { display: flex; gap: 8px; margin: 8px 0; }
.select-actions button {
    background: none; border: 1px solid #2a3a4a; color: #6a7a8a;
    padding: 4px 12px; border-radius: 4px; font-size: 12px; cursor: pointer;
}
.select-actions button:hover { color: #c0d0e0; border-color: #4a5a6a; }

.footer { text-align: center; padding: 24px; color: #4a5a6a; font-size: 12px; margin-top: 40px; }
</style>
</head>
<body>

<div class="header">
    <h1>Local Maestro</h1>
    <div class="sub">Portfolio Correlation & Risk Analysis</div>
</div>

<div class="container">
    <h2>Select Strategies</h2>

    <div class="source-tabs">
        <div class="source-tab active" onclick="switchSource('portfolio')">Portfolio</div>
        <div class="source-tab" onclick="switchSource('watchlist')">Watchlist</div>
        <div class="source-tab" onclick="switchSource('drafts')">Drafts</div>
        <div class="source-tab" onclick="switchSource('local')">Local Files</div>
    </div>

    <div class="info-bar" id="info-bar">
        <div class="info-item">
            <div class="info-label">Selected</div>
            <div class="info-value" id="info-selected">0 strategies</div>
        </div>
        <div class="info-item">
            <div class="info-label">Effective Date Range</div>
            <div class="info-value" id="info-range">-</div>
        </div>
        <div class="info-item">
            <div class="info-label">Trading Days</div>
            <div class="info-value" id="info-days">-</div>
        </div>
        <div class="info-item">
            <div class="info-label">Limited By</div>
            <div class="info-value warning" id="info-limiter">-</div>
        </div>
    </div>

    <h3>Analysis Period</h3>
    <div class="period-row">
        <div class="period-btn active" onclick="setPeriod('all')">All Time</div>
        <div class="period-btn" onclick="setPeriod('3y')">Prior 3 Years</div>
        <div class="period-btn" onclick="setPeriod('1y')">Prior 1 Year</div>
        <div class="period-btn" onclick="setPeriod('ytd')">Year-to-Date</div>
        <div class="period-btn" onclick="setPeriod('6m')">Prior 6 Months</div>
        <div class="period-btn" onclick="setPeriod('3m')">Prior 3 Months</div>
        <div class="period-btn" onclick="setPeriod('custom')">Custom</div>
    </div>
    <div class="custom-dates" id="custom-dates">
        <label style="color:#8899aa">From:</label>
        <input type="date" id="custom-start" onchange="updateInfo()">
        <label style="color:#8899aa">To:</label>
        <input type="date" id="custom-end" onchange="updateInfo()">
    </div>

    <div class="select-actions">
        <button onclick="selectAll()">Select All (this tab)</button>
        <button onclick="selectNone()">Deselect All (this tab)</button>
        <button onclick="selectLongOnly()">Only 1Y+ History</button>
        <button onclick="deselectEverything()">Clear All Sources</button>
        <button onclick="clearCache()" title="Delete cached backtests so they're re-fetched on next analyze">Clear Cache</button>
    </div>

    <input type="text" id="strat-search" placeholder="Search strategies..."
        oninput="renderList()"
        style="width:100%;padding:10px 14px;background:#1a2332;border:1px solid #2a3a4a;
        border-radius:6px;color:#c0d0e0;font-size:14px;margin:8px 0;outline:none;"
        onfocus="this.style.borderColor='#f0a030'" onblur="this.style.borderColor='#2a3a4a'">

    <div class="strategy-list" id="strategy-list">
        <div class="strat-header">
            <div></div><div>Strategy</div><div>Start</div><div>End</div><div>Days</div>
        </div>
        <div style="padding:40px;text-align:center;color:#6a7a8a;">
            Loading strategies...
        </div>
    </div>

    <div style="margin:20px 0;">
        <h3>Add to Local Files</h3>
        <div id="drop-zone" style="border:2px dashed #2a3a4a;border-radius:8px;padding:32px;text-align:center;
            color:#6a7a8a;cursor:pointer;transition:all 0.2s;margin:12px 0;"
            onclick="document.getElementById('file-input').click()"
            ondragover="event.preventDefault();this.style.borderColor='#f0a030';this.style.color='#c0d0e0';"
            ondragleave="this.style.borderColor='#2a3a4a';this.style.color='#6a7a8a';"
            ondrop="handleDrop(event)">
            Drop strategy JSON/TXT files here, or click to browse — saved to local_files/
            <input type="file" id="file-input" multiple accept=".json,.txt" style="display:none"
                onchange="handleFiles(this.files)">
        </div>
    </div>

    <div class="actions" style="align-items:center;">
        <button class="btn btn-primary" id="analyze-btn" onclick="runAnalysis()" disabled>
            Analyze Selected Strategies
        </button>
        <label style="display:flex;align-items:center;gap:8px;cursor:pointer;color:#8899aa;font-size:13px;margin-left:16px;">
            <input type="checkbox" id="synthetic-mode" style="width:18px;height:18px;cursor:pointer;">
            <span>Extended History</span>
        </label>
        <select id="synthetic-engine" style="background:#1a2332;border:1px solid #2a3a4a;color:#8899aa;padding:4px 8px;border-radius:4px;font-size:12px;margin-left:4px;display:none;">
            <option value="hybrid">Hybrid (Rainboy pre + Composer post)</option>
            <option value="pure">Pure Rainbow (best for correlations)</option>
        </select>
        <label style="display:flex;align-items:center;gap:8px;cursor:pointer;color:#8899aa;font-size:13px;margin-left:16px;">
            <input type="checkbox" id="save-local" style="width:18px;height:18px;cursor:pointer;">
            <span>Save to Local Files</span>
        </label>
        <script>
            document.getElementById('synthetic-mode').addEventListener('change', function() {
                document.getElementById('synthetic-engine').style.display = this.checked ? 'inline-block' : 'none';
            });
        </script>
    </div>
</div>

<div class="loading" id="loading">
    <div class="spinner"></div>
    <p id="loading-text">Fetching backtest data...</p>
</div>

<div class="footer">
    Local Maestro &mdash; Offline Portfolio Correlation & Risk Analysis
</div>

<script>
// allStrategies: master map by id, persists across source switches
// Each entry: {id, name, source, start, end, days, checked, cached}
let allStrategies = {};  // id -> strategy object
let currentSource = 'portfolio';
let currentPeriod = 'all';
let loadedSources = {};  // track which sources have been fetched

async function api(path) {
    const resp = await fetch('/api' + path);
    return await resp.json();
}

function getAllChecked() {
    return Object.values(allStrategies).filter(s => s.checked && s.start);
}

function getAllCheckedIds() {
    return Object.values(allStrategies).filter(s => s.checked).map(s => s.id);
}

async function switchSource(source) {
    currentSource = source;
    document.querySelectorAll('.source-tab').forEach(t => t.classList.remove('active'));
    document.querySelector(`[onclick="switchSource('${source}')"]`).classList.add('active');

    // If not loaded yet, fetch from API
    if (!loadedSources[source]) {
        const list = document.getElementById('strategy-list');
        list.innerHTML = '<div class="strat-header"><div></div><div>Strategy</div><div>Source</div><div>Start</div><div>End</div><div>Days</div></div>' +
            '<div style="padding:40px;text-align:center;color:#6a7a8a;">Loading ' + source + '...</div>';
        try {
            const data = await api('/strategies/' + source);
            const newStrats = data.strategies || [];
            newStrats.forEach(s => {
                // Only add if not already in master map (avoid duplicates from diff sources)
                if (!allStrategies[s.id]) {
                    s.checked = (source === 'portfolio');  // Auto-check portfolio, uncheck others
                    allStrategies[s.id] = s;
                }
            });
            loadedSources[source] = true;
        } catch (e) {
            const list = document.getElementById('strategy-list');
            list.innerHTML = '<div style="padding:20px;color:#e06040;">Error loading: ' + e.message + '</div>';
            return;
        }
    }

    renderList();
    updateInfo();
}

function getSourceStrategies(source) {
    return Object.values(allStrategies).filter(s => s.source === source);
}

function renderList() {
    const list = document.getElementById('strategy-list');
    const limiter = findLimiter();
    const searchTerm = (document.getElementById('strat-search').value || '').toLowerCase().trim();

    // Show current source tab strategies, filtered by search
    let sourceStrats = getSourceStrategies(currentSource);
    if (searchTerm) {
        sourceStrats = sourceStrats.filter(s => s.name.toLowerCase().includes(searchTerm));
    }

    let html = '<div class="strat-header"><div></div><div>Strategy</div><div>Start</div><div>End</div><div>Days</div></div>';

    // Sort: checked first, then by days descending
    const sorted = [...sourceStrats].sort((a, b) => {
        if (a.checked !== b.checked) return a.checked ? -1 : 1;
        return (b.days || 0) - (a.days || 0);
    });

    if (sorted.length === 0) {
        html += '<div style="padding:20px;text-align:center;color:#6a7a8a;">No strategies found.</div>';
    }

    sorted.forEach((s, i) => {
        const isLimiting = limiter && s.id === limiter.id && s.checked;
        const cls = isLimiting ? 'strat-row limiting' : (s.checked ? 'strat-row' : 'strat-row disabled');
        const tag = isLimiting ? '<span class="strat-tag limiting-tag">LIMITING</span>' :
                    (!s.cached ? '<span class="strat-tag">FETCH ON ANALYZE</span>' : '');

        html += `<div class="${cls}">
            <div><input type="checkbox" ${s.checked ? 'checked' : ''}
                 onchange="toggleStrategy('${s.id}', this.checked)"></div>
            <div class="strat-name" title="${s.name}">${s.name} ${tag}</div>
            <div class="strat-dates">${s.start || '-'}</div>
            <div class="strat-dates">${s.end || '-'}</div>
            <div class="strat-days">${s.days || '-'}</div>
        </div>`;
    });

    list.innerHTML = html;
}

function findLimiter() {
    const checked = getAllChecked();
    if (checked.length === 0) return null;

    let limiter = null;
    let latestStart = '1900-01-01';
    checked.forEach(s => {
        if (s.start > latestStart) {
            latestStart = s.start;
            limiter = s;
        }
    });
    return limiter;
}

function getEffectiveStart() {
    switch (currentPeriod) {
        case '3y': { const d = new Date(); d.setFullYear(d.getFullYear() - 3); return d.toISOString().split('T')[0]; }
        case '1y': { const d = new Date(); d.setFullYear(d.getFullYear() - 1); return d.toISOString().split('T')[0]; }
        case 'ytd': return new Date().getFullYear() + '-01-01';
        case '6m': { const d = new Date(); d.setMonth(d.getMonth() - 6); return d.toISOString().split('T')[0]; }
        case '3m': { const d = new Date(); d.setMonth(d.getMonth() - 3); return d.toISOString().split('T')[0]; }
        case 'custom': return document.getElementById('custom-start').value || null;
        default: return null;
    }
}

function getEffectiveEnd() {
    if (currentPeriod === 'custom') return document.getElementById('custom-end').value || null;
    return null;
}

function updateInfo() {
    const checked = getAllChecked();
    const allCheckedCount = getAllCheckedIds().length;

    // Count selected per source
    const sourceCounts = {};
    Object.values(allStrategies).forEach(s => {
        if (s.checked) {
            sourceCounts[s.source] = (sourceCounts[s.source] || 0) + 1;
        }
    });
    const sourceLabel = Object.entries(sourceCounts).map(([k,v]) => `${v} ${k}`).join(', ');

    document.getElementById('info-selected').textContent = allCheckedCount > 0
        ? `${allCheckedCount} strategies (${sourceLabel})`
        : '0 strategies';
    document.getElementById('analyze-btn').disabled = allCheckedCount < 2;

    if (checked.length === 0) {
        document.getElementById('info-range').textContent = '-';
        document.getElementById('info-days').textContent = '-';
        document.getElementById('info-limiter').textContent = '-';
        return;
    }

    // Calculate effective date range
    let rangeStart = checked.reduce((latest, s) => s.start > latest ? s.start : latest, '1900-01-01');
    const rangeEnd = checked.reduce((earliest, s) => s.end < earliest ? s.end : earliest, '2099-12-31');

    const periodStart = getEffectiveStart();
    const periodEnd = getEffectiveEnd();
    if (periodStart && periodStart > rangeStart) rangeStart = periodStart;
    const effectiveEnd = periodEnd || rangeEnd;

    document.getElementById('info-range').textContent = `${rangeStart} to ${effectiveEnd}`;

    // Approximate trading days
    const startDate = new Date(rangeStart);
    const endDate = new Date(effectiveEnd);
    const calDays = (endDate - startDate) / (1000 * 60 * 60 * 24);
    const tradingDays = Math.round(calDays * 252 / 365);
    const daysEl = document.getElementById('info-days');
    daysEl.textContent = tradingDays > 0 ? `~${tradingDays}` : '0';
    daysEl.className = 'info-value' + (tradingDays < 60 ? ' warning' : tradingDays > 200 ? ' good' : '');

    const limiter = findLimiter();
    const limiterEl = document.getElementById('info-limiter');
    if (limiter && !periodStart) {
        const maxLen = 40;
        const name = limiter.name.length > maxLen ? limiter.name.substring(0, maxLen) + '...' : limiter.name;
        limiterEl.textContent = `${name} (${limiter.start})`;
        limiterEl.className = 'info-value warning';
    } else if (periodStart) {
        limiterEl.textContent = `Custom period: ${periodStart}`;
        limiterEl.className = 'info-value';
    } else {
        limiterEl.textContent = 'None';
        limiterEl.className = 'info-value good';
    }

    renderList();
}

function toggleStrategy(id, checked) {
    const s = allStrategies[id];
    if (s) s.checked = checked;
    updateInfo();
}

function selectAll() {
    getSourceStrategies(currentSource).forEach(s => s.checked = true);
    renderList(); updateInfo();
}
function selectNone() {
    getSourceStrategies(currentSource).forEach(s => s.checked = false);
    renderList(); updateInfo();
}
function selectLongOnly() {
    getSourceStrategies(currentSource).forEach(s => { s.checked = s.days >= 252; });
    renderList(); updateInfo();
}
function deselectEverything() {
    Object.values(allStrategies).forEach(s => s.checked = false);
    renderList(); updateInfo();
}

function setPeriod(p) {
    currentPeriod = p;
    document.querySelectorAll('.period-btn').forEach(b => b.classList.remove('active'));
    document.querySelector(`[onclick="setPeriod('${p}')"]`).classList.add('active');
    document.getElementById('custom-dates').className = 'custom-dates' + (p === 'custom' ? ' visible' : '');
    updateInfo();
}

async function runAnalysis() {
    const selected = getAllCheckedIds();
    if (selected.length < 2) return;

    const loading = document.getElementById('loading');
    const loadingText = document.getElementById('loading-text');
    loading.classList.add('active');
    loadingText.textContent = `Fetching backtests for ${selected.length} strategies...`;

    try {
        const synthetic = document.getElementById('synthetic-mode').checked;
        const syntheticEngine = document.getElementById('synthetic-engine').value;
        const saveLocal = document.getElementById('save-local').checked;
        const resp = await fetch('/api/analyze', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                ids: selected,
                start: getEffectiveStart(),
                end: getEffectiveEnd(),
                synthetic: synthetic,
                synthetic_engine: syntheticEngine,
                save_local: saveLocal,
            }),
        });

        if (!resp.ok) {
            const err = await resp.text();
            throw new Error(err);
        }

        const result = await resp.json();
        loadingText.textContent = 'Opening report...';

        // Open the generated report
        window.open(result.report_url, '_blank');
    } catch (e) {
        alert('Analysis failed: ' + e.message);
    } finally {
        loading.classList.remove('active');
    }
}

async function clearCache() {
    if (!confirm('Delete all cached backtests? They will be re-fetched on next Analyze.')) return;
    try {
        const resp = await fetch('/api/clear-cache', { method: 'POST' });
        const result = await resp.json();
        alert(result.message || 'Cache cleared');
        // Reset loaded sources so dates refresh
        loadedSources = {};
        switchSource(currentSource);
    } catch (e) {
        alert('Error: ' + e.message);
    }
}

// File upload handlers
function handleDrop(e) {
    e.preventDefault();
    e.target.style.borderColor = '#2a3a4a';
    e.target.style.color = '#6a7a8a';
    if (e.dataTransfer.files.length) handleFiles(e.dataTransfer.files);
}

async function handleFiles(files) {
    const dropZone = document.getElementById('drop-zone');
    const origText = dropZone.innerText;
    dropZone.innerText = `Uploading ${files.length} file(s)...`;
    dropZone.style.color = '#f0a030';

    for (const file of files) {
        try {
            const text = await file.text();
            const resp = await fetch('/api/upload', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ filename: file.name, content: text }),
            });
            const result = await resp.json();
            if (result.error) {
                alert(`Error uploading ${file.name}: ${result.error}`);
                continue;
            }
            // Add to allStrategies as 'upload' source
            const s = result.strategy;
            s.source = 'upload';
            s.checked = true;
            allStrategies[s.id] = s;
        } catch (e) {
            alert(`Failed to upload ${file.name}: ${e.message}`);
        }
    }

    dropZone.innerText = origText;
    dropZone.style.color = '#6a7a8a';

    // Add upload tab if not exists
    const tabs = document.querySelector('.source-tabs');
    if (!tabs.querySelector('[onclick*="upload"]')) {
        const tab = document.createElement('div');
        tab.className = 'source-tab';
        tab.setAttribute('onclick', "switchSource('upload')");
        tab.textContent = 'Local Files';
        tabs.appendChild(tab);
    }
    loadedSources['upload'] = true;
    switchSource('upload');
}

// Init
switchSource('portfolio');
</script>
</body>
</html>"""


# ── HTTP Server ──────────────────────────────────────────────────────

class MaestroHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"  [{datetime.now().strftime('%H:%M:%S')}] {fmt % args}")

    def send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_error_json(self, message, status=500):
        self.send_json({"error": message}, status)

    def _find_upload(self, strat_id):
        """Find a raw strategy file in local_files/ matching this ID."""
        import hashlib
        upload_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "local_files")
        if not os.path.isdir(upload_dir):
            return None
        for f in os.listdir(upload_dir):
            if not f.endswith((".json", ".txt")):
                continue
            # Match by file-hash ID scheme
            file_hash = hashlib.md5(f.encode()).hexdigest()[:12]
            if strat_id == f"local_{file_hash}":
                return os.path.join(upload_dir, f)
        return None

    def _backtest_upload(self, filepath, strat_id):
        """Run Rainboy backtest on a raw strategy file, cache the result."""
        try:
            from data_loader import run_rainboy_backtest
            with open(filepath) as fh:
                strat_data = json.load(fh)
            strat_name = strat_data.get("name", Path(filepath).stem)

            print(f"  [analyze] Backtesting upload '{strat_name}'...")
            report_data = run_rainboy_backtest(filepath)

            # Convert to cache format
            dvm = {}
            for date_str, val in zip(report_data["dates"], report_data["equity"]):
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                epoch_day = (dt - datetime(1970, 1, 1)).days
                dvm[str(epoch_day)] = val

            cache = {
                "symphony_id": strat_id,
                "cached_at": datetime.now().isoformat(),
                "backtest": {
                    "dvm_capital": {strat_id: dvm},
                    "legend": {strat_id: {"name": strat_name}},
                }
            }
            cache_path = os.path.join(BACKTEST_DIR, f"{strat_id}.json")
            with open(cache_path, "w") as f:
                json.dump(cache, f)
            print(f"  [analyze] OK '{strat_name}'")
            return True
        except Exception as e:
            print(f"  [analyze] Backtest failed for {filepath}: {e}")
            return False

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/" or path == "":
            # Serve setup page
            body = SETUP_PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif path in ("/api/strategies/portfolio", "/api/strategies/watchlist", "/api/strategies/drafts"):
            source = path.split("/")[-1]
            try:
                if source == "portfolio":
                    symphonies = list_portfolio_symphonies()
                elif source == "watchlist":
                    symphonies = list_watchlist_symphonies()
                else:
                    symphonies = list_draft_symphonies()

                # Only fetch backtests for strategies we already have cached
                # (avoid 100+ API calls for watchlist/drafts)
                for s in symphonies:
                    cache_path = os.path.join(BACKTEST_DIR, f"{s['id']}.json")
                    if os.path.exists(cache_path):
                        try:
                            with open(cache_path) as fh:
                                cache = json.load(fh)
                            start, end, days = get_strategy_date_range(cache)
                            s["start"] = start
                            s["end"] = end
                            s["days"] = days
                            s["cached"] = True
                        except Exception:
                            s["start"] = s["end"] = None
                            s["days"] = 0
                            s["cached"] = False
                    else:
                        s["start"] = None
                        s["end"] = None
                        s["days"] = 0
                        s["cached"] = False

                self.send_json({"strategies": symphonies})
            except Exception as e:
                self.send_error_json(str(e))

        elif path == "/api/strategies/local":
            # Only show files in local_files/ — user-managed, no temp cache
            strategies = []
            local_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "local_files")
            os.makedirs(local_dir, exist_ok=True)
            already_ids = set()

            for f in sorted(os.listdir(local_dir)):
                if not f.endswith((".json", ".txt")):
                    continue
                filepath = os.path.join(local_dir, f)
                try:
                    with open(filepath) as fh:
                        strat_data = json.load(fh)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue

                if strat_data.get("step") != "root":
                    continue  # Not a strategy file

                # Use file-based ID to avoid collisions when multiple files
                # share the same JSON id (e.g. subgroups extracted from one parent)
                import hashlib
                file_hash = hashlib.md5(f.encode()).hexdigest()[:12]
                strat_id = f"local_{file_hash}"
                strat_name = strat_data.get("name", os.path.splitext(f)[0])

                # Check if already backtested and cached
                cache_path = os.path.join(BACKTEST_DIR, f"{strat_id}.json")
                if strat_id in already_ids:
                    continue  # Already listed from cache

                if os.path.exists(cache_path):
                    try:
                        with open(cache_path) as fh:
                            cache = json.load(fh)
                        start, end, days = get_strategy_date_range(cache)
                        strategies.append({
                            "id": strat_id, "name": strat_name, "source": "local",
                            "start": start, "end": end, "days": days,
                            "cached": True,
                        })
                    except Exception:
                        pass
                else:
                    # Not yet backtested — list it, will backtest on analyze
                    strategies.append({
                        "id": strat_id, "name": strat_name, "source": "local",
                        "start": None, "end": None, "days": 0,
                        "cached": False, "_upload_path": filepath,
                    })

            self.send_json({"strategies": strategies})

        elif path.startswith("/reports/"):
            # Serve generated report files
            filename = os.path.basename(path)
            filepath = os.path.join(REPORTS_DIR, filename)
            if os.path.exists(filepath):
                with open(filepath, "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_error(404)

        else:
            self.send_error(404)

    def do_POST(self):
        parsed = urlparse(self.path)

        if parsed.path == "/api/analyze":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len).decode())

            ids = body.get("ids", [])
            start_date = body.get("start")
            end_date = body.get("end")
            synthetic = body.get("synthetic", False)
            synthetic_engine = body.get("synthetic_engine", "hybrid")
            save_local = body.get("save_local", False)

            if len(ids) < 2:
                self.send_error_json("Need at least 2 strategies", 400)
                return

            try:
                # Phase 1: Load Composer backtests + Rainboy data for each strategy
                strategy_pairs = []  # [{composer, rainboy, sid}, ...]
                failed_ids = []
                for sid in ids:
                    # Load Composer backtest
                    cache_path = os.path.join(BACKTEST_DIR, f"{sid}.json")
                    if not os.path.exists(cache_path):
                        uploaded = self._find_upload(sid)
                        if uploaded:
                            if not self._backtest_upload(uploaded, sid):
                                failed_ids.append(sid)
                                continue
                        else:
                            cache = fetch_backtest(sid)
                            if not cache:
                                failed_ids.append(sid)
                                continue
                    try:
                        composer_data = load_composer_backtest_json(cache_path)
                    except Exception as e:
                        print(f"  [analyze] Error parsing {sid}: {e}")
                        failed_ids.append(sid)
                        continue

                    # Load Rainboy data if synthetic mode
                    rainboy_data = None
                    if synthetic:
                        synth_cache = os.path.join(BACKTEST_DIR, f"{sid}_synthetic.json")

                        # Check if cached synthetic is still valid:
                        # - Must exist
                        # - Source file in uploads/ must not be newer than cache
                        # - Cache must be from this session (< 30 min)
                        if os.path.exists(synth_cache):
                            cache_mtime = os.path.getmtime(synth_cache)
                            source_file = self._find_upload(sid)
                            stale = (datetime.now().timestamp() - cache_mtime) > 1800
                            source_updated = source_file and os.path.getmtime(source_file) > cache_mtime
                            if not stale and not source_updated:
                                try:
                                    rainboy_data = load_composer_backtest_json(synth_cache)
                                except Exception:
                                    pass
                            elif source_updated:
                                print(f"  [analyze] Synthetic cache stale for {sid} — source file updated")

                        if not rainboy_data:
                            uploaded = self._find_upload(sid)
                            if not uploaded:
                                print(f"  [analyze] Synthetic: Fetching strategy code for {sid} from Composer API...")
                                code = fetch_symphony_code(sid)
                                if code:
                                    # Save to temp dir, not uploads/ (uploads is user-managed)
                                    import tempfile
                                    fetched_path = os.path.join(tempfile.gettempdir(), f"maestro_{sid}.json")
                                    with open(fetched_path, "w") as f:
                                        json.dump(code, f, indent=2)
                                    print(f"  [analyze] Fetched strategy code: {code.get('name', sid)}")
                                    uploaded = fetched_path

                            if uploaded:
                                if self._backtest_upload(uploaded, f"{sid}_synthetic"):
                                    try:
                                        rainboy_data = load_composer_backtest_json(synth_cache)
                                    except Exception:
                                        pass

                        if not rainboy_data:
                            print(f"  [analyze] Synthetic: No Rainboy data for {sid}, Composer only")

                    strategy_pairs.append({
                        "composer": composer_data,
                        "rainboy": rainboy_data,
                        "sid": sid,
                    })

                # Phase 2: Stitch strategies
                all_strategies = []
                if synthetic and any(p["rainboy"] for p in strategy_pairs):
                    if synthetic_engine == "pure":
                        # Pure Rainboy: use Rainboy where available, Composer as fallback
                        for pair in strategy_pairs:
                            all_strategies.append(pair["rainboy"] or pair["composer"])
                    else:
                        # Hybrid with shared junction: prevents correlation artifacts
                        # by ensuring ALL strategies switch engines at the same date
                        all_strategies = stitch_hybrid_portfolio(strategy_pairs)
                else:
                    # No synthetic, just Composer data
                    all_strategies = [p["composer"] for p in strategy_pairs]

                # Save to uploads/ if requested
                if save_local and all_strategies:
                    upload_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "local_files")
                    os.makedirs(upload_dir, exist_ok=True)
                    saved = 0
                    for pair in strategy_pairs:
                        sid = pair["sid"]
                        # Save Composer backtest cache
                        cache_path = os.path.join(BACKTEST_DIR, f"{sid}.json")
                        if os.path.exists(cache_path):
                            strat_name = pair["composer"].get("name", sid)
                            # Clean name for filename
                            safe_name = re.sub(r'[^\w\s\-\.]', '', strat_name)[:60].strip()
                            dest = os.path.join(upload_dir, f"{safe_name} ({sid}).json")
                            if not os.path.exists(dest):
                                import shutil
                                shutil.copy2(cache_path, dest)
                                saved += 1
                                print(f"  [save] Saved to uploads: {safe_name}")
                        # Also save strategy code if we fetched it
                        fetched = os.path.join(os.environ.get("TMPDIR", "/tmp"), f"maestro_{sid}.json")
                        if os.path.exists(fetched):
                            strat_name = pair["composer"].get("name", sid)
                            safe_name = re.sub(r'[^\w\s\-\.]', '', strat_name)[:60].strip()
                            code_dest = os.path.join(upload_dir, f"{safe_name} ({sid}) code.json")
                            if not os.path.exists(code_dest):
                                import shutil
                                shutil.copy2(fetched, code_dest)
                                print(f"  [save] Saved strategy code to uploads: {safe_name}")
                    if saved:
                        print(f"  [save] Saved {saved} backtest(s) to uploads/")

                if failed_ids:
                    print(f"  [analyze] Failed to load: {failed_ids}")

                if len(all_strategies) < 2:
                    msg = f"Not enough valid strategies (loaded {len(all_strategies)}/{len(ids)})"
                    if failed_ids:
                        msg += f". Failed: {', '.join(failed_ids[:5])}"
                    self.send_error_json(msg, 400)
                    return

                # Align and analyze
                dates, equity_df = align_strategies(all_strategies, start_date, end_date)
                analyzer = PortfolioAnalyzer(equity_df)
                analysis = analyzer.full_analysis()

                # Inject strategy IDs so optimizer tab can reference them
                # Map strategy names to IDs
                name_to_id = {s["name"]: s["id"] for s in all_strategies}
                analysis["strategy_ids"] = [name_to_id.get(n, "") for n in analysis["strategy_names"]]

                # Generate report
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"maestro_{timestamp}.html"
                output_path = os.path.join(REPORTS_DIR, filename)
                generate_html(analysis, output_path)

                self.send_json({"report_url": f"/reports/{filename}"})

            except Exception as e:
                self.send_error_json(str(e))

        elif parsed.path == "/api/optimize":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len).decode())

            ids = body.get("ids", [])
            start_date = body.get("start")
            end_date = body.get("end")
            n_random = min(body.get("n_random", 50000), 200000)
            n_boundary = min(body.get("n_boundary", 5000), 50000)

            if len(ids) < 2:
                self.send_error_json("Need at least 2 strategies", 400)
                return

            try:
                # Load backtest data
                all_strategies = []
                for sid in ids:
                    cache_path = os.path.join(BACKTEST_DIR, f"{sid}.json")
                    if not os.path.exists(cache_path):
                        cache = fetch_backtest(sid)
                        if not cache:
                            continue
                    data = load_composer_backtest_json(cache_path)
                    all_strategies.append(data)

                if len(all_strategies) < 2:
                    self.send_error_json("Not enough valid strategies", 400)
                    return

                # Align strategies
                dates, equity_df = align_strategies(all_strategies, start_date, end_date)
                returns_df = equity_df.pct_change().iloc[1:]

                # Try to extract SPY benchmark returns from one of the cached backtests
                import numpy as np
                from data_loader import epoch_day_to_date
                benchmark_returns = None
                for sid in ids:
                    cache_path = os.path.join(BACKTEST_DIR, f"{sid}.json")
                    if os.path.exists(cache_path):
                        with open(cache_path) as fh:
                            cache_data = json.load(fh)
                        bt = cache_data.get("backtest", cache_data)
                        dvm = bt.get("dvm_capital", {})
                        if "SPY" in dvm:
                            spy_data = dvm["SPY"]
                            spy_sorted = sorted(spy_data.items(), key=lambda x: int(x[0]))
                            spy_dates = [epoch_day_to_date(int(d)) for d, _ in spy_sorted]
                            spy_equity = [float(v) for _, v in spy_sorted]
                            import pandas as pd
                            spy_series = pd.Series(spy_equity, index=pd.to_datetime(spy_dates))
                            # Align to same dates as returns_df
                            spy_aligned = spy_series.reindex(returns_df.index, method="ffill")
                            if spy_aligned.notna().sum() > len(returns_df) * 0.8:
                                benchmark_returns = spy_aligned.pct_change().iloc[1:].values
                                # Trim returns_df to match
                                returns_df = returns_df.iloc[1:]
                            break

                # Run Monte Carlo optimization
                print(f"  Optimizing: {n_random + n_boundary} portfolios, {len(returns_df)} days, {len(returns_df.columns)} strategies...")
                result = optimize_portfolios(returns_df, n_random=n_random,
                                              n_boundary=n_boundary,
                                              benchmark_returns=benchmark_returns)
                print(f"  Optimization complete.")

                self.send_json(result)

            except Exception as e:
                import traceback
                traceback.print_exc()
                self.send_error_json(str(e))

        elif parsed.path == "/api/clear-cache":
            try:
                count = 0
                for f in os.listdir(BACKTEST_DIR):
                    if f.endswith(".json"):
                        os.remove(os.path.join(BACKTEST_DIR, f))
                        count += 1
                print(f"  [cache] Cleared {count} cached backtests")
                self.send_json({"message": f"Cleared {count} cached backtests"})
            except Exception as e:
                self.send_error_json(str(e))

        elif parsed.path == "/api/upload":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len).decode())

            filename = body.get("filename", "unknown.json")
            content = body.get("content", "")

            try:
                strat_data = json.loads(content)
            except json.JSONDecodeError:
                self.send_error_json("File is not valid JSON", 400)
                return

            if strat_data.get("step") != "root":
                # Could be a Composer backtest cache — check for dvm_capital
                if "dvm_capital" in strat_data or "dvm_capital" in strat_data.get("backtest", {}):
                    sid = strat_data.get("symphony_id", os.path.splitext(filename)[0])
                    cache_path = os.path.join(BACKTEST_DIR, f"{sid}.json")
                    with open(cache_path, "w") as f:
                        json.dump(strat_data, f)
                    start, end, days = get_strategy_date_range(strat_data)
                    bt = strat_data.get("backtest", strat_data)
                    legend = bt.get("legend", {})
                    name = legend.get(sid, {}).get("name", filename)
                    self.send_json({"strategy": {
                        "id": sid, "name": name, "start": start, "end": end,
                        "days": days, "cached": True,
                    }})
                    return
                self.send_error_json("Not a Composer strategy (missing step:root) or backtest cache", 400)
                return

            # It's a strategy JSON — save it and run Rainboy backtest
            strat_name = strat_data.get("name", os.path.splitext(filename)[0])
            strat_id = strat_data.get("id", f"upload_{hash(content) & 0xFFFFFFFF:08x}")

            print(f"  [upload] Backtesting '{strat_name}'...")

            # Save strategy JSON to temp file
            UPLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "local_files")
            os.makedirs(UPLOAD_DIR, exist_ok=True)
            strat_path = os.path.join(UPLOAD_DIR, f"{strat_id}.json")
            with open(strat_path, "w") as f:
                json.dump(strat_data, f, indent=2)

            # Run Rainboy backtest
            try:
                from data_loader import run_rainboy_backtest
                report_data = run_rainboy_backtest(strat_path)
                # Convert to Composer-style cache format so analyze can load it
                import pandas as pd
                dates = report_data["dates"]
                equity = report_data["equity"]
                # Build dvm_capital: {strategy_id: {epoch_day: capital}}
                dvm = {}
                for date_str, val in zip(dates, equity):
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                    epoch_day = (dt - datetime(1970, 1, 1)).days
                    dvm[str(epoch_day)] = val

                cache = {
                    "symphony_id": strat_id,
                    "cached_at": datetime.now().isoformat(),
                    "backtest": {
                        "dvm_capital": {strat_id: dvm},
                        "legend": {strat_id: {"name": strat_name}},
                    }
                }
                cache_path = os.path.join(BACKTEST_DIR, f"{strat_id}.json")
                with open(cache_path, "w") as f:
                    json.dump(cache, f)

                start, end, days = get_strategy_date_range(cache)
                print(f"  [upload] OK '{strat_name}': {days} days ({start} to {end})")

                self.send_json({"strategy": {
                    "id": strat_id, "name": strat_name, "start": start, "end": end,
                    "days": days, "cached": True,
                }})

            except Exception as e:
                import traceback
                traceback.print_exc()
                self.send_error_json(f"Backtest failed for {strat_name}: {e}")

        else:
            self.send_error(404)


def main():
    parser = argparse.ArgumentParser(description="Local Maestro Interactive Server")
    parser.add_argument("--port", "-p", type=int, default=8080, help="Port (default: 8080)")
    parser.add_argument("--no-open", action="store_true", help="Don't auto-open browser")
    args = parser.parse_args()

    if not API_KEY:
        print("Warning: No Composer API credentials found in scripts/.env")
        print("Portfolio/Watchlist fetching will not work. Local cache still available.")

    HTTPServer.allow_reuse_address = True
    server = HTTPServer(("127.0.0.1", args.port), MaestroHandler)
    url = f"http://127.0.0.1:{args.port}"
    print(f"Local Maestro running at {url}", flush=True)
    print("Press Ctrl+C to stop.\n", flush=True)

    # Open browser
    if not args.no_open:
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()


if __name__ == "__main__":
    main()
