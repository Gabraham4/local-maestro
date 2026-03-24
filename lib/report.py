"""
HTML report generator for Local Maestro.
Creates a self-contained interactive HTML dashboard with Plotly.js charts.
"""

import json
import math
from typing import Dict


def _sanitize_json(obj):
    """Replace NaN/Inf with null for JSON serialization."""
    if isinstance(obj, dict):
        return {k: _sanitize_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_sanitize_json(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj


def generate_html(analysis: Dict, output_path: str) -> str:
    """Generate a self-contained HTML report from analysis data."""

    data_json = json.dumps(_sanitize_json(analysis), indent=None)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Local Maestro - Portfolio Analysis</title>
<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #0f1923;
    color: #e0e6ed;
    min-height: 100vh;
}}
.header {{
    background: linear-gradient(135deg, #1a2332 0%, #0f1923 100%);
    border-bottom: 2px solid #2a3a4a;
    padding: 16px 32px;
    display: flex;
    align-items: center;
    gap: 16px;
}}
.header h1 {{
    font-size: 28px;
    background: linear-gradient(135deg, #f0c040, #e8a020);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-weight: 700;
}}
.header .subtitle {{
    color: #8899aa;
    font-size: 13px;
}}
.header .back-btn {{
    margin-left: auto;
    padding: 8px 20px;
    background: #1e2d3d;
    color: #8899aa;
    border: 1px solid #2a3a4a;
    border-radius: 6px;
    font-size: 13px;
    font-weight: 600;
    cursor: pointer;
    text-decoration: none;
    transition: all 0.2s;
}}
.header .back-btn:hover {{
    color: #f0a030;
    border-color: #f0a030;
}}
.toolbar {{
    background: #1a2332;
    padding: 12px 32px;
    display: flex;
    gap: 16px;
    align-items: center;
    border-bottom: 1px solid #2a3a4a;
}}
.toolbar .info {{
    color: #8899aa;
    font-size: 13px;
}}
.toolbar .period {{
    color: #c0d0e0;
    font-size: 14px;
    font-weight: 600;
}}
.export-btn {{
    margin-left: auto;
    padding: 6px 16px;
    background: #1e2d3d;
    color: #8899aa;
    border: 1px solid #2a3a4a;
    border-radius: 4px;
    font-size: 12px;
    font-weight: 600;
    cursor: pointer;
}}
.export-btn:hover {{ color: #f0a030; border-color: #f0a030; }}
.container {{ max-width: 1400px; margin: 0 auto; padding: 24px 32px; }}
h2 {{ color: #c0d0e0; font-size: 22px; margin-bottom: 8px; }}
h3 {{ color: #a0b0c0; font-size: 17px; margin: 24px 0 8px; }}

/* Tabs */
.tabs {{
    display: flex;
    gap: 0;
    margin: 20px 0 24px;
    border-bottom: 2px solid #2a3a4a;
}}
.tab {{
    padding: 10px 24px;
    cursor: pointer;
    color: #6a7a8a;
    font-weight: 600;
    font-size: 14px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    border-bottom: 3px solid transparent;
    transition: all 0.2s;
    user-select: none;
}}
.tab:hover {{ color: #a0b0c0; }}
.tab.active {{
    color: #f0a030;
    border-bottom-color: #f0a030;
}}
.tab-content {{ display: none; }}
.tab-content.active {{ display: block; }}

/* Tables */
.metrics-table {{
    width: 100%;
    border-collapse: collapse;
    margin: 16px 0 24px;
    font-size: 13px;
}}
.metrics-table th {{
    background: #1e2d3d;
    color: #8899aa;
    padding: 10px 12px;
    text-align: right;
    font-weight: 600;
    border-bottom: 2px solid #2a3a4a;
    white-space: nowrap;
}}
.metrics-table th:first-child {{ text-align: left; }}
.metrics-table td {{
    padding: 10px 12px;
    text-align: right;
    border-bottom: 1px solid #1e2d3d;
    white-space: nowrap;
}}
.metrics-table td:first-child {{
    text-align: left;
    color: #c0d0e0;
    font-weight: 600;
}}
.metrics-table tr.portfolio-row {{
    background: #1a2a3a;
}}
.metrics-table tr.portfolio-row td {{
    color: #f0c040;
    font-weight: 600;
    border-bottom: 2px solid #2a3a4a;
}}
.metrics-table tr.mean-row {{
    background: #162030;
}}
.metrics-table tr.mean-row td {{
    color: #8899aa;
    font-style: italic;
}}
.highlight-high {{ color: #40c060 !important; }}
.highlight-high::before {{ content: "\\25CF "; font-size: 8px; }}
.highlight-low {{ color: #e06040 !important; }}
.highlight-low::before {{ content: "\\25CB "; font-size: 8px; }}

/* Correlation heatmap */
.corr-container {{ margin: 20px 0; }}
.corr-table {{
    border-collapse: collapse;
    margin: 0 auto;
    font-size: 13px;
}}
.corr-table th {{
    padding: 12px 16px;
    color: #8899aa;
    font-weight: 600;
    max-width: 180px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}}
.corr-table td {{
    padding: 14px 20px;
    text-align: center;
    font-weight: 600;
    font-size: 14px;
    min-width: 80px;
}}
.corr-table .mean-col {{
    background: #1a2a3a !important;
    color: #c0d0e0;
    font-weight: 700;
}}

/* Chart containers */
.chart-box {{
    background: #1a2332;
    border: 1px solid #2a3a4a;
    border-radius: 8px;
    padding: 16px;
    margin: 20px 0;
}}
.chart-title {{
    color: #c0d0e0;
    font-size: 18px;
    font-weight: 700;
    margin-bottom: 4px;
}}
.chart-subtitle {{
    color: #6a7a8a;
    font-size: 12px;
    margin-bottom: 12px;
}}

/* Options */
.option-row {{
    display: flex;
    gap: 12px;
    align-items: center;
    margin: 8px 0;
}}
.option-row label {{
    color: #8899aa;
    font-size: 13px;
    cursor: pointer;
}}
.option-row input[type="checkbox"] {{
    cursor: pointer;
}}
select {{
    background: #1a2332;
    color: #c0d0e0;
    border: 1px solid #2a3a4a;
    padding: 6px 12px;
    border-radius: 4px;
    font-size: 13px;
}}

/* Optimizer tab */
.opt-layout {{
    display: grid;
    grid-template-columns: 1fr 280px;
    gap: 20px;
}}
.opt-main {{ min-width: 0; }}
.opt-controls {{
    display: flex;
    gap: 16px;
    align-items: flex-end;
    flex-wrap: wrap;
    margin-bottom: 12px;
}}
.opt-setting label {{
    display: block;
    color: #6a7a8a;
    font-size: 11px;
    text-transform: uppercase;
    margin-bottom: 4px;
}}
.opt-info {{
    background: #1e2d3d;
    border: 1px solid #2a3a4a;
    border-radius: 6px;
    padding: 8px 16px;
    font-size: 13px;
    color: #8899aa;
}}
.opt-filter-row {{
    display: flex;
    gap: 8px;
    align-items: center;
    margin: 8px 0;
    font-size: 13px;
    color: #8899aa;
}}
.opt-filter-row input, .opt-filter-row select {{
    background: #1a2332;
    border: 1px solid #2a3a4a;
    color: #c0d0e0;
    padding: 4px 8px;
    border-radius: 4px;
    font-size: 13px;
}}
.opt-btn {{
    background: #1e2d3d;
    border: 1px solid #2a3a4a;
    color: #8899aa;
    padding: 6px 14px;
    border-radius: 4px;
    font-size: 12px;
    cursor: pointer;
    font-weight: 600;
}}
.opt-btn:hover {{ color: #c0d0e0; border-color: #4a5a6a; }}
.opt-sidebar {{
    background: #1a2332;
    border: 1px solid #2a3a4a;
    border-radius: 8px;
    padding: 16px;
    max-height: 700px;
    overflow-y: auto;
    font-size: 13px;
}}
.opt-sidebar h3 {{ color: #a0b0c0; font-size: 14px; margin: 12px 0 8px; }}
.opt-weight-row {{
    display: flex;
    justify-content: space-between;
    padding: 4px 0;
    border-bottom: 1px solid #1e2d3d;
}}
.opt-weight-name {{ color: #8899aa; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 180px; }}
.opt-weight-val {{ color: #f0c040; font-weight: 600; white-space: nowrap; }}
.opt-sidebar-actions {{ margin: 12px 0; }}
.opt-avg-label {{ color: #6a7a8a; font-size: 12px; margin: 8px 0 4px; }}
.opt-avg-metrics {{
    margin-top: 12px;
    padding: 8px 0;
    border-top: 1px solid #2a3a4a;
}}
.opt-metric-row {{
    display: flex;
    justify-content: space-between;
    padding: 3px 0;
    color: #8899aa;
}}
.opt-metric-val {{ color: #c0d0e0; font-weight: 600; }}

.footer {{
    text-align: center;
    padding: 24px;
    color: #4a5a6a;
    font-size: 12px;
    border-top: 1px solid #1e2d3d;
    margin-top: 40px;
}}
</style>
</head>
<body>

<div class="header">
    <div>
        <h1>Local Maestro</h1>
        <div class="subtitle">Portfolio Correlation & Risk Analysis</div>
    </div>
    <a class="back-btn" href="/" onclick="if(window.opener){{window.close();return false;}}">Edit Selections</a>
</div>

<div class="toolbar">
    <span class="period" id="period-label"></span>
    <span class="info" id="info-label"></span>
    <button class="export-btn" onclick="exportCurrentTab()" title="Export current tab data as compact text for LLM analysis">Export Tab Data</button>
</div>

<div class="container">
    <h2>Backtest Analysis</h2>

    <div class="tabs">
        <div class="tab active" onclick="switchTab('returns')">Returns</div>
        <div class="tab" onclick="switchTab('correlations')">Correlations</div>
        <div class="tab" onclick="switchTab('volatility')">Volatility</div>
        <div class="tab" onclick="switchTab('exposure')">Exposure</div>
        <div class="tab" onclick="switchTab('metrics')">Metrics</div>
        <div class="tab" onclick="switchTab('optimizer')" style="margin-left:auto;color:#f0a030;border:1px solid #3a3a2a;border-bottom:none;border-radius:6px 6px 0 0;background:#1a2332;">Optimizer</div>
    </div>

    <!-- RETURNS TAB -->
    <div id="tab-returns" class="tab-content active">
        <div class="chart-box">
            <div class="chart-title">Cumulative Rate of Return</div>
            <div class="chart-subtitle">Double-click legend to isolate a strategy.</div>
            <div class="option-row">
                <label><input type="checkbox" id="logScale" onchange="toggleLogScale()"> Log Scale</label>
            </div>
            <div id="chart-cumulative"></div>
        </div>
        <div class="chart-box">
            <div class="chart-title">Rolling Rate of Return</div>
            <div class="chart-subtitle" id="rolling-ret-subtitle"></div>
            <div id="chart-rolling-return"></div>
        </div>
        <div class="chart-box">
            <div class="chart-title">Rolling Daily Win Rate</div>
            <div class="chart-subtitle" id="rolling-wr-subtitle"></div>
            <div id="chart-rolling-winrate"></div>
        </div>
        <div class="chart-box">
            <div class="chart-title">Underwater Plot</div>
            <div class="chart-subtitle">Returns plotted when in a state of drawdown.</div>
            <div id="chart-underwater"></div>
        </div>
    </div>

    <!-- CORRELATIONS TAB -->
    <div id="tab-correlations" class="tab-content">
        <div class="chart-box">
            <div class="chart-title">Correlation of Returns</div>
            <div id="corr-returns-table"></div>
        </div>
        <div class="chart-box">
            <div class="chart-title">Rolling Mean Correlation</div>
            <div class="chart-subtitle" id="rolling-corr-subtitle"></div>
            <div id="chart-rolling-corr"></div>
        </div>
        <div class="chart-box">
            <div class="chart-title">Rolling CARP</div>
            <div class="chart-subtitle">Correlation And Risk-adjusted Performance. (21 day rolling periods)</div>
            <div id="chart-rolling-carp"></div>
        </div>
        <div class="chart-box">
            <div class="chart-title">Correlation During Drawdowns</div>
            <div class="chart-subtitle">Evaluates returns from trading days where the Total Portfolio is in a state of drawdown.</div>
            <div id="corr-dd-table"></div>
        </div>
        <div class="chart-box">
            <div class="chart-title">Rolling Mean Drawdown Correlation</div>
            <div class="chart-subtitle" id="rolling-ddcorr-subtitle"></div>
            <div id="chart-rolling-dd-corr"></div>
        </div>
        <div class="chart-box">
            <div class="chart-title">Rolling Smart CARP</div>
            <div class="chart-subtitle">CARP factoring volatility and correlations only during drawdowns. (21 day rolling periods)</div>
            <div id="chart-rolling-smart-carp"></div>
        </div>
    </div>

    <!-- VOLATILITY TAB -->
    <div id="tab-volatility" class="tab-content">
        <div class="chart-box">
            <div class="chart-title">Rolling Volatility</div>
            <div class="chart-subtitle" id="rolling-vol-subtitle"></div>
            <div id="chart-rolling-vol"></div>
        </div>
        <div class="chart-box">
            <div class="chart-title">Rolling Sharpe Ratio</div>
            <div class="chart-subtitle" id="rolling-sharpe-subtitle"></div>
            <div id="chart-rolling-sharpe"></div>
        </div>
        <div class="chart-box">
            <div class="chart-title">Rolling Sortino Ratio</div>
            <div class="chart-subtitle" id="rolling-sortino-subtitle"></div>
            <div id="chart-rolling-sortino"></div>
        </div>
    </div>

    <!-- EXPOSURE TAB -->
    <div id="tab-exposure" class="tab-content">
        <div class="chart-box">
            <div class="chart-title">Strategy Exposure</div>
            <div class="chart-subtitle">Exposure data requires Composer backtest cache files with tdvm_weights.</div>
            <div id="exposure-content">
                <p style="color:#6a7a8a;padding:20px;">Exposure data not available for this analysis. To enable, provide Composer backtest JSON files that include tdvm_weights allocation data.</p>
            </div>
        </div>
    </div>

    <!-- METRICS TAB -->
    <div id="tab-metrics" class="tab-content">
        <h3>Portfolios</h3>
        <div id="metrics-portfolio-table"></div>
        <h3>Symphonies</h3>
        <div class="chart-subtitle" style="margin-bottom:8px">&#9679; = Highest / &#9675; = Lowest</div>
        <div id="metrics-strategy-table"></div>
    </div>

    <!-- OPTIMIZER TAB -->
    <div id="tab-optimizer" class="tab-content">
        <div class="opt-layout">
            <div class="opt-main">
                <div class="opt-controls">
                    <div class="opt-setting">
                        <label>Random Portfolios</label>
                        <select id="opt-n-random">
                            <option value="10000">10,000</option>
                            <option value="50000" selected>50,000</option>
                            <option value="100000">100,000</option>
                        </select>
                    </div>
                    <div class="opt-setting">
                        <label>Boundary Portfolios</label>
                        <select id="opt-n-boundary">
                            <option value="2000">2,000</option>
                            <option value="5000" selected>5,000</option>
                            <option value="20000">20,000</option>
                        </select>
                    </div>
                    <div class="opt-setting">
                        <label>Color By</label>
                        <select id="opt-heatmap" onchange="optUpdateHeatmap()">
                            <option value="sharpe">Sharpe Ratio</option>
                            <option value="sortino" selected>Sortino Ratio</option>
                            <option value="smart_sharpe">Smart Sharpe</option>
                            <option value="smart_sortino">Smart Sortino</option>
                            <option value="calmar">Calmar Ratio</option>
                            <option value="serenity">Serenity</option>
                            <option value="decorrelation">De-correlation</option>
                            <option value="profit_factor">Profit Factor</option>
                            <option value="win_rate">Win Rate %</option>
                            <option value="alpha">Alpha vs SPY</option>
                            <option value="max_dd">Max Drawdown</option>
                            <option value="ann_return">Annual Return</option>
                        </select>
                    </div>
                    <button class="btn btn-primary" id="opt-run-btn" onclick="optRunOptimization()" style="padding:10px 28px;font-size:14px;">
                        Run Optimization
                    </button>
                </div>
                <div class="opt-info" id="opt-info" style="display:none;">
                    <span id="opt-info-text"></span>
                </div>
                <div class="opt-filter-row" id="opt-filter-row" style="display:none;">
                    <label>Filter by</label>
                    <select id="opt-filter-metric">
                        <option value="sharpe">Sharpe</option>
                        <option value="sortino" selected>Sortino</option>
                        <option value="smart_sharpe">Smart Sharpe</option>
                        <option value="smart_sortino">Smart Sortino</option>
                        <option value="calmar">Calmar</option>
                        <option value="serenity">Serenity</option>
                        <option value="decorrelation">De-correlation</option>
                        <option value="profit_factor">Profit Factor</option>
                        <option value="alpha">Alpha vs SPY</option>
                        <option value="ann_return">Return</option>
                    </select>
                    <label>Top</label>
                    <select id="opt-filter-mode">
                        <option value="pct">%</option>
                        <option value="num">#</option>
                    </select>
                    <input type="number" id="opt-filter-value" value="5" min="1" max="100" style="width:60px;">
                    <button class="opt-btn" onclick="optApplyFilter()">Apply Filter</button>
                    <button class="opt-btn" onclick="optClearFilter()">Clear</button>
                </div>
                <div class="chart-box" style="margin-top:12px;">
                    <div id="opt-chart" style="height:550px;">
                        <div style="padding:80px 40px;text-align:center;color:#4a5a6a;">
                            <div style="font-size:48px;margin-bottom:16px;">&#9878;</div>
                            <div style="font-size:16px;color:#6a7a8a;">Efficient Frontier Explorer</div>
                            <div style="font-size:13px;margin-top:8px;">
                                Click <strong>Run Optimization</strong> to generate random portfolio weight combinations
                                and find the optimal risk/return tradeoff.
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <div class="opt-sidebar" id="opt-sidebar" style="display:none;">
                <h3 style="margin-top:0;">Selected Portfolio</h3>
                <div id="opt-weights-list"></div>
                <div class="opt-sidebar-actions">
                    <button class="opt-btn" id="opt-avg-btn" onclick="optCalcAverage()">Average Visible Points</button>
                </div>
                <div id="opt-avg-info" style="display:none;">
                    <div class="opt-avg-label" id="opt-avg-label"></div>
                    <h3>Averaged Weights</h3>
                    <div id="opt-avg-weights"></div>
                    <div class="opt-avg-metrics" id="opt-avg-metrics"></div>
                </div>
            </div>
        </div>
    </div>
</div>

<div class="footer">
    Local Maestro &mdash; Offline Portfolio Correlation & Risk Analysis<br>
    Based on <a href="https://mymaestro.co" style="color:#4a6a8a">MyMaestro.co</a> by Codernaut
</div>

<script>
// ── Data ────────────────────────────────────────────────────────────
const DATA = {data_json};

// Strategy colors (matching Maestro's palette)
const COLORS = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
    '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
    '#aec7e8', '#ffbb78', '#98df8a', '#ff9896', '#c5b0d5',
    '#c49c94', '#f7b6d2', '#c7c7c7', '#dbdb8d', '#9edae5'
];
const PORTFOLIO_COLOR = '#ff1493';  // Hot pink for portfolio line
const MEAN_COLOR = '#ff69b4';       // Pink dashed for mean

const LAYOUT_DEFAULTS = {{
    paper_bgcolor: '#1a2332',
    plot_bgcolor: '#0f1923',
    font: {{ color: '#8899aa', family: '-apple-system, sans-serif', size: 12 }},
    margin: {{ t: 10, r: 60, b: 50, l: 60 }},
    xaxis: {{
        gridcolor: '#1e2d3d',
        linecolor: '#2a3a4a',
        tickformat: '%b %Y',
    }},
    yaxis: {{
        gridcolor: '#1e2d3d',
        linecolor: '#2a3a4a',
    }},
    legend: {{
        bgcolor: 'rgba(26,35,50,0.9)',
        bordercolor: '#2a3a4a',
        font: {{ size: 11 }},
    }},
    hovermode: 'x unified',
}};

const CONFIG = {{ responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d'] }};

// ── Tab switching ───────────────────────────────────────────────────
function switchTab(name) {{
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    document.querySelector(`[onclick="switchTab('${{name}}')"]`).classList.add('active');
    document.getElementById('tab-' + name).classList.add('active');
    // Trigger resize for Plotly charts
    window.dispatchEvent(new Event('resize'));
}}

// ── Helpers ─────────────────────────────────────────────────────────
function getColor(i) {{ return COLORS[i % COLORS.length]; }}

function truncName(name, max) {{
    max = max || 30;
    return name.length > max ? name.substring(0, max) + '...' : name;
}}

function makeTrace(dates, values, name, color, opts) {{
    opts = opts || {{}};
    return Object.assign({{
        x: dates,
        y: values,
        name: truncName(name),
        type: 'scatter',
        mode: 'lines',
        line: {{ color: color, width: opts.width || 1.5 }},
        hovertemplate: '%{{y:.2f}}<extra>' + truncName(name) + '</extra>',
    }}, opts);
}}

function plotChart(divId, chartData, extraLayout) {{
    const names = DATA.strategy_names;
    const dates = chartData.dates;
    const traces = [];

    // Strategy traces
    names.forEach((name, i) => {{
        if (chartData.data && chartData.data[name]) {{
            traces.push(makeTrace(dates, chartData.data[name], name, getColor(i)));
        }} else if (chartData[name] && chartData[name].dates) {{
            traces.push(makeTrace(chartData[name].dates, chartData[name].values, name, getColor(i)));
        }}
    }});

    // Portfolio trace
    if (chartData.data && chartData.data['Portfolio']) {{
        traces.push(makeTrace(dates, chartData.data['Portfolio'], 'Portfolio', PORTFOLIO_COLOR,
            {{ line: {{ color: PORTFOLIO_COLOR, width: 2.5 }} }}));
    }} else if (chartData['Portfolio'] && chartData['Portfolio'].dates) {{
        traces.push(makeTrace(chartData['Portfolio'].dates, chartData['Portfolio'].values,
            'Portfolio', PORTFOLIO_COLOR, {{ line: {{ color: PORTFOLIO_COLOR, width: 2.5 }} }}));
    }}

    // Mean trace
    if (chartData.data && chartData.data['Mean']) {{
        traces.push(makeTrace(dates, chartData.data['Mean'], 'Mean', MEAN_COLOR,
            {{ line: {{ color: MEAN_COLOR, width: 2, dash: 'dash' }} }}));
    }} else if (chartData['Mean'] && chartData['Mean'].dates) {{
        traces.push(makeTrace(chartData['Mean'].dates, chartData['Mean'].values,
            'Mean', MEAN_COLOR, {{ line: {{ color: MEAN_COLOR, width: 2, dash: 'dash' }} }}));
    }}

    const layout = Object.assign({{}}, LAYOUT_DEFAULTS, {{ height: 400 }}, extraLayout || {{}});
    Plotly.newPlot(divId, traces, layout, CONFIG);
}}

function plotSimpleChart(divId, chartData, extraLayout) {{
    // For charts with only Portfolio line (like CARP)
    const dates = chartData.dates;
    const traces = [];

    if (chartData.data && chartData.data['Portfolio']) {{
        traces.push(makeTrace(dates, chartData.data['Portfolio'], 'Portfolio', PORTFOLIO_COLOR,
            {{ line: {{ color: PORTFOLIO_COLOR, width: 2 }} }}));
    }} else if (chartData['Portfolio'] && chartData['Portfolio'].dates) {{
        traces.push(makeTrace(chartData['Portfolio'].dates, chartData['Portfolio'].values,
            'Portfolio', PORTFOLIO_COLOR, {{ line: {{ color: PORTFOLIO_COLOR, width: 2 }} }}));
    }}

    const layout = Object.assign({{}}, LAYOUT_DEFAULTS, {{ height: 350 }}, extraLayout || {{}});
    Plotly.newPlot(divId, traces, layout, CONFIG);
}}

// ── Correlation Heatmap Table ───────────────────────────────────────
function renderCorrTable(containerId, corrData, meanCorr) {{
    const names = DATA.strategy_names;
    const n = names.length;

    let html = '<table class="corr-table"><tr><th></th>';
    names.forEach(name => {{ html += `<th>${{truncName(name, 25)}}</th>`; }});
    html += '<th class="mean-col">Mean</th></tr>';

    names.forEach((rowName, i) => {{
        html += `<tr><th>${{truncName(rowName, 25)}}</th>`;
        names.forEach((colName, j) => {{
            const val = corrData[rowName] ? corrData[rowName][colName] : 0;
            const intensity = Math.abs(val);
            let bg;
            if (i === j) {{
                bg = '#1a3050';
            }} else {{
                const r = Math.round(26 + intensity * 40);
                const g = Math.round(48 + intensity * 60);
                const b = Math.round(80 + intensity * 80);
                bg = `rgb(${{r}},${{g}},${{b}})`;
            }}
            html += `<td style="background:${{bg}};color:#e0e6ed">${{val.toFixed(3)}}</td>`;
        }});
        // Mean column
        const mean = meanCorr[rowName] || 0;
        html += `<td class="mean-col">${{mean.toFixed(3)}}</td>`;
        html += '</tr>';
    }});

    html += '</table>';
    html += '<div style="text-align:right;color:#6a7a8a;font-size:11px;margin-top:4px;">Mean excludes self.</div>';
    document.getElementById(containerId).innerHTML = html;
}}

// ── Metrics Tables ──────────────────────────────────────────────────
function renderMetricsTables() {{
    const port = DATA.metrics.portfolio;
    const strats = DATA.metrics.strategies;
    const mean = DATA.metrics.mean;
    const highlights = DATA.metrics.highlights;

    // Portfolio table
    let html = '<table class="metrics-table"><tr>';
    const portCols = ['Cum. Return %', 'Ann. Return %', 'Exp. Ann. Return %', 'Daily Win Rate %',
                      'Max Drawdown %', 'Calmar Ratio', 'Volatility %', 'Sharpe Ratio',
                      'Sortino Ratio', 'CARP Ratio', 'Smart CARP'];
    const portKeys = ['cum_return', 'ann_return', 'exp_ann_return', 'daily_win_rate',
                      'max_drawdown', 'calmar', 'volatility', 'sharpe', 'sortino', 'carp', 'smart_carp'];
    html += '<th></th>';
    portCols.forEach(c => {{ html += `<th>${{c}}</th>`; }});
    html += '</tr><tr class="portfolio-row"><td>' + port.name + '</td>';
    portKeys.forEach(k => {{
        const v = port[k];
        html += `<td>${{v != null ? v.toLocaleString(undefined, {{minimumFractionDigits:2, maximumFractionDigits:2}}) : '-'}}</td>`;
    }});
    html += '</tr></table>';
    document.getElementById('metrics-portfolio-table').innerHTML = html;

    // Strategy table
    html = '<table class="metrics-table"><tr>';
    const stratCols = ['Cum. Return %', 'Ann. Return %', 'Exp. Ann. Return %', 'Daily Win Rate %',
                       'Max Drawdown %', 'Calmar Ratio', 'Volatility %', 'Sharpe Ratio',
                       'Sortino Ratio', 'Mean Correlation', 'Mean DD Correlation'];
    const stratKeys = ['cum_return', 'ann_return', 'exp_ann_return', 'daily_win_rate',
                       'max_drawdown', 'calmar', 'volatility', 'sharpe', 'sortino',
                       'mean_correlation', 'mean_dd_correlation'];
    html += '<th></th>';
    stratCols.forEach(c => {{ html += `<th>${{c}}</th>`; }});
    html += '</tr>';

    const highlightKeys = ['cum_return', 'ann_return', 'exp_ann_return', 'daily_win_rate',
                           'max_drawdown', 'calmar', 'volatility', 'sharpe', 'sortino'];

    strats.forEach(s => {{
        html += `<tr><td>${{truncName(s.name, 35)}}</td>`;
        stratKeys.forEach(k => {{
            let cls = '';
            if (highlightKeys.includes(k) && highlights[k]) {{
                if (highlights[k].highest === s.name) cls = ' class="highlight-high"';
                else if (highlights[k].lowest === s.name) cls = ' class="highlight-low"';
            }}
            const v = s[k];
            html += `<td${{cls}}>${{v != null ? v.toLocaleString(undefined, {{minimumFractionDigits:2, maximumFractionDigits:2}}) : '-'}}</td>`;
        }});
        html += '</tr>';
    }});

    // Mean row
    html += '<tr class="mean-row"><td>Mean</td>';
    stratKeys.forEach(k => {{
        const v = mean[k];
        html += `<td>${{v != null ? v.toLocaleString(undefined, {{minimumFractionDigits:2, maximumFractionDigits:2}}) : '-'}}</td>`;
    }});
    html += '</tr></table>';
    document.getElementById('metrics-strategy-table').innerHTML = html;
}}

// ── Log scale toggle ────────────────────────────────────────────────
let isLogScale = false;
function toggleLogScale() {{
    isLogScale = !isLogScale;
    Plotly.relayout('chart-cumulative', {{
        'yaxis.type': isLogScale ? 'log' : 'linear'
    }});
}}

// ── Export ──────────────────────────────────────────────────────────
function exportCurrentTab() {{
    const activeTab = document.querySelector('.tab-content.active');
    if (!activeTab) return;
    const tabId = activeTab.id.replace('tab-', '');
    let text = '';
    const period = `${{DATA.period.start}} to ${{DATA.period.end}} (${{DATA.period.trading_days}} days)`;
    const names = DATA.strategy_names;

    if (tabId === 'returns') {{
        text = `# Local Maestro — Returns Export\nPeriod: ${{period}}\n\n`;
        // Per-strategy summary
        text += `## Strategy Summary\nName | Cum.Return% | Ann.Return% | MaxDD% | DailyWinRate%\n`;
        text += `--- | --- | --- | --- | ---\n`;
        DATA.metrics.strategies.forEach(s => {{
            text += `${{s.name}} | ${{s.cum_return}} | ${{s.ann_return}} | ${{s.max_drawdown}} | ${{s.daily_win_rate}}\n`;
        }});
        const p = DATA.metrics.portfolio;
        text += `**${{p.name}}** | **${{p.cum_return}}** | **${{p.ann_return}}** | **${{p.max_drawdown}}** | **${{p.daily_win_rate}}**\n`;
        // Drawdown data (compact: just max DD periods)
        text += `\n## Drawdown Series (sampled monthly)\n`;
        const dd = DATA.charts.drawdowns;
        if (dd['Portfolio']) {{
            const dates = dd['Portfolio'].dates;
            const vals = dd['Portfolio'].values;
            text += `Date | Portfolio_DD%\n--- | ---\n`;
            for (let i = 0; i < dates.length; i += 21) {{
                text += `${{dates[i]}} | ${{vals[i] != null ? vals[i].toFixed(2) : ''}}\n`;
            }}
        }}

    }} else if (tabId === 'correlations') {{
        text = `# Local Maestro — Correlations Export\nPeriod: ${{period}}\n\n`;
        text += `## Correlation of Returns\n`;
        const corr = DATA.correlations.returns;
        const mc = DATA.correlations.mean_corr;
        text += `Strategy | ${{names.map(n => truncName(n,20)).join(' | ')}} | Mean\n`;
        text += `--- | ${{names.map(() => '---').join(' | ')}} | ---\n`;
        names.forEach(row => {{
            const vals = names.map(col => (corr[row] && corr[row][col] != null) ? corr[row][col].toFixed(3) : '');
            text += `${{truncName(row,20)}} | ${{vals.join(' | ')}} | ${{(mc[row] || 0).toFixed(3)}}\n`;
        }});
        text += `\n## Drawdown Correlation\n`;
        const ddc = DATA.correlations.drawdowns;
        const mdc = DATA.correlations.mean_dd_corr;
        text += `Strategy | ${{names.map(n => truncName(n,20)).join(' | ')}} | Mean\n`;
        text += `--- | ${{names.map(() => '---').join(' | ')}} | ---\n`;
        names.forEach(row => {{
            const vals = names.map(col => (ddc[row] && ddc[row][col] != null) ? ddc[row][col].toFixed(3) : '');
            text += `${{truncName(row,20)}} | ${{vals.join(' | ')}} | ${{(mdc[row] || 0).toFixed(3)}}\n`;
        }});

    }} else if (tabId === 'volatility') {{
        text = `# Local Maestro — Volatility Export\nPeriod: ${{period}}\n\n`;
        text += `## Strategy Risk Metrics\nName | Volatility% | Sharpe | Sortino | MaxDD% | Calmar\n`;
        text += `--- | --- | --- | --- | --- | ---\n`;
        DATA.metrics.strategies.forEach(s => {{
            text += `${{s.name}} | ${{s.volatility}} | ${{s.sharpe}} | ${{s.sortino}} | ${{s.max_drawdown}} | ${{s.calmar}}\n`;
        }});
        const p = DATA.metrics.portfolio;
        text += `**${{p.name}}** | **${{p.volatility}}** | **${{p.sharpe}}** | **${{p.sortino}}** | **${{p.max_drawdown}}** | **${{p.calmar}}**\n`;

    }} else if (tabId === 'metrics') {{
        text = `# Local Maestro — Full Metrics Export\nPeriod: ${{period}}\n\n`;
        text += `## Portfolio\n`;
        const p = DATA.metrics.portfolio;
        Object.entries(p).forEach(([k, v]) => {{
            if (k !== 'name') text += `${{k}}: ${{v}}\n`;
        }});
        text += `\n## Per-Strategy Metrics\n`;
        text += `Name | Cum% | Ann% | ExpAnn% | WinRate% | MaxDD% | Calmar | Vol% | Sharpe | Sortino | MeanCorr | MeanDDCorr\n`;
        text += `--- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | ---\n`;
        DATA.metrics.strategies.forEach(s => {{
            text += `${{s.name}} | ${{s.cum_return}} | ${{s.ann_return}} | ${{s.exp_ann_return}} | ${{s.daily_win_rate}} | ${{s.max_drawdown}} | ${{s.calmar}} | ${{s.volatility}} | ${{s.sharpe}} | ${{s.sortino}} | ${{s.mean_correlation}} | ${{s.mean_dd_correlation}}\n`;
        }});
        const m = DATA.metrics.mean;
        text += `**Mean** | ${{m.cum_return}} | ${{m.ann_return}} | ${{m.exp_ann_return}} | ${{m.daily_win_rate}} | ${{m.max_drawdown}} | ${{m.calmar}} | ${{m.volatility}} | ${{m.sharpe}} | ${{m.sortino}} | ${{m.mean_correlation}} | ${{m.mean_dd_correlation}}\n`;
        text += `\nCarp: ${{p.carp}}  |  Smart Carp: ${{p.smart_carp}}\n`;

    }} else if (tabId === 'optimizer') {{
        if (!optData) {{
            alert('Run optimization first, then export.');
            return;
        }}
        text = `# Local Maestro — Optimizer Export\nPeriod: ${{period}}\n`;
        text += `Portfolios: ${{optData.n_portfolios}} | Days: ${{optData.n_days}} | Strategies: ${{optData.strategies.length}}\n\n`;
        // Equal weight
        const eq = optData.equal_weight_idx;
        const v = (key, i) => optData[key] ? optData[key][i] : 'N/A';
        text += `## Equal Weight Portfolio\n`;
        text += `Return: ${{v('ann_return',eq)}}% | Vol: ${{v('ann_vol',eq)}}% | Sharpe: ${{v('sharpe',eq)}} | SmartSharpe: ${{v('smart_sharpe',eq)}} | Sortino: ${{v('sortino',eq)}} | SmartSortino: ${{v('smart_sortino',eq)}} | MaxDD: ${{v('max_dd',eq)}}% | Calmar: ${{v('calmar',eq)}} | Serenity: ${{v('serenity',eq)}} | Decorr: ${{v('decorrelation',eq)}} | PF: ${{v('profit_factor',eq)}} | WinRate: ${{v('win_rate',eq)}}% | Alpha: ${{v('alpha',eq)}}%\n\n`;
        // Top portfolios by Sortino
        const sorted = Array.from({{length: optData.n_portfolios}}, (_, i) => i);
        sorted.sort((a, b) => optData.sortino[b] - optData.sortino[a]);
        text += `## Top 20 Portfolios by Sortino\nRank | Return% | Vol% | Sharpe | SmSharpe | Sortino | SmSort | MaxDD% | Calmar | Serenity | Decorr | PF | WR% | Alpha% | Weights\n`;
        text += `--- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | ---\n`;
        for (let r = 0; r < Math.min(20, sorted.length); r++) {{
            const i = sorted[r];
            const w = optData.weights[i];
            const wStr = optData.strategies.map((n, si) => w[si] > 0.01 ? `${{truncName(n,20)}}:${{(w[si]*100).toFixed(0)}}%` : '').filter(Boolean).join(', ');
            text += `${{r+1}} | ${{v('ann_return',i)}} | ${{v('ann_vol',i)}} | ${{v('sharpe',i)}} | ${{v('smart_sharpe',i)}} | ${{v('sortino',i)}} | ${{v('smart_sortino',i)}} | ${{v('max_dd',i)}} | ${{v('calmar',i)}} | ${{v('serenity',i)}} | ${{v('decorrelation',i)}} | ${{v('profit_factor',i)}} | ${{v('win_rate',i)}} | ${{v('alpha',i)}} | ${{wStr}}\n`;
        }}
        // Frontier summary stats
        text += `\n## Distribution Summary\n`;
        const arrStats = (arr, label) => {{
            if (!arr) return `${{label}}: N/A`;
            const s = [...arr].sort((a,b) => a-b);
            return `${{label}}: min=${{s[0].toFixed(2)}} p25=${{s[Math.floor(s.length*0.25)].toFixed(2)}} median=${{s[Math.floor(s.length*0.5)].toFixed(2)}} p75=${{s[Math.floor(s.length*0.75)].toFixed(2)}} max=${{s[s.length-1].toFixed(2)}}`;
        }};
        text += arrStats(optData.ann_return, 'Return%') + '\\n';
        text += arrStats(optData.ann_vol, 'Vol%') + '\\n';
        text += arrStats(optData.sharpe, 'Sharpe') + '\\n';
        text += arrStats(optData.smart_sharpe, 'SmartSharpe') + '\\n';
        text += arrStats(optData.sortino, 'Sortino') + '\\n';
        text += arrStats(optData.smart_sortino, 'SmartSortino') + '\\n';
        text += arrStats(optData.max_dd, 'MaxDD%') + '\\n';
        text += arrStats(optData.calmar, 'Calmar') + '\\n';
        text += arrStats(optData.serenity, 'Serenity') + '\\n';
        text += arrStats(optData.decorrelation, 'Decorrelation') + '\\n';
        text += arrStats(optData.profit_factor, 'ProfitFactor') + '\\n';
        text += arrStats(optData.win_rate, 'WinRate%') + '\\n';
        text += arrStats(optData.alpha, 'Alpha%') + '\\n';

    }} else {{
        text = `# Local Maestro — ${{tabId}} Export\nPeriod: ${{period}}\nNo structured export available for this tab.\n`;
    }}

    // Download as file
    const blob = new Blob([text], {{ type: 'text/markdown' }});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `maestro_${{tabId}}_export.md`;
    a.click();
    URL.revokeObjectURL(url);
}}

// ── Initialize ──────────────────────────────────────────────────────
function init() {{
    const period = DATA.period;
    document.getElementById('period-label').textContent =
        `From ${{period.start}} to ${{period.end}}`;
    document.getElementById('info-label').textContent =
        `${{DATA.strategy_names.length}} strategies | ${{period.trading_days}} trading days`;

    const windowLabel = `(21 day rolling periods)`;
    document.getElementById('rolling-ret-subtitle').textContent = windowLabel;
    document.getElementById('rolling-wr-subtitle').textContent = windowLabel;
    document.getElementById('rolling-corr-subtitle').textContent = 'Mean Correlation ' + windowLabel;
    document.getElementById('rolling-ddcorr-subtitle').textContent = 'Mean Drawdown Correlation ' + windowLabel;
    document.getElementById('rolling-vol-subtitle').textContent = windowLabel;
    document.getElementById('rolling-sharpe-subtitle').textContent = windowLabel;
    document.getElementById('rolling-sortino-subtitle').textContent = windowLabel;

    // ── Returns tab ──
    plotChart('chart-cumulative', DATA.charts.cumulative_returns,
        {{ yaxis: {{ title: 'Cumulative Return %', gridcolor: '#1e2d3d' }} }});

    plotChart('chart-rolling-return', DATA.charts.rolling_returns,
        {{ yaxis: {{ title: 'Return %', gridcolor: '#1e2d3d' }} }});

    plotChart('chart-rolling-winrate', DATA.charts.rolling_win_rate,
        {{ yaxis: {{ title: 'Daily Win Rate %', gridcolor: '#1e2d3d' }} }});

    // Underwater plot (drawdowns only, no Mean)
    const ddData = DATA.charts.drawdowns;
    const ddTraces = [];
    DATA.strategy_names.forEach((name, i) => {{
        if (ddData[name]) {{
            ddTraces.push(makeTrace(ddData[name].dates, ddData[name].values, name, getColor(i)));
        }}
    }});
    if (ddData['Portfolio']) {{
        ddTraces.push(makeTrace(ddData['Portfolio'].dates, ddData['Portfolio'].values,
            'Portfolio', PORTFOLIO_COLOR, {{ line: {{ color: PORTFOLIO_COLOR, width: 2.5 }} }}));
    }}
    Plotly.newPlot('chart-underwater', ddTraces,
        Object.assign({{}}, LAYOUT_DEFAULTS, {{
            height: 400,
            yaxis: {{ title: 'Drawdown %', gridcolor: '#1e2d3d' }},
        }}), CONFIG);

    // ── Correlations tab ──
    renderCorrTable('corr-returns-table', DATA.correlations.returns, DATA.correlations.mean_corr);
    renderCorrTable('corr-dd-table', DATA.correlations.drawdowns, DATA.correlations.mean_dd_corr);

    plotChart('chart-rolling-corr', DATA.charts.rolling_correlation,
        {{ yaxis: {{ title: 'Correlation', gridcolor: '#1e2d3d', range: [-0.2, 1] }} }});

    plotSimpleChart('chart-rolling-carp', DATA.charts.rolling_carp,
        {{ yaxis: {{ title: 'CARP', gridcolor: '#1e2d3d' }} }});

    plotChart('chart-rolling-dd-corr', DATA.charts.rolling_dd_correlation,
        {{ yaxis: {{ title: 'Drawdown Correlation', gridcolor: '#1e2d3d', range: [-0.5, 1] }} }});

    plotSimpleChart('chart-rolling-smart-carp', DATA.charts.rolling_smart_carp,
        {{ yaxis: {{ title: 'Smart CARP', gridcolor: '#1e2d3d' }} }});

    // ── Volatility tab ──
    plotChart('chart-rolling-vol', DATA.charts.rolling_volatility,
        {{ yaxis: {{ title: 'Volatility %', gridcolor: '#1e2d3d' }} }});

    plotChart('chart-rolling-sharpe', DATA.charts.rolling_sharpe,
        {{ yaxis: {{ title: 'Sharpe Ratio', gridcolor: '#1e2d3d' }} }});

    plotChart('chart-rolling-sortino', DATA.charts.rolling_sortino,
        {{ yaxis: {{ title: 'Sortino Ratio', gridcolor: '#1e2d3d' }} }});

    // ── Metrics tab ──
    renderMetricsTables();
}}

// ── Optimizer ────────────────────────────────────────────────────────
let optData = null;       // Raw optimization results
let optFiltered = null;   // Indices of currently visible points

function optRunOptimization() {{
    const btn = document.getElementById('opt-run-btn');
    btn.disabled = true;
    btn.textContent = 'Running...';

    const nRandom = parseInt(document.getElementById('opt-n-random').value);
    const nBoundary = parseInt(document.getElementById('opt-n-boundary').value);

    // Get strategy IDs from the report data (embedded as strategy_ids if available)
    const payload = {{
        ids: DATA.strategy_ids || [],
        start: DATA.period.start,
        end: DATA.period.end,
        n_random: nRandom,
        n_boundary: nBoundary,
    }};

    // POST to server
    const serverBase = window.location.origin || 'http://127.0.0.1:8080';
    fetch(serverBase + '/api/optimize', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(payload),
    }})
    .then(resp => {{
        if (!resp.ok) return resp.text().then(t => {{ throw new Error(t); }});
        return resp.json();
    }})
    .then(data => {{
        optData = data;
        optFiltered = null;
        optRenderChart();
        document.getElementById('opt-sidebar').style.display = 'block';
        document.getElementById('opt-filter-row').style.display = 'flex';
        document.getElementById('opt-info').style.display = 'block';
        document.getElementById('opt-info-text').textContent =
            `${{data.n_portfolios.toLocaleString()}} portfolios generated | ${{data.n_days}} trading days | ${{data.strategies.length}} strategies`;

        // Show equal-weight portfolio in sidebar
        optShowWeights(0);
    }})
    .catch(err => {{
        alert('Optimization failed: ' + err.message);
    }})
    .finally(() => {{
        btn.disabled = false;
        btn.textContent = 'Run Optimization';
    }});
}}

function optRenderChart() {{
    if (!optData) return;

    const metric = document.getElementById('opt-heatmap').value;
    const metricLabel = {{
        sharpe: 'Sharpe Ratio', sortino: 'Sortino Ratio', calmar: 'Calmar Ratio',
        max_dd: 'Max Drawdown %', ann_return: 'Annual Return %',
        smart_sharpe: 'Smart Sharpe', smart_sortino: 'Smart Sortino',
        serenity: 'Serenity', decorrelation: 'De-correlation',
        profit_factor: 'Profit Factor', win_rate: 'Win Rate %',
        alpha: 'Alpha vs SPY', mean_corr: 'Mean Correlation',
    }}[metric] || metric;

    const x = optData.ann_vol;
    const y = optData.ann_return;
    const colorVals = optData[metric];
    const N = x.length;

    // Determine visible indices
    const indices = optFiltered || Array.from({{ length: N }}, (_, i) => i);

    const xVis = indices.map(i => x[i]);
    const yVis = indices.map(i => y[i]);
    const cVis = indices.map(i => colorVals[i]);
    const customdata = indices.map(i => i);  // Store original index for click

    // Color scale: green=good, red=bad. Invert for metrics where lower=better.
    const invertedMetrics = ['max_dd', 'mean_corr', 'ann_vol'];
    const colorscale = invertedMetrics.includes(metric)
        ? [[0, '#1a9850'], [0.5, '#fee08b'], [1, '#d73027']]
        : [[0, '#d73027'], [0.3, '#fee08b'], [0.7, '#66bd63'], [1, '#1a9850']];

    const trace = {{
        x: xVis, y: yVis,
        mode: 'markers',
        type: 'scattergl',
        marker: {{
            color: cVis,
            colorscale: colorscale,
            size: 3,
            opacity: 0.6,
            colorbar: {{
                title: metricLabel,
                titlefont: {{ color: '#8899aa', size: 11 }},
                tickfont: {{ color: '#8899aa' }},
                len: 0.6,
            }},
        }},
        customdata: customdata,
        hovertemplate: 'Risk: %{{x:.1f}}%<br>Return: %{{y:.1f}}%<br>' + metricLabel + ': %{{marker.color:.2f}}<extra></extra>',
    }};

    // Equal weight marker
    const eqIdx = optData.equal_weight_idx;
    const eqTrace = {{
        x: [x[eqIdx]], y: [y[eqIdx]],
        mode: 'markers+text',
        type: 'scatter',
        marker: {{ color: '#ff1493', size: 12, symbol: 'diamond', line: {{ color: '#fff', width: 1 }} }},
        text: ['Equal Weight'],
        textposition: 'top center',
        textfont: {{ color: '#ff1493', size: 11 }},
        hovertemplate: 'Equal Weight<br>Risk: %{{x:.1f}}%<br>Return: %{{y:.1f}}%<extra></extra>',
        showlegend: false,
    }};

    const layout = Object.assign({{}}, LAYOUT_DEFAULTS, {{
        height: 550,
        xaxis: {{ title: 'Risk (Std Dev %)', gridcolor: '#1e2d3d', linecolor: '#2a3a4a' }},
        yaxis: {{ title: 'Annual Return %', gridcolor: '#1e2d3d', linecolor: '#2a3a4a' }},
        hovermode: 'closest',
        margin: {{ t: 20, r: 80, b: 60, l: 70 }},
    }});

    const config = {{ responsive: true, displayModeBar: true }};
    Plotly.newPlot('opt-chart', [trace, eqTrace], layout, config);

    // Click handler to select a portfolio
    document.getElementById('opt-chart').on('plotly_click', function(data) {{
        if (data.points && data.points[0] && data.points[0].customdata !== undefined) {{
            optShowWeights(data.points[0].customdata);
        }}
    }});
}}

function optUpdateHeatmap() {{
    optRenderChart();
}}

function optShowWeights(idx) {{
    if (!optData) return;
    const weights = optData.weights[idx];
    const names = optData.strategies;
    let html = '';
    const pairs = names.map((n, i) => [n, weights[i]]).sort((a, b) => b[1] - a[1]);
    pairs.forEach(([name, w]) => {{
        if (w < 0.001) return;  // Skip negligible
        html += `<div class="opt-weight-row">
            <span class="opt-weight-name" title="${{name}}">${{truncName(name, 28)}}</span>
            <span class="opt-weight-val">${{(w * 100).toFixed(1)}}%</span>
        </div>`;
    }});
    document.getElementById('opt-weights-list').innerHTML = html;
}}

function optApplyFilter() {{
    if (!optData) return;
    const metric = document.getElementById('opt-filter-metric').value;
    const mode = document.getElementById('opt-filter-mode').value;
    const value = parseInt(document.getElementById('opt-filter-value').value);
    const vals = optData[metric];
    const N = vals.length;

    // Sort indices by metric (descending, except max_dd which is ascending for "best")
    const sorted = Array.from({{ length: N }}, (_, i) => i);
    if (metric === 'max_dd') {{
        sorted.sort((a, b) => vals[b] - vals[a]);  // Less negative = better
    }} else {{
        sorted.sort((a, b) => vals[b] - vals[a]);  // Higher = better
    }}

    let count;
    if (mode === 'pct') {{
        count = Math.max(1, Math.round(N * value / 100));
    }} else {{
        count = Math.min(value, N);
    }}

    optFiltered = sorted.slice(0, count);
    document.getElementById('opt-info-text').textContent =
        `Showing ${{optFiltered.length.toLocaleString()}} / ${{N.toLocaleString()}} portfolios (Top ${{value}}${{mode === 'pct' ? '%' : ''}} by ${{metric}})`;
    optRenderChart();
}}

function optClearFilter() {{
    optFiltered = null;
    if (optData) {{
        document.getElementById('opt-info-text').textContent =
            `${{optData.n_portfolios.toLocaleString()}} portfolios generated | ${{optData.n_days}} trading days | ${{optData.strategies.length}} strategies`;
    }}
    optRenderChart();
}}

function optCalcAverage() {{
    if (!optData) return;
    const indices = optFiltered || Array.from({{ length: optData.n_portfolios }}, (_, i) => i);
    const nStrats = optData.strategies.length;
    const avgWeights = new Array(nStrats).fill(0);

    indices.forEach(idx => {{
        const w = optData.weights[idx];
        for (let s = 0; s < nStrats; s++) {{
            avgWeights[s] += w[s];
        }}
    }});

    // Normalize
    const total = avgWeights.reduce((a, b) => a + b, 0);
    for (let s = 0; s < nStrats; s++) avgWeights[s] /= total;

    // Compute average metrics for visible points
    const avg = (key) => indices.reduce((s, i) => s + (optData[key] ? optData[key][i] : 0), 0) / indices.length;
    const avgReturn = avg('ann_return');
    const avgVol = avg('ann_vol');
    const avgSharpe = avg('sharpe');
    const avgSortino = avg('sortino');
    const avgDD = avg('max_dd');
    const avgCalmar = avg('calmar');
    const avgSmartSharpe = avg('smart_sharpe');
    const avgSmartSortino = avg('smart_sortino');
    const avgSerenity = avg('serenity');
    const avgDecorr = avg('decorrelation');
    const avgPF = avg('profit_factor');
    const avgWR = avg('win_rate');
    const avgAlpha = avg('alpha');

    // Show averaged weights
    const names = optData.strategies;
    let html = '';
    const pairs = names.map((n, i) => [n, avgWeights[i]]).sort((a, b) => b[1] - a[1]);
    pairs.forEach(([name, w]) => {{
        if (w < 0.005) return;
        html += `<div class="opt-weight-row">
            <span class="opt-weight-name" title="${{name}}">${{truncName(name, 28)}}</span>
            <span class="opt-weight-val">${{(w * 100).toFixed(1)}}%</span>
        </div>`;
    }});

    document.getElementById('opt-avg-weights').innerHTML = html;
    document.getElementById('opt-avg-label').textContent =
        `Averaged ${{indices.length.toLocaleString()}} visible portfolios`;
    document.getElementById('opt-avg-metrics').innerHTML = `
        <div class="opt-metric-row"><span>Ann. Return</span><span class="opt-metric-val">${{avgReturn.toFixed(1)}}%</span></div>
        <div class="opt-metric-row"><span>Volatility</span><span class="opt-metric-val">${{avgVol.toFixed(1)}}%</span></div>
        <div class="opt-metric-row"><span>Sharpe</span><span class="opt-metric-val">${{avgSharpe.toFixed(2)}}</span></div>
        <div class="opt-metric-row"><span>Smart Sharpe</span><span class="opt-metric-val">${{avgSmartSharpe.toFixed(2)}}</span></div>
        <div class="opt-metric-row"><span>Sortino</span><span class="opt-metric-val">${{avgSortino.toFixed(2)}}</span></div>
        <div class="opt-metric-row"><span>Smart Sortino</span><span class="opt-metric-val">${{avgSmartSortino.toFixed(2)}}</span></div>
        <div class="opt-metric-row"><span>Max DD</span><span class="opt-metric-val">${{avgDD.toFixed(1)}}%</span></div>
        <div class="opt-metric-row"><span>Calmar</span><span class="opt-metric-val">${{avgCalmar.toFixed(2)}}</span></div>
        <div class="opt-metric-row"><span>Serenity</span><span class="opt-metric-val">${{avgSerenity.toFixed(2)}}</span></div>
        <div class="opt-metric-row"><span>Decorrelation</span><span class="opt-metric-val">${{avgDecorr.toFixed(3)}}</span></div>
        <div class="opt-metric-row"><span>Profit Factor</span><span class="opt-metric-val">${{avgPF.toFixed(2)}}</span></div>
        <div class="opt-metric-row"><span>Win Rate</span><span class="opt-metric-val">${{avgWR.toFixed(1)}}%</span></div>
        <div class="opt-metric-row"><span>Alpha vs SPY</span><span class="opt-metric-val">${{avgAlpha.toFixed(1)}}%</span></div>
    `;
    document.getElementById('opt-avg-info').style.display = 'block';
}}

// Run on load
document.addEventListener('DOMContentLoaded', init);
</script>
</body>
</html>"""

    with open(output_path, "w") as f:
        f.write(html)

    return output_path
