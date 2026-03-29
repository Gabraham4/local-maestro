"""
Data loader for Local Maestro.
Supports loading backtest data from:
1. Composer backtest cache JSON files (dvm_capital format)
2. Rainboy backtester HTML reports (auto-run on strategy JSONs)
3. Simple CSV files (date, equity columns)
"""

import json
import os
import re
import subprocess
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def epoch_day_to_date(epoch_day: int) -> str:
    """Convert epoch day (days since 1970-01-01) to ISO date string."""
    return (datetime(1970, 1, 1) + timedelta(days=epoch_day)).strftime("%Y-%m-%d")


def load_composer_backtest_json(filepath: str) -> Dict:
    """
    Load a Composer backtest cache JSON file.
    Returns dict with keys: name, id, dates, equity, weights (optional)
    """
    with open(filepath, "r") as f:
        data = json.load(f)

    # Handle wrapped cache format: {symphony_id, cached_at, backtest: {...}}
    if "backtest" in data and "dvm_capital" not in data:
        strategy_id_hint = data.get("symphony_id", None)
        data = data["backtest"]
        if strategy_id_hint and not data.get("legend"):
            # Use the outer symphony_id if legend is missing
            data.setdefault("_symphony_id_hint", strategy_id_hint)

    # Extract strategy name and ID from legend
    legend = data.get("legend", {})
    strategy_id = list(legend.keys())[0] if legend else Path(filepath).stem
    strategy_name = legend.get(strategy_id, {}).get("name", strategy_id) if legend else strategy_id

    # Extract daily equity from dvm_capital
    dvm = data.get("dvm_capital", {})
    if not dvm:
        raise ValueError(f"No dvm_capital found in {filepath}")

    # dvm_capital has symphony_id as key, with epoch_day: value pairs
    capital_data = dvm.get(strategy_id, {})
    if not capital_data:
        # Try first key
        first_key = list(dvm.keys())[0]
        capital_data = dvm[first_key]

    # Sort by epoch day and convert
    sorted_days = sorted(capital_data.items(), key=lambda x: int(x[0]))
    dates = [epoch_day_to_date(int(d)) for d, _ in sorted_days]
    equity = [float(v) for _, v in sorted_days]

    result = {
        "name": strategy_name,
        "id": strategy_id,
        "dates": dates,
        "equity": equity,
    }

    # Extract allocation weights if available (tdvm_weights)
    tdvm = data.get("tdvm_weights", {})
    if tdvm:
        weights = {}
        for ticker, day_weights in tdvm.items():
            if day_weights:  # skip empty
                ticker_clean = ticker.split("::")[-1].split("//")[0] if "::" in ticker else ticker
                weights[ticker_clean] = {
                    epoch_day_to_date(int(d)): float(w)
                    for d, w in day_weights.items()
                }
        result["weights"] = weights

    return result


def load_rainboy_html_report(filepath: str) -> Dict:
    """
    Parse a Rainboy backtester HTML report to extract daily equity data.
    The HTML embeds JavaScript with strategyData containing dates, returns, equity arrays.
    """
    with open(filepath, "r") as f:
        html = f.read()

    # Extract strategyData from embedded JavaScript
    pattern = r"const\s+strategyData\s*=\s*(\{[^;]+?\});"
    match = re.search(pattern, html, re.DOTALL)
    if not match:
        raise ValueError(f"Could not find strategyData in {filepath}")

    # Parse the JS object (it's close to JSON)
    js_obj = match.group(1)
    # Clean up JS to valid JSON
    js_obj = re.sub(r"(\w+):", r'"\1":', js_obj)  # quote keys
    js_obj = js_obj.replace("'", '"')  # single to double quotes

    try:
        data = json.loads(js_obj)
    except json.JSONDecodeError:
        # Fallback: extract arrays individually
        data = {}
        for key in ["dates", "returns", "equity"]:
            arr_match = re.search(rf"{key}\s*:\s*\[([^\]]+)\]", html)
            if arr_match:
                arr_str = arr_match.group(1)
                if key == "dates":
                    data[key] = [s.strip().strip("'\"") for s in arr_str.split(",")]
                else:
                    data[key] = [float(x.strip()) for x in arr_str.split(",")]

    if "dates" not in data or "equity" not in data:
        raise ValueError(f"Could not extract dates/equity from {filepath}")

    # Align lengths — Rainboy reports may have equity[0] as starting value before first date
    dates = data["dates"]
    equity = data["equity"]
    if len(equity) == len(dates) + 1:
        equity = equity[1:]  # trim leading starting value

    # Extract strategy name from report title
    title_match = re.search(r"<title>([^<]+)</title>", html)
    name = title_match.group(1).strip() if title_match else Path(filepath).stem

    return {
        "name": name,
        "id": Path(filepath).stem,
        "dates": dates,
        "equity": equity,
    }


def run_rainboy_backtest(strategy_json_path: str, backtest_sh_path: str = None,
                          start_date: str = None) -> Dict:
    """
    Run the Rainboy backtester on a strategy JSON file and parse the output.
    Returns the same format as load_rainboy_html_report.
    """
    if backtest_sh_path is None:
        backtest_sh_path = os.environ.get(
            "RAINBOY_BACKTEST_PATH",
            os.path.expanduser("~/Rainboy CLI Backtester/backtest.sh"),
        )

    if not os.path.exists(backtest_sh_path):
        raise FileNotFoundError(f"Rainboy backtester not found at {backtest_sh_path}")

    # Ensure absolute path for backtest.sh
    strategy_json_path = os.path.abspath(strategy_json_path)

    # Extract strategy name from JSON
    with open(strategy_json_path, "r") as f:
        strat_data = json.load(f)
    strategy_name = strat_data.get("name", Path(strategy_json_path).stem)

    # Build command
    cmd = [backtest_sh_path, strategy_json_path]
    if start_date:
        cmd.extend(["--start", start_date])

    # Run backtester
    backtest_dir = os.path.dirname(backtest_sh_path)
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=backtest_dir,
        timeout=120,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Backtest failed for {strategy_json_path}: {result.stderr}")

    # Find the generated HTML report
    reports_dir = os.path.join(backtest_dir, "Reports")
    if not os.path.exists(reports_dir):
        raise RuntimeError("No Reports directory found after backtest")

    # Find most recent HTML file
    html_files = sorted(
        Path(reports_dir).glob("*.html"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not html_files:
        raise RuntimeError("No HTML report generated")

    report_data = load_rainboy_html_report(str(html_files[0]))
    report_data["name"] = strategy_name

    return report_data


def stitch_hybrid_backtest(composer_data: Dict, rainboy_data: Dict) -> Dict:
    """
    Per-strategy stitch (legacy fallback). Use stitch_hybrid_portfolio() for
    multi-strategy analysis to avoid correlation artifacts.
    """
    if not composer_data.get("dates"):
        return rainboy_data
    return _stitch_single(composer_data, rainboy_data, composer_data["dates"][0])


def _stitch_single(composer_data: Dict, rainboy_data: Dict, junction_date: str) -> Dict:
    """
    Stitch a single strategy at a specific junction date.
    Before junction: Rainboy data. From junction onwards: Composer data.
    Rebased at junction so equity curves connect seamlessly.
    """
    c_dates = composer_data["dates"]
    c_equity = composer_data["equity"]
    r_dates = rainboy_data["dates"]
    r_equity = rainboy_data["equity"]

    if not c_dates or not r_dates:
        return composer_data if c_dates else rainboy_data

    # Rainboy data before the junction
    pre_dates = []
    pre_equity = []
    for d, e in zip(r_dates, r_equity):
        if d < junction_date:
            pre_dates.append(d)
            pre_equity.append(e)

    if not pre_dates:
        return composer_data

    # Composer data from junction onwards
    post_dates = []
    post_equity = []
    for d, e in zip(c_dates, c_equity):
        if d >= junction_date:
            post_dates.append(d)
            post_equity.append(e)

    if not post_dates:
        return rainboy_data

    # Rebase Rainboy pre-period to match Composer's value at junction
    rainboy_last = pre_equity[-1]
    composer_at_junction = post_equity[0]
    scale = composer_at_junction / rainboy_last if rainboy_last > 0 else 1.0
    pre_equity_rebased = [e * scale for e in pre_equity]

    merged_dates = pre_dates + post_dates
    merged_equity = pre_equity_rebased + post_equity

    print(f"  [hybrid] Stitched at {junction_date}: "
          f"{len(pre_dates)} Rainboy days + {len(post_dates)} Composer days "
          f"= {len(merged_dates)} total")

    return {
        "name": composer_data.get("name", rainboy_data.get("name", "Unknown")),
        "id": composer_data.get("id", rainboy_data.get("id", "")),
        "dates": merged_dates,
        "equity": merged_equity,
    }


def stitch_hybrid_portfolio(strategy_pairs: List[Dict]) -> List[Dict]:
    """
    Multi-strategy hybrid stitch with a SHARED junction date.

    The junction is the latest Composer start date across all strategies.
    Before the junction: ALL strategies use Rainboy (same price source).
    After the junction: ALL strategies use Composer (same price source).

    This prevents correlation artifacts from mixing Yahoo (Rainboy) and
    Xignite (Composer) price data within the same time window.

    Args:
        strategy_pairs: List of dicts with keys:
            - "composer": Composer backtest data (dates, equity, name, id)
            - "rainboy": Rainboy backtest data (dates, equity, name, id) or None

    Returns:
        List of stitched strategy dicts in the same order.
    """
    # Find the latest Composer start date — this becomes the shared junction
    composer_starts = []
    for pair in strategy_pairs:
        c = pair.get("composer")
        if c and c.get("dates"):
            composer_starts.append(c["dates"][0])

    if not composer_starts:
        return [p.get("rainboy") or p["composer"] for p in strategy_pairs]

    # Shared junction = latest Composer start (so ALL strategies have Composer data after it)
    junction = max(composer_starts)
    print(f"  [hybrid] Shared junction date: {junction} "
          f"(latest of {len(composer_starts)} Composer start dates)")

    results = []
    for pair in strategy_pairs:
        composer = pair["composer"]
        rainboy = pair.get("rainboy")
        name = composer.get("name", "?")[:40]

        if rainboy and rainboy.get("dates"):
            stitched = _stitch_single(composer, rainboy, junction)
            results.append(stitched)
        else:
            print(f"  [hybrid] {name}: No Rainboy data, using Composer only")
            results.append(composer)

    return results


def load_csv(filepath: str) -> Dict:
    """
    Load a simple CSV with columns: date, equity (or value).
    Can also handle multi-column CSVs where each column after date is a strategy.
    """
    df = pd.read_csv(filepath, parse_dates=[0])
    df.columns = [c.strip() for c in df.columns]

    date_col = df.columns[0]
    dates = df[date_col].dt.strftime("%Y-%m-%d").tolist()

    if len(df.columns) == 2:
        # Single strategy
        name = df.columns[1]
        return {
            "name": name,
            "id": name,
            "dates": dates,
            "equity": df[name].tolist(),
        }
    else:
        # Multiple strategies - return list
        strategies = []
        for col in df.columns[1:]:
            strategies.append({
                "name": col,
                "id": col,
                "dates": dates,
                "equity": df[col].tolist(),
            })
        return strategies


def load_from_path(filepath: str, **kwargs) -> list:
    """
    Auto-detect file type and load backtest data.
    Returns a list of strategy dicts.
    """
    path = Path(filepath)

    if path.suffix == ".csv":
        result = load_csv(filepath)
        return result if isinstance(result, list) else [result]

    elif path.suffix == ".json":
        with open(filepath, "r") as f:
            data = json.load(f)

        # Detect format
        if "dvm_capital" in data:
            # Direct Composer backtest response
            return [load_composer_backtest_json(filepath)]
        elif "backtest" in data and "dvm_capital" in data.get("backtest", {}):
            # Wrapped cache format: {symphony_id, cached_at, backtest: {...}}
            return [load_composer_backtest_json(filepath)]
        elif "step" in data and data.get("step") == "root":
            # Strategy JSON - needs backtesting
            return [run_rainboy_backtest(filepath, **kwargs)]
        else:
            raise ValueError(f"Unrecognized JSON format in {filepath}")

    elif path.suffix == ".html":
        return [load_rainboy_html_report(filepath)]

    else:
        # Try parsing as JSON regardless of extension (.txt, etc.)
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
            if "dvm_capital" in data:
                return [load_composer_backtest_json(filepath)]
            elif "backtest" in data and "dvm_capital" in data.get("backtest", {}):
                return [load_composer_backtest_json(filepath)]
            elif "step" in data and data.get("step") == "root":
                return [run_rainboy_backtest(filepath, **kwargs)]
            else:
                raise ValueError(f"Unrecognized JSON format in {filepath}")
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ValueError(f"Unsupported file type: {path.suffix}")


def align_strategies(strategies: List[Dict], start_date: str = None,
                      end_date: str = None,
                      min_overlap_pct: float = 0.9) -> Tuple[pd.DatetimeIndex, pd.DataFrame]:
    """
    Align multiple strategies to a common date range.

    Instead of strict inner join (which shrinks to the shortest strategy),
    this finds the best overlap window and drops strategies that don't cover it.

    Returns (dates, DataFrame of equity values with strategy names as columns).
    """
    dfs = []
    for strat in strategies:
        s = pd.Series(
            strat["equity"],
            index=pd.to_datetime(strat["dates"]),
            name=strat["name"],
        )
        # Apply date filters to each strategy individually first.
        # When start_date is specified, include the previous trading day so that
        # the return ON the start date is captured (matches MyMaestro.co behaviour).
        if start_date:
            ts = pd.Timestamp(start_date)
            before = s[s.index < ts]
            if len(before) > 0:
                s = s[s.index >= before.index[-1]]
            else:
                s = s[s.index >= ts]
        if end_date:
            s = s[s.index <= pd.Timestamp(end_date)]
        if len(s) > 1:
            dfs.append(s)

    if len(dfs) < 2:
        raise ValueError("Not enough strategies with data in the requested date range")

    # Use inner join — only keep dates where ALL selected strategies have data.
    # This trims the window to the shortest strategy rather than silently dropping it.
    df = pd.concat(dfs, axis=1, join="inner").sort_index()
    df = df.dropna()  # Drop any remaining NaN rows
    print(f"  Aligned: {len(df)} trading days from {df.index[0].strftime('%Y-%m-%d')} to {df.index[-1].strftime('%Y-%m-%d')}")

    if len(df) < 2:
        raise ValueError("Not enough overlapping data points after alignment")

    # Handle duplicate strategy names by appending suffix
    if df.columns.duplicated().any():
        cols = list(df.columns)
        seen = {}
        for i, c in enumerate(cols):
            if c in seen:
                seen[c] += 1
                cols[i] = f"{c} ({seen[c]})"
            else:
                seen[c] = 0
        df.columns = cols

    # Rebase equity to start at the same value (percentage-based)
    for col in df.columns:
        df[col] = df[col] / df[col].iloc[0] * 10000  # Rebase to 10000

    return df.index, df
