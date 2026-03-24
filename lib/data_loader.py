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

    # Extract strategy name from report title
    title_match = re.search(r"<title>([^<]+)</title>", html)
    name = title_match.group(1).strip() if title_match else Path(filepath).stem

    return {
        "name": name,
        "id": Path(filepath).stem,
        "dates": data["dates"],
        "equity": data["equity"],
    }


def run_rainboy_backtest(strategy_json_path: str, backtest_sh_path: str = None,
                          start_date: str = None) -> Dict:
    """
    Run the Rainboy backtester on a strategy JSON file and parse the output.
    Returns the same format as load_rainboy_html_report.
    """
    if backtest_sh_path is None:
        backtest_sh_path = os.path.expanduser(
            "~/AIProjects/ComposerTrading/MyTools/Rainboy CLI Backtester/backtest.sh"
        )

    if not os.path.exists(backtest_sh_path):
        raise FileNotFoundError(f"Rainboy backtester not found at {backtest_sh_path}")

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

    # Find the date range that most strategies cover
    # Strategy: use outer join, then pick the window where most strategies have data
    df_outer = pd.concat(dfs, axis=1, join="outer").sort_index()

    # Count non-null strategies per date
    coverage = df_outer.notna().sum(axis=1)

    # Find the longest contiguous window where >= 90% of strategies have data
    # Start from the date where coverage first reaches max, going forward
    max_coverage = coverage.max()
    threshold = max(2, int(max_coverage * min_overlap_pct))

    # Dates where enough strategies have data
    good_dates = coverage[coverage >= threshold].index

    if len(good_dates) < 2:
        # Fallback: strict inner join
        df = pd.concat(dfs, axis=1, join="inner").sort_index()
    else:
        df = df_outer.loc[good_dates]
        # Drop strategies that have too many missing values in this range
        missing_pct = df.isna().mean()
        keep_cols = missing_pct[missing_pct < 0.1].index  # Keep strategies with <10% missing
        df = df[keep_cols].dropna()  # Drop any remaining NaN rows

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
