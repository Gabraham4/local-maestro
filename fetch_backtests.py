#!/usr/bin/env python3
"""
Fetch backtests from Composer API and save as cache JSON files.
Uses the same API format that Local Maestro can read directly.

Credentials are loaded from the shared scripts/.env file in the project root.

Usage:
    # Fetch backtests for specific strategy IDs
    python fetch_backtests.py ENtpwkO1bLCFmhSMjjgp ygUqoICgwIZTaOyCzmAG

    # Fetch and immediately analyze
    python fetch_backtests.py ENtpwkO1bLCFmhSMjjgp ygUqoICgwIZTaOyCzmAG --analyze

    # Auto-detect IDs from a folder of strategy JSON files
    python fetch_backtests.py --from-dir /path/to/strategy/jsons/

    # Custom output directory
    python fetch_backtests.py ids... --output-dir ./my_backtests/
"""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path


# ── Credentials ──────────────────────────────────────────────────────

def load_credentials():
    """
    Load Composer API credentials from the shared scripts/.env file.
    Walks up from this script's location to find the ComposerTrading project root.
    """
    # Walk up to find the project root (contains scripts/.env)
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
            api_key = creds.get("COMPOSER_API_KEY", "")
            api_secret = creds.get("COMPOSER_API_SECRET", "")
            if api_key and api_secret:
                return api_key, api_secret
            break
        search = search.parent

    print("Error: Could not find Composer API credentials.")
    print("Expected COMPOSER_API_KEY and COMPOSER_API_SECRET in scripts/.env")
    sys.exit(1)


# ── API ──────────────────────────────────────────────────────────────

def fetch_backtest(symphony_id: str, api_key: str, api_secret: str,
                   output_dir: str) -> str:
    """
    Fetch a backtest from Composer's public backtest API (POST, no auth needed).
    Falls back to authenticated endpoints if public fails.
    Returns the path to the saved JSON file, or None on failure.
    """
    import base64
    import urllib.request

    body = json.dumps({
        "capital": 10000,
        "slippage_percent": 0.0001,
        "spread_markup": 0.002,
        "apply_reg_fee": True,
        "apply_taf_fee": True,
        "benchmark_tickers": ["SPY"],
    }).encode()

    data = None
    last_err = ""

    # Try 1: Public endpoint (no auth needed, works for public symphonies)
    public_url = f"https://backtest-api.composer.trade/api/v1/public/symphonies/{symphony_id}/backtest"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    req = urllib.request.Request(public_url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        last_err = e.read().decode()[:200] if e.fp else str(e)
    except Exception as e:
        last_err = str(e)

    # Try 2: Authenticated endpoint (for private/owned symphonies)
    if not data:
        auth_url = f"https://backtest-api.composer.trade/api/v1/symphonies/{symphony_id}/backtest"
        auth_headers = {
            "Content-Type": "application/json",
            "x-api-key-id": api_key,
            "Authorization": f"Bearer {api_secret}",
            "x-origin": "public-api",
        }
        auth_req = urllib.request.Request(auth_url, data=body, headers=auth_headers, method="POST")
        try:
            with urllib.request.urlopen(auth_req, timeout=120) as resp:
                data = json.loads(resp.read().decode())
        except Exception as e2:
            print(f"  FAILED: {symphony_id} - {last_err[:80]} / auth: {e2}")
            return None

    if not data:
        print(f"  FAILED: {symphony_id} (empty response)")
        return None

    # Wrap in cache format
    cache = {
        "symphony_id": symphony_id,
        "cached_at": datetime.now().isoformat(),
        "backtest": data,
    }

    output_path = os.path.join(output_dir, f"{symphony_id}.json")
    with open(output_path, "w") as f:
        json.dump(cache, f)

    # Extract name and day count for display
    legend = data.get("legend", {})
    name = legend.get(symphony_id, {}).get("name", symphony_id)
    dvm = data.get("dvm_capital", {})
    capital = dvm.get(symphony_id, next(iter(dvm.values()), {})) if dvm else {}
    n_days = len(capital)

    print(f"  OK: {name} ({n_days} days)")
    return output_path


def extract_ids_from_dir(dir_path: str) -> list:
    """Extract symphony IDs from strategy JSON filenames in a directory."""
    ids = []
    for f in sorted(os.listdir(dir_path)):
        if f.endswith(".json"):
            m = re.search(r"\(([A-Za-z0-9]{16,})\)\.json$", f)
            if m:
                ids.append(m.group(1))
    return ids


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fetch Composer backtests for Local Maestro",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("ids", nargs="*", help="Composer symphony IDs to fetch")
    parser.add_argument("--from-dir", "-f",
                        help="Directory of strategy JSONs (auto-extract IDs from filenames)")
    parser.add_argument("--output-dir", "-o", default="backtest_data",
                        help="Output directory for cached backtest files (default: backtest_data)")
    parser.add_argument("--analyze", "-a", action="store_true",
                        help="Automatically run Local Maestro analysis after fetching")
    parser.add_argument("--start", "-s", help="Start date for analysis (YYYY-MM-DD)")

    args = parser.parse_args()

    # Collect IDs
    ids = list(args.ids) if args.ids else []
    if args.from_dir:
        dir_ids = extract_ids_from_dir(args.from_dir)
        print(f"Found {len(dir_ids)} strategy IDs in {args.from_dir}")
        ids.extend(dir_ids)

    if not ids:
        parser.print_help()
        print("\nError: No symphony IDs provided. Pass IDs directly or use --from-dir.")
        sys.exit(1)

    # Load credentials from shared .env
    api_key, api_secret = load_credentials()

    # Create output directory
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    print(f"Fetching {len(ids)} backtest(s)...")
    fetched_files = []
    for sid in ids:
        sid = sid.strip()
        # Handle full URLs
        if "composer.trade" in sid or "mymaestro.co" in sid:
            m = re.search(r"sid=([a-zA-Z0-9]+)", sid)
            if m:
                sid = m.group(1)

        path = fetch_backtest(sid, api_key, api_secret, output_dir)
        if path:
            fetched_files.append(path)

    print(f"\nFetched {len(fetched_files)}/{len(ids)} backtests to {output_dir}/")

    if args.analyze and len(fetched_files) >= 2:
        print("\nRunning Local Maestro analysis...")
        cmd = [sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "maestro.py")]
        cmd.extend(fetched_files)
        if args.start:
            cmd.extend(["--start", args.start])
        subprocess.run(cmd)
    elif args.analyze and len(fetched_files) < 2:
        print("Need at least 2 strategies for analysis.")


if __name__ == "__main__":
    main()
