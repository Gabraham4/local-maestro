#!/usr/bin/env python3
"""
Local Maestro - Offline Portfolio Correlation & Risk Analysis
A local recreation of MyMaestro.co for analyzing Composer trading strategies.

Usage:
    # Analyze Composer backtest cache JSON files
    python maestro.py backtest1.json backtest2.json backtest3.json

    # Analyze all JSON files in a directory
    python maestro.py --dir ./backtest_cache/

    # Analyze with custom date range
    python maestro.py --dir ./data/ --start 2023-01-01 --end 2025-12-31

    # Analyze with custom weights (decimal, must sum to 1.0)
    python maestro.py file1.json file2.json --weights 0.6 0.4

    # Auto-backtest strategy JSON files via Rainboy
    python maestro.py --backtest strategy1.json strategy2.json

    # Analyze a CSV file (columns: date, strategy1, strategy2, ...)
    python maestro.py portfolio_data.csv

    # Custom output path
    python maestro.py files... --output my_report.html
"""

import argparse
import json
import os
import sys
import webbrowser
from datetime import datetime
from pathlib import Path

# Add lib to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "lib"))

from data_loader import load_from_path, load_composer_backtest_json, align_strategies
from analytics import PortfolioAnalyzer
from report import generate_html


def main():
    parser = argparse.ArgumentParser(
        description="Local Maestro - Portfolio Correlation & Risk Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("files", nargs="*", help="Backtest JSON, CSV, or HTML report files")
    parser.add_argument("--dir", "-d", help="Directory containing backtest files to analyze")
    parser.add_argument("--start", "-s", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", "-e", help="End date (YYYY-MM-DD)")
    parser.add_argument("--weights", "-w", nargs="+", type=float,
                        help="Portfolio weights (decimal, must sum to 1.0)")
    parser.add_argument("--window", type=int, default=21,
                        help="Rolling window size in trading days (default: 21)")
    parser.add_argument("--output", "-o", help="Output HTML file path")
    parser.add_argument("--backtest", "-b", action="store_true",
                        help="Auto-backtest strategy JSON files via Rainboy backtester")
    parser.add_argument("--no-open", action="store_true",
                        help="Don't automatically open the report in browser")

    args = parser.parse_args()

    # Collect input files
    files = list(args.files) if args.files else []
    if args.dir:
        dir_path = Path(args.dir)
        if not dir_path.exists():
            print(f"Error: Directory {args.dir} does not exist")
            sys.exit(1)
        for ext in ["*.json", "*.csv", "*.html"]:
            files.extend(str(p) for p in dir_path.glob(ext))

    if not files:
        parser.print_help()
        print("\nError: No input files specified. Provide file paths or use --dir.")
        sys.exit(1)

    print(f"Loading {len(files)} file(s)...")

    # Load all strategies
    all_strategies = []
    for filepath in files:
        try:
            kwargs = {}
            if args.backtest:
                kwargs["backtest_sh_path"] = None  # Use default path
            if args.start:
                kwargs["start_date"] = args.start

            strategies = load_from_path(filepath, **kwargs)
            all_strategies.extend(strategies)
            for s in strategies:
                print(f"  Loaded: {s['name']} ({len(s['dates'])} days)")
        except Exception as e:
            print(f"  Warning: Could not load {filepath}: {e}")

    if len(all_strategies) < 2:
        print("Error: Need at least 2 strategies for portfolio analysis.")
        sys.exit(1)

    # Filter out strategies with too few data points for meaningful analysis
    min_days = 60
    before = len(all_strategies)
    all_strategies = [s for s in all_strategies if len(s["dates"]) >= min_days]
    if len(all_strategies) < before:
        print(f"  Filtered out {before - len(all_strategies)} strategies with < {min_days} days of data")

    print(f"\nAligning {len(all_strategies)} strategies to common date range...")

    # Align strategies
    dates, equity_df = align_strategies(all_strategies, args.start, args.end)
    print(f"  Aligned: {len(dates)} trading days from {dates[0].strftime('%Y-%m-%d')} to {dates[-1].strftime('%Y-%m-%d')}")

    # Set up weights
    weights = None
    if args.weights:
        if len(args.weights) != len(all_strategies):
            print(f"Error: Got {len(args.weights)} weights but {len(all_strategies)} strategies")
            sys.exit(1)
        total = sum(args.weights)
        if abs(total - 1.0) > 0.01:
            print(f"Warning: Weights sum to {total:.3f}, not 1.0. Normalizing.")
            args.weights = [w / total for w in args.weights]
        weights = dict(zip(equity_df.columns, args.weights))

    # Run analysis
    print("Running analysis...")
    analyzer = PortfolioAnalyzer(equity_df, weights=weights, rolling_window=args.window)
    analysis = analyzer.full_analysis()

    # Generate report
    if args.output:
        output_path = args.output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        reports_dir = os.path.join(os.path.dirname(__file__), "reports")
        os.makedirs(reports_dir, exist_ok=True)
        output_path = os.path.join(reports_dir, f"maestro_{timestamp}.html")

    print(f"Generating report...")
    generate_html(analysis, output_path)
    print(f"Report saved to: {output_path}")

    # Open in browser
    if not args.no_open:
        webbrowser.open(f"file://{os.path.abspath(output_path)}")
        print("Opened in browser.")


if __name__ == "__main__":
    main()
