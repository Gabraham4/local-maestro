# Gobi's Portfolio Merger

Merges multiple Composer strategy JSON files into a single `master.json` for use with Rainboy's backtester. Originally shared by Gobi in the Composer community (Feb 2026).

## Setup

1. Export your strategy JSONs using the Symphony Tools extension (copy/paste each into individual `.json` files)
2. Place all `.json` files in this folder alongside `merge.py`

## Usage

```bash
# Interactive — pick files and set allocation weights
python merge.py

# Non-interactive — merge all files with equal weight
python merge.py --all
```

## How It Works

The script reads your strategy JSONs, lets you select which to merge, then asks for allocation percentages (equal weight by default, or custom like `50,30,20`).

**Output structure depends on your allocation choice:**

| Allocation | Resulting JSON Structure |
|------------|--------------------------|
| Single strategy (100%) | Strategy copied as-is |
| Equal weight | `root -> wt-cash-equal -> [all children]` |
| Custom weights (e.g. 50/30/20) | `root -> wt-cash-specified -> [wt-cash-equal per strategy with num/den weights]` |

The output `master.json` can be imported directly into Rainboy's backtester to evaluate the combined portfolio.

## Requirements

Python 3 (standard library only — no pip installs needed).
