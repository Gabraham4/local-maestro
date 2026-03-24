#!/usr/bin/env python3
"""Generate sample CSV data for testing Local Maestro."""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

np.random.seed(42)

# Generate 2 years of trading days
start = datetime(2023, 1, 3)
dates = pd.bdate_range(start, periods=504)  # ~2 years of trading days

# Generate 4 synthetic strategy equity curves with different characteristics
strategies = {}

# Strategy 1: High return, high volatility (like a leveraged equity strategy)
daily_ret = np.random.normal(0.0012, 0.025, len(dates))
daily_ret[100:110] = np.random.normal(-0.03, 0.02, 10)  # Drawdown period
equity = 10000 * np.cumprod(1 + daily_ret)
strategies["Aggressive Momentum"] = equity

# Strategy 2: Moderate return, low volatility (like a bond rotation strategy)
daily_ret = np.random.normal(0.0005, 0.008, len(dates))
daily_ret[200:210] = np.random.normal(-0.01, 0.005, 10)  # Small drawdown
equity = 10000 * np.cumprod(1 + daily_ret)
strategies["Bond Rotation"] = equity

# Strategy 3: High return, moderate vol, somewhat correlated with Strategy 1
base = np.random.normal(0.001, 0.018, len(dates))
corr_component = strategies["Aggressive Momentum"] / np.roll(strategies["Aggressive Momentum"], 1) - 1
corr_component[0] = 0
daily_ret = 0.6 * base + 0.4 * corr_component * 0.8
equity = 10000 * np.cumprod(1 + daily_ret)
strategies["Sector Rotation"] = equity

# Strategy 4: Uncorrelated, steady returns (like a volatility strategy)
daily_ret = np.random.normal(0.0008, 0.015, len(dates))
# Anti-correlated during drawdowns
daily_ret[100:110] = np.random.normal(0.02, 0.01, 10)  # Gains during S1 drawdown
equity = 10000 * np.cumprod(1 + daily_ret)
strategies["Vol Harvester"] = equity

# Save as CSV
df = pd.DataFrame(strategies, index=dates)
df.index.name = "date"
output_path = "sample_data/sample_4_strategies.csv"
df.to_csv(output_path)
print(f"Generated {output_path}: {len(dates)} days, {len(strategies)} strategies")
print("\nStrategies:")
for name, eq in strategies.items():
    cum_ret = (eq[-1] / eq[0] - 1) * 100
    print(f"  {name}: {cum_ret:.1f}% cumulative return")
