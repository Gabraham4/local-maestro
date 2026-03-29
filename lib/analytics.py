"""
Core analytics engine for Local Maestro.
Computes all portfolio metrics, correlations, rolling statistics.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple


TRADING_DAYS_PER_YEAR = 252
RISK_FREE_RATE_DAILY = 0.0  # Can be adjusted


def optimize_portfolios(returns_df: pd.DataFrame, n_random: int = 50000,
                         n_boundary: int = 5000,
                         benchmark_returns: np.ndarray = None) -> Dict:
    """
    Monte Carlo portfolio optimization.
    Generates random weight combinations, computes metrics for each.
    Returns JSON-serializable results for the efficient frontier scatter plot.

    Args:
        returns_df: DataFrame of daily returns (columns = strategy names)
        n_random: Number of random portfolios (Dirichlet-distributed weights)
        n_boundary: Number of boundary portfolios (concentrated in 1-3 strategies)
        benchmark_returns: Optional 1-D array of daily benchmark returns (e.g. SPY), same length as returns_df
    """
    names = list(returns_df.columns)
    n_strats = len(names)
    n_days = len(returns_df)
    R = returns_df.values  # (n_days, n_strats)

    # ── Generate weight vectors ──
    w_random = np.random.dirichlet(np.ones(n_strats), size=n_random)

    w_boundary = []
    for _ in range(n_boundary):
        k = np.random.randint(1, min(4, n_strats + 1))
        chosen = np.random.choice(n_strats, size=k, replace=False)
        w = np.zeros(n_strats)
        raw = np.random.dirichlet(np.ones(k) * 0.3)
        w[chosen] = raw
        w_boundary.append(w)
    w_boundary = np.array(w_boundary)

    w_equal = np.ones((1, n_strats)) / n_strats
    W = np.vstack([w_equal, w_random, w_boundary])
    N = len(W)

    # ── Portfolio daily returns: (N, n_days) ──
    P = W @ R.T

    # ── Vectorized metric computation ──
    annual = TRADING_DAYS_PER_YEAR

    cum = np.prod(1 + P, axis=1)
    n_years = n_days / annual
    ann_ret = (np.power(cum, 1.0 / n_years) - 1) * 100
    ann_vol = np.std(P, axis=1, ddof=1) * np.sqrt(annual) * 100

    mean_daily = np.mean(P, axis=1)
    std_daily = np.std(P, axis=1, ddof=1)
    sharpe = np.where(std_daily > 0, (mean_daily / std_daily) * np.sqrt(annual), 0)

    downside = np.minimum(P, 0)
    downside_rms = np.sqrt(np.mean(downside ** 2, axis=1))
    sortino = np.where(downside_rms > 0, (mean_daily / downside_rms) * np.sqrt(annual), 0)

    cum_equity = np.cumprod(1 + P, axis=1)
    running_max = np.maximum.accumulate(cum_equity, axis=1)
    dd = cum_equity / running_max - 1
    max_dd = np.min(dd, axis=1) * 100

    calmar = np.where(max_dd < 0, ann_ret / np.abs(max_dd), 0)

    # ── Win rate (% of positive days) ──
    win_rate = (np.sum(P > 0, axis=1) / n_days) * 100

    # ── Profit factor (gross gains / gross losses) ──
    gross_gains = np.sum(np.maximum(P, 0), axis=1)
    gross_losses = np.abs(np.sum(np.minimum(P, 0), axis=1))
    profit_factor = np.where(gross_losses > 0, gross_gains / gross_losses, 10.0)

    # ── Correlation-based metrics ──
    # Compute pairwise strategy correlation matrix once
    corr_matrix = returns_df.corr().values  # (n_strats, n_strats)

    # Weighted mean pairwise correlation for each portfolio:
    # mean_corr = sum(w_i * w_j * corr_ij for i<j) / sum(w_i * w_j for i<j)
    # Efficient: mean_corr = (w^T @ C @ w - sum(w_i^2)) / (1 - sum(w_i^2))
    wCw = np.einsum('ni,ij,nj->n', W, corr_matrix, W)  # w^T C w per portfolio
    w_sq_sum = np.sum(W ** 2, axis=1)
    with np.errstate(divide='ignore', invalid='ignore'):
        mean_corr = np.where(w_sq_sum < 1.0 - 1e-8,
                             (wCw - w_sq_sum) / (1.0 - w_sq_sum), 0)
    mean_corr = np.clip(mean_corr, -1, 1)

    # De-correlation score (1 - mean_corr; higher = more diversified)
    decorrelation = 1.0 - mean_corr

    # Drawdown correlation: correlation of strategy returns on portfolio drawdown days
    # Compute per-portfolio drawdown mask and mean DD correlation
    # (Expensive for N portfolios — use a fast approximation with the global portfolio DD mask)
    portfolio_dd_mask = dd < -0.01  # Days where portfolio is in meaningful drawdown (>1%)
    # For efficiency, compute DD correlation from strategy returns on DD days
    # Use the equal-weight portfolio's DD mask as a shared approximation
    eq_dd_mask = portfolio_dd_mask[0]  # Equal weight DD days
    if eq_dd_mask.sum() >= 5:
        dd_returns = R[eq_dd_mask]  # (dd_days, n_strats)
        dd_corr_matrix = np.corrcoef(dd_returns.T)
        dd_corr_matrix = np.nan_to_num(dd_corr_matrix, nan=0.0)
    else:
        dd_corr_matrix = corr_matrix

    wDw = np.einsum('ni,ij,nj->n', W, dd_corr_matrix, W)
    with np.errstate(divide='ignore', invalid='ignore'):
        mean_dd_corr = np.where(w_sq_sum < 1.0 - 1e-8,
                                (wDw - w_sq_sum) / (1.0 - w_sq_sum), 0)
    mean_dd_corr = np.clip(mean_dd_corr, -1, 1)

    # CARP = Sortino / (1 + mean_corr) — Correlation-Adjusted Risk-adjusted Performance
    carp = np.where((1 + mean_corr) > 0.01, sortino / (1 + mean_corr), 0)

    # Smart CARP = Sortino / (1 + mean_dd_corr) — drawdown-correlation-adjusted
    smart_carp = np.where((1 + mean_dd_corr) > 0.01, sortino / (1 + mean_dd_corr), 0)

    # ── Smart Sharpe / Smart Sortino (Lo 2002 autocorrelation adjustment) ──
    # Adjusts for serial correlation in returns that inflates naive Sharpe/Sortino.
    # penalty = sqrt(1 + 2 * sum_{k=1}^{q} (1 - k/(q+1)) * rho_k)
    # where rho_k = autocorrelation at lag k, q = number of lags (typically ~6 for daily)
    q_lags = min(6, n_days // 10)  # number of autocorrelation lags
    # Compute autocorrelation penalty per portfolio
    ac_penalty = np.ones(N)
    if q_lags > 0 and n_days > q_lags + 1:
        for lag in range(1, q_lags + 1):
            # Autocorrelation at lag k for each portfolio: corr(r_t, r_{t-k})
            r_early = P[:, :-lag]  # (N, n_days-lag)
            r_late = P[:, lag:]    # (N, n_days-lag)
            # Pearson correlation per row
            m_early = r_early.mean(axis=1, keepdims=True)
            m_late = r_late.mean(axis=1, keepdims=True)
            num = np.sum((r_early - m_early) * (r_late - m_late), axis=1)
            den = np.sqrt(np.sum((r_early - m_early)**2, axis=1) *
                          np.sum((r_late - m_late)**2, axis=1))
            rho_k = np.where(den > 1e-12, num / den, 0)
            weight = 1.0 - lag / (q_lags + 1)  # Bartlett kernel weight
            ac_penalty += 2 * weight * rho_k
    ac_penalty = np.sqrt(np.maximum(ac_penalty, 0.01))  # Floor to avoid division by ~0

    smart_sharpe = sharpe / ac_penalty
    smart_sortino = sortino / ac_penalty

    # ── CAPM Alpha vs benchmark (if provided) ──
    # alpha = portfolio_ann_return - (risk_free + beta * (benchmark_ann_return - risk_free))
    # With risk_free ≈ 0: alpha = portfolio_ann_return - beta * benchmark_ann_return
    # beta = cov(portfolio, benchmark) / var(benchmark)
    alpha = np.zeros(N)
    if benchmark_returns is not None and len(benchmark_returns) == n_days:
        bm = benchmark_returns
        bm_ann_ret = (np.prod(1 + bm) ** (1.0 / n_years) - 1) * 100
        # Vectorized beta: cov(P_i, bm) / var(bm) for each portfolio
        bm_demean = bm - bm.mean()
        bm_var = np.dot(bm_demean, bm_demean)  # N * var(bm), unnormalized
        if bm_var > 1e-12:
            P_demean = P - P.mean(axis=1, keepdims=True)  # (N, n_days)
            cov_P_bm = P_demean @ bm_demean  # (N,) — unnormalized covariance
            beta = cov_P_bm / bm_var  # (N,)
            alpha = ann_ret - beta * bm_ann_ret
        else:
            alpha = ann_ret - bm_ann_ret  # Fallback: excess return if benchmark is flat

    # ── Serenity = Sortino * Calmar (simplified proxy) ──
    # True Serenity is complex; this captures the spirit: high risk-adj return with low drawdown
    serenity = sortino * np.where(max_dd < 0, np.abs(ann_ret / max_dd), 0)

    # ── Clip extremes ──
    sharpe = np.clip(sharpe, -10, 30)
    sortino = np.clip(sortino, -10, 50)
    calmar = np.clip(calmar, -5, 20)
    smart_sharpe = np.clip(smart_sharpe, -10, 30)
    smart_sortino = np.clip(smart_sortino, -10, 50)
    carp = np.clip(carp, -10, 40)
    smart_carp = np.clip(smart_carp, -10, 60)
    profit_factor = np.clip(profit_factor, 0, 20)
    serenity = np.clip(serenity, -20, 100)

    return {
        "strategies": names,
        "n_portfolios": int(N),
        "n_days": n_days,
        "weights": W.round(3).tolist(),
        "ann_return": ann_ret.round(2).tolist(),
        "ann_vol": ann_vol.round(2).tolist(),
        "sharpe": sharpe.round(3).tolist(),
        "sortino": sortino.round(3).tolist(),
        "max_dd": max_dd.round(2).tolist(),
        "calmar": calmar.round(3).tolist(),
        "win_rate": win_rate.round(1).tolist(),
        "profit_factor": profit_factor.round(3).tolist(),
        "mean_corr": mean_corr.round(3).tolist(),
        "decorrelation": decorrelation.round(3).tolist(),
        "smart_sharpe": smart_sharpe.round(3).tolist(),
        "smart_sortino": smart_sortino.round(3).tolist(),
        "carp": carp.round(3).tolist(),
        "smart_carp": smart_carp.round(3).tolist(),
        "serenity": serenity.round(2).tolist(),
        "alpha": alpha.round(2).tolist(),
        "equal_weight_idx": 0,
    }


class PortfolioAnalyzer:
    """Analyzes a set of strategy equity curves and computes portfolio-level analytics."""

    def __init__(self, equity_df: pd.DataFrame, weights: Optional[Dict[str, float]] = None,
                 rolling_window: int = 21):
        """
        equity_df: DataFrame with DatetimeIndex and strategy names as columns.
                   Values are equity (rebased to 10000).
        weights: {strategy_name: weight}. Default: equal weight.
        rolling_window: Window size for rolling computations (default 21 trading days).
        """
        self.equity = equity_df
        self.names = list(equity_df.columns)
        self.n_strategies = len(self.names)
        self.window = rolling_window

        # Set weights
        if weights is None:
            w = 1.0 / self.n_strategies
            self.weights = {name: w for name in self.names}
        else:
            self.weights = weights

        # Compute daily returns
        self.returns = equity_df.pct_change().iloc[1:]  # Drop first NaN row
        self.dates = self.returns.index

        # Compute portfolio returns (daily rebalanced to target weights)
        weight_series = pd.Series([self.weights[n] for n in self.names], index=self.names)
        self.portfolio_returns = (self.returns * weight_series).sum(axis=1)
        self.portfolio_returns.name = "Portfolio"

        # Compute portfolio equity
        self.portfolio_equity = (1 + self.portfolio_returns).cumprod() * 10000
        self.portfolio_equity.name = "Portfolio"

        # Mean returns (average of all strategies, not weighted)
        self.mean_returns = self.returns.mean(axis=1)
        self.mean_returns.name = "Mean"
        self.mean_equity = (1 + self.mean_returns).cumprod() * 10000

    # ── Metric Computations ──────────────────────────────────────────

    def cumulative_return(self, returns: pd.Series) -> float:
        """Cumulative return as percentage."""
        return ((1 + returns).prod() - 1) * 100

    def annualized_return(self, returns: pd.Series) -> float:
        """Annualized return percentage."""
        cum = (1 + returns).prod()
        n_years = len(returns) / TRADING_DAYS_PER_YEAR
        if n_years <= 0:
            return 0.0
        return (cum ** (1 / n_years) - 1) * 100

    def exp_annualized_return(self, returns: pd.Series) -> float:
        """Expected annualized return (log-based)."""
        log_returns = np.log1p(returns)
        mean_log = log_returns.mean()
        return (np.exp(mean_log * TRADING_DAYS_PER_YEAR) - 1) * 100

    def daily_win_rate(self, returns: pd.Series) -> float:
        """Percentage of days with positive returns."""
        return (returns > 0).mean() * 100

    def max_drawdown(self, returns: pd.Series) -> float:
        """Maximum drawdown as positive percentage."""
        equity = (1 + returns).cumprod()
        running_max = equity.cummax()
        drawdown = (equity - running_max) / running_max
        return abs(drawdown.min()) * 100

    def drawdown_series(self, returns: pd.Series) -> pd.Series:
        """Daily drawdown series (negative values)."""
        equity = (1 + returns).cumprod()
        running_max = equity.cummax()
        return ((equity - running_max) / running_max) * 100

    def volatility(self, returns: pd.Series) -> float:
        """Annualized volatility percentage."""
        return returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR) * 100

    def sharpe_ratio(self, returns: pd.Series) -> float:
        """Annualized Sharpe ratio."""
        excess = returns - RISK_FREE_RATE_DAILY
        if excess.std() == 0:
            return 0.0
        return (excess.mean() / excess.std()) * np.sqrt(TRADING_DAYS_PER_YEAR)

    def sortino_ratio(self, returns: pd.Series) -> float:
        """Annualized Sortino ratio using full downside deviation.

        Uses all days, substituting 0 for positive returns, so the denominator
        reflects the proportion of down days as well as their magnitude.
        This matches MyMaestro.co's calculation.
        """
        excess = returns - RISK_FREE_RATE_DAILY
        downside = excess.copy()
        downside[downside > 0] = 0.0
        downside_dev = np.sqrt((downside ** 2).mean())
        if downside_dev == 0:
            return float('inf') if excess.mean() > 0 else 0.0
        return (excess.mean() / downside_dev) * np.sqrt(TRADING_DAYS_PER_YEAR)

    def calmar_ratio(self, returns: pd.Series) -> float:
        """Calmar ratio = annualized return / max drawdown."""
        mdd = self.max_drawdown(returns)
        if mdd == 0:
            return float('inf')
        ann_ret = self.annualized_return(returns)
        return ann_ret / mdd

    # ── Correlation Computations ─────────────────────────────────────

    def correlation_matrix(self) -> pd.DataFrame:
        """Pairwise correlation matrix of daily returns."""
        return self.returns.corr()

    def drawdown_correlation_matrix(self) -> pd.DataFrame:
        """
        Correlation matrix computed only on days when the portfolio is in drawdown.
        """
        portfolio_dd = self.drawdown_series(self.portfolio_returns)
        in_drawdown = portfolio_dd < -1.0  # Portfolio is in meaningful drawdown (below -1%)
        if in_drawdown.sum() < 5:
            # Not enough drawdown days, use all days
            return self.returns.corr()
        dd_returns = self.returns[in_drawdown]
        return dd_returns.corr()

    def mean_correlation(self, corr_matrix: pd.DataFrame = None) -> Dict[str, float]:
        """Mean correlation for each strategy (excluding self)."""
        if corr_matrix is None:
            corr_matrix = self.correlation_matrix()
        result = {}
        for name in self.names:
            others = [corr_matrix.loc[name, other] for other in self.names if other != name]
            result[name] = np.mean(others) if others else 0.0
        return result

    def portfolio_mean_correlation(self, corr_matrix: pd.DataFrame = None) -> float:
        """
        Weight-adjusted mean pairwise correlation.
        Uses portfolio weights so that heavily-weighted strategy pairs
        contribute more to the mean than low-weight pairs.
        Formula: (w^T C w - sum(w_i^2)) / (1 - sum(w_i^2))
        Falls back to simple average for equal weights (same result).
        """
        if corr_matrix is None:
            corr_matrix = self.correlation_matrix()
        n = len(self.names)
        if n < 2:
            return 0.0
        w = np.array([self.weights[name] for name in self.names])
        C = corr_matrix.loc[self.names, self.names].values
        wCw = w @ C @ w
        w_sq_sum = np.sum(w ** 2)
        if w_sq_sum >= 1.0 - 1e-8:
            # Single strategy dominates — correlation is meaningless
            return 0.0
        return float(np.clip((wCw - w_sq_sum) / (1.0 - w_sq_sum), -1, 1))

    # ── CARP Computations ────────────────────────────────────────────

    def carp_ratio(self) -> float:
        """
        CARP = Sortino / (1 + mean_correlation)
        Correlation And Risk-adjusted Performance.
        """
        sortino = self.sortino_ratio(self.portfolio_returns)
        mean_corr = self.portfolio_mean_correlation()
        denom = 1 + mean_corr
        if denom <= 0:
            return float('inf')
        return sortino / denom

    def smart_carp(self) -> float:
        """
        Smart CARP = Sortino / (1 + mean_drawdown_correlation)
        CARP factoring only drawdown-period correlations.
        """
        sortino = self.sortino_ratio(self.portfolio_returns)
        dd_corr = self.drawdown_correlation_matrix()
        mean_dd_corr = self.portfolio_mean_correlation(dd_corr)
        denom = 1 + mean_dd_corr
        if denom <= 0:
            return float('inf')
        return sortino / denom

    # ── Rolling Computations ─────────────────────────────────────────

    def rolling_returns(self) -> pd.DataFrame:
        """Rolling cumulative return over window period (%)."""
        result = pd.DataFrame(index=self.dates)
        for name in self.names:
            result[name] = self.returns[name].rolling(self.window).apply(
                lambda x: ((1 + x).prod() - 1) * 100, raw=True
            )
        result["Portfolio"] = self.portfolio_returns.rolling(self.window).apply(
            lambda x: ((1 + x).prod() - 1) * 100, raw=True
        )
        result["Mean"] = result[self.names].mean(axis=1)
        return result

    def rolling_win_rate(self) -> pd.DataFrame:
        """Rolling daily win rate over window period (%)."""
        result = pd.DataFrame(index=self.dates)
        for name in self.names:
            result[name] = self.returns[name].rolling(self.window).apply(
                lambda x: (x > 0).mean() * 100, raw=True
            )
        result["Portfolio"] = self.portfolio_returns.rolling(self.window).apply(
            lambda x: (x > 0).mean() * 100, raw=True
        )
        result["Mean"] = result[self.names].mean(axis=1)
        return result

    def rolling_volatility(self) -> pd.DataFrame:
        """Rolling annualized volatility over window period (%)."""
        result = pd.DataFrame(index=self.dates)
        for name in self.names:
            result[name] = self.returns[name].rolling(self.window).std() * np.sqrt(TRADING_DAYS_PER_YEAR) * 100
        result["Portfolio"] = self.portfolio_returns.rolling(self.window).std() * np.sqrt(TRADING_DAYS_PER_YEAR) * 100
        result["Mean"] = result[self.names].mean(axis=1)
        return result

    def rolling_sharpe(self) -> pd.DataFrame:
        """Rolling Sharpe ratio over window period."""
        result = pd.DataFrame(index=self.dates)
        for name in self.names:
            mean_r = self.returns[name].rolling(self.window).mean()
            std_r = self.returns[name].rolling(self.window).std()
            result[name] = (mean_r / std_r) * np.sqrt(TRADING_DAYS_PER_YEAR)
        mean_r = self.portfolio_returns.rolling(self.window).mean()
        std_r = self.portfolio_returns.rolling(self.window).std()
        result["Portfolio"] = (mean_r / std_r) * np.sqrt(TRADING_DAYS_PER_YEAR)
        result["Mean"] = result[self.names].mean(axis=1)
        return result

    def rolling_sortino(self) -> pd.DataFrame:
        """Rolling Sortino ratio over window period."""
        result = pd.DataFrame(index=self.dates)

        def _sortino(x):
            excess = x - RISK_FREE_RATE_DAILY
            downside = np.where(excess < 0, excess, 0.0)
            ds = np.sqrt((downside ** 2).mean())
            if ds == 0:
                return np.nan
            return (excess.mean() / ds) * np.sqrt(TRADING_DAYS_PER_YEAR)

        for name in self.names:
            result[name] = self.returns[name].rolling(self.window).apply(_sortino, raw=True)
        result["Portfolio"] = self.portfolio_returns.rolling(self.window).apply(_sortino, raw=True)
        result["Mean"] = result[self.names].mean(axis=1)
        return result

    def rolling_correlation(self) -> pd.DataFrame:
        """
        Rolling mean pairwise correlation for each strategy.
        Returns DataFrame with strategy means and overall mean.
        """
        n = len(self.names)
        if n < 2:
            return pd.DataFrame(index=self.dates, columns=self.names + ["Mean"], data=0)

        # Compute rolling pairwise correlations
        pairwise_rolling = {}
        for i in range(n):
            for j in range(i + 1, n):
                pair = f"{self.names[i]}|{self.names[j]}"
                pairwise_rolling[pair] = self.returns[self.names[i]].rolling(self.window).corr(
                    self.returns[self.names[j]]
                )

        pair_df = pd.DataFrame(pairwise_rolling, index=self.dates)

        # Mean correlation per strategy
        result = pd.DataFrame(index=self.dates)
        for name in self.names:
            relevant = [col for col in pair_df.columns if name in col.split("|")]
            result[name] = pair_df[relevant].mean(axis=1)

        result["Mean"] = pair_df.mean(axis=1)
        return result

    def rolling_drawdown_correlation(self) -> pd.DataFrame:
        """
        Rolling mean pairwise correlation computed only on drawdown days.
        Uses a window approach where we look at drawdown subsets within each window.
        """
        n = len(self.names)
        if n < 2:
            return pd.DataFrame(index=self.dates, columns=self.names + ["Mean"], data=0)

        portfolio_dd = self.drawdown_series(self.portfolio_returns)

        # For each rolling window, compute correlation on days where portfolio was in drawdown
        result_data = []
        for i in range(len(self.dates)):
            if i < self.window - 1:
                result_data.append([np.nan] * (n + 1))
                continue

            window_slice = slice(i - self.window + 1, i + 1)
            window_dd = portfolio_dd.iloc[window_slice]
            dd_mask = window_dd < -1.0

            if dd_mask.sum() < 3:
                # Not enough drawdown days in window
                result_data.append([np.nan] * (n + 1))
                continue

            window_returns = self.returns.iloc[window_slice][dd_mask]
            try:
                corr = window_returns.corr()
            except Exception:
                result_data.append([np.nan] * (n + 1))
                continue

            row = []
            for name in self.names:
                others = [corr.loc[name, other] for other in self.names
                         if other != name and not np.isnan(corr.loc[name, other])]
                row.append(np.mean(others) if others else np.nan)

            # Overall mean
            total = []
            for ii in range(n):
                for jj in range(ii + 1, n):
                    v = corr.iloc[ii, jj]
                    if not np.isnan(v):
                        total.append(v)
            row.append(np.mean(total) if total else np.nan)
            result_data.append(row)

        return pd.DataFrame(
            result_data,
            index=self.dates,
            columns=self.names + ["Mean"],
        )

    def rolling_carp(self) -> pd.DataFrame:
        """Rolling CARP = rolling_sortino / (1 + rolling_mean_correlation)."""
        sortino = self.rolling_sortino()
        corr = self.rolling_correlation()
        result = pd.DataFrame(index=self.dates)
        result["Portfolio"] = sortino["Portfolio"] / (1 + corr["Mean"])
        return result

    def rolling_smart_carp(self) -> pd.DataFrame:
        """Rolling Smart CARP = rolling_sortino / (1 + rolling_mean_dd_correlation)."""
        sortino = self.rolling_sortino()
        dd_corr = self.rolling_drawdown_correlation()
        result = pd.DataFrame(index=self.dates)
        result["Portfolio"] = sortino["Portfolio"] / (1 + dd_corr["Mean"])
        return result

    # ── Summary Metrics ──────────────────────────────────────────────

    def strategy_metrics(self) -> List[Dict]:
        """Compute summary metrics for each strategy."""
        corr_matrix = self.correlation_matrix()
        dd_corr_matrix = self.drawdown_correlation_matrix()
        mean_corrs = self.mean_correlation(corr_matrix)
        mean_dd_corrs = self.mean_correlation(dd_corr_matrix)

        metrics = []
        for name in self.names:
            r = self.returns[name]
            metrics.append({
                "name": name,
                "cum_return": round(self.cumulative_return(r), 2),
                "ann_return": round(self.annualized_return(r), 2),
                "exp_ann_return": round(self.exp_annualized_return(r), 2),
                "daily_win_rate": round(self.daily_win_rate(r), 2),
                "max_drawdown": round(self.max_drawdown(r), 2),
                "calmar": round(self.calmar_ratio(r), 2),
                "volatility": round(self.volatility(r), 2),
                "sharpe": round(self.sharpe_ratio(r), 2),
                "sortino": round(self.sortino_ratio(r), 2),
                "mean_correlation": round(mean_corrs[name], 3),
                "mean_dd_correlation": round(mean_dd_corrs[name], 3),
            })
        return metrics

    def portfolio_metrics(self) -> Dict:
        """Compute summary metrics for the combined portfolio."""
        r = self.portfolio_returns
        weight_label = "Equal-Weight (Daily)" if len(set(self.weights.values())) == 1 else "Custom-Weight (Daily)"
        return {
            "name": f"Portfolio: {weight_label}",
            "cum_return": round(self.cumulative_return(r), 2),
            "ann_return": round(self.annualized_return(r), 2),
            "exp_ann_return": round(self.exp_annualized_return(r), 2),
            "daily_win_rate": round(self.daily_win_rate(r), 2),
            "max_drawdown": round(self.max_drawdown(r), 2),
            "calmar": round(self.calmar_ratio(r), 2),
            "volatility": round(self.volatility(r), 2),
            "sharpe": round(self.sharpe_ratio(r), 2),
            "sortino": round(self.sortino_ratio(r), 2),
            "carp": round(self.carp_ratio(), 2),
            "smart_carp": round(self.smart_carp(), 2),
        }

    def mean_metrics(self) -> Dict:
        """Compute mean of all strategy metrics."""
        strat_metrics = self.strategy_metrics()
        keys = ["cum_return", "ann_return", "exp_ann_return", "daily_win_rate",
                "max_drawdown", "calmar", "volatility", "sharpe", "sortino",
                "mean_correlation", "mean_dd_correlation"]
        result = {"name": "Mean"}
        for key in keys:
            vals = [m[key] for m in strat_metrics]
            result[key] = round(np.mean(vals), 2)
        return result

    # ── Full Analysis Output ─────────────────────────────────────────

    def full_analysis(self) -> Dict:
        """
        Run the complete analysis and return all data needed for the report.
        """
        # Metrics
        strat_metrics = self.strategy_metrics()
        port_metrics = self.portfolio_metrics()
        mean_met = self.mean_metrics()

        # Find highest/lowest for each metric
        highlight_keys = ["cum_return", "ann_return", "exp_ann_return", "daily_win_rate",
                         "max_drawdown", "calmar", "volatility", "sharpe", "sortino"]
        highlights = {}
        for key in highlight_keys:
            vals = [(m["name"], m[key]) for m in strat_metrics]
            highlights[key] = {
                "highest": max(vals, key=lambda x: x[1])[0],
                "lowest": min(vals, key=lambda x: x[1])[0],
            }

        # Correlation matrices
        corr_matrix = self.correlation_matrix()
        dd_corr_matrix = self.drawdown_correlation_matrix()

        # Rolling data
        rolling_ret = self.rolling_returns()
        rolling_wr = self.rolling_win_rate()
        rolling_vol = self.rolling_volatility()
        rolling_sh = self.rolling_sharpe()
        rolling_so = self.rolling_sortino()
        rolling_corr = self.rolling_correlation()
        rolling_dd_corr = self.rolling_drawdown_correlation()
        rolling_cp = self.rolling_carp()
        rolling_scp = self.rolling_smart_carp()

        # Drawdown series
        drawdowns = {}
        for name in self.names:
            drawdowns[name] = self.drawdown_series(self.returns[name])
        drawdowns["Portfolio"] = self.drawdown_series(self.portfolio_returns)

        # Cumulative returns for chart
        cum_returns = {}
        for name in self.names:
            cum_returns[name] = ((1 + self.returns[name]).cumprod() - 1) * 100
        cum_returns["Portfolio"] = ((1 + self.portfolio_returns).cumprod() - 1) * 100
        cum_returns["Mean"] = ((1 + self.mean_returns).cumprod() - 1) * 100

        def _to_json_safe(df_or_series):
            """Convert pandas object to JSON-safe format."""
            if isinstance(df_or_series, pd.DataFrame):
                return {
                    "dates": df_or_series.index.strftime("%Y-%m-%d").tolist(),
                    "columns": df_or_series.columns.tolist(),
                    "data": {col: df_or_series[col].replace([np.inf, -np.inf], np.nan).tolist()
                             for col in df_or_series.columns},
                }
            elif isinstance(df_or_series, pd.Series):
                return {
                    "dates": df_or_series.index.strftime("%Y-%m-%d").tolist(),
                    "values": df_or_series.replace([np.inf, -np.inf], np.nan).tolist(),
                }
            elif isinstance(df_or_series, dict):
                result = {}
                for k, v in df_or_series.items():
                    if isinstance(v, pd.Series):
                        result[k] = {
                            "dates": v.index.strftime("%Y-%m-%d").tolist(),
                            "values": v.replace([np.inf, -np.inf], np.nan).tolist(),
                        }
                    else:
                        result[k] = v
                return result
            return df_or_series

        return {
            "period": {
                "start": self.dates[0].strftime("%Y-%m-%d"),
                "end": self.dates[-1].strftime("%Y-%m-%d"),
                "trading_days": len(self.dates),
            },
            "strategy_names": self.names,
            "weights": self.weights,
            "metrics": {
                "portfolio": port_metrics,
                "strategies": strat_metrics,
                "mean": mean_met,
                "highlights": highlights,
            },
            "correlations": {
                "returns": corr_matrix.round(3).to_dict(),
                "drawdowns": dd_corr_matrix.round(3).to_dict(),
                "mean_corr": self.mean_correlation(corr_matrix),
                "mean_dd_corr": self.mean_correlation(dd_corr_matrix),
            },
            "charts": {
                "cumulative_returns": _to_json_safe(cum_returns),
                "drawdowns": _to_json_safe(drawdowns),
                "rolling_returns": _to_json_safe(rolling_ret),
                "rolling_win_rate": _to_json_safe(rolling_wr),
                "rolling_volatility": _to_json_safe(rolling_vol),
                "rolling_sharpe": _to_json_safe(rolling_sh),
                "rolling_sortino": _to_json_safe(rolling_so),
                "rolling_correlation": _to_json_safe(rolling_corr),
                "rolling_dd_correlation": _to_json_safe(rolling_dd_corr),
                "rolling_carp": _to_json_safe(rolling_cp),
                "rolling_smart_carp": _to_json_safe(rolling_scp),
            },
        }
