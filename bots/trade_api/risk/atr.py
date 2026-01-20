import numpy as np

def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> float:
    tr = np.maximum(high[1:], close[:-1]) - np.minimum(low[1:], close[:-1])
    if len(tr) < period:
        return float(tr.mean()) if len(tr) else 0.0
    # rolling mean ohne pandas (Heroku-freundlich)
    if len(tr) < period:
        return float(tr.mean()) if len(tr) else 0.0
    window = tr[-period:]
    return float(np.mean(window))

def position_size(balance_usd: float, entry: float, atr_val: float, risk_pct: float = 0.01) -> float:
    # position size = risk capital / (ATR as $ stop width)
    risk_cap = balance_usd * risk_pct
    stop_width = max(atr_val, entry * 0.0025)  # floor stop width 0.25%
    size = risk_cap / stop_width
    return float(max(size, 0.0))
