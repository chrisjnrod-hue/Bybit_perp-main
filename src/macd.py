
from typing import List

def ema(values: List[float], period: int) -> List[float]:
    """
    Compute EMA for a list of values.
    Returns list of EMA values same length as input; first (period-1) entries will be None.
    """
    if period <= 0:
        raise ValueError("period must be > 0")
    res = [None] * len(values)
    k = 2 / (period + 1)
    # find first index to start (simple SMA for seed)
    if len(values) < period:
        return res
    sma = sum(values[:period]) / period
    res[period - 1] = sma
    for i in range(period, len(values)):
        res[i] = (values[i] - res[i - 1]) * k + res[i - 1]
    return res

def macd_histogram(closes: List[float], fast=12, slow=26, signal=9):
    """
    Return tuples (macd_line, signal_line, histogram) aligned with closes.
    Entries before sufficient data will be None.
    """
    if len(closes) == 0:
        return [], [], []
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    macd = [None] * len(closes)
    for i in range(len(closes)):
        if ema_fast[i] is not None and ema_slow[i] is not None:
            macd[i] = ema_fast[i] - ema_slow[i]
    # signal line is ema of macd values (ignore None)
    # Replace None with 0 for seed handling (keeps alignment consistent)
    macd_vals = [v if v is not None else 0.0 for v in macd]
    sig = ema(macd_vals, signal)
    hist = [None] * len(closes)
    for i in range(len(closes)):
        if macd[i] is not None and sig[i] is not None:
            hist[i] = macd[i] - sig[i]
    return macd, sig, hist

def slope(values: List[float], lookback=3):
    """
    Simple slope: difference between last value and value lookback candles ago.
    Returns None if insufficient data.
    """
    if not values or len(values) < lookback + 1:
        return None
    # find last valid values
    last_idx = len(values) - 1
    prev_idx = last_idx - lookback
    if values[last_idx] is None or values[prev_idx] is None:
        return None
    return values[last_idx] - values[prev_idx]
