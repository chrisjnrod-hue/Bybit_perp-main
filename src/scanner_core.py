# scanner_core.py - helpers (normalization, MACD, flip detection, TV rating)
import math
import statistics
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional, Tuple, Callable, Any as AnyT

getcontext().prec = 28

# Optional pandas/ta imports
try:
    import pandas as pd
    import numpy as np
except Exception:
    pd = None  # type: ignore
    np = None  # type: ignore

_PANDAS_TA_AVAILABLE = False
_PANDAS_TA_STYLE = False
_ta_module = None
try:
    import pandas_ta as ta  # type: ignore
    _ta_module = ta
    _PANDAS_TA_AVAILABLE = True
    _PANDAS_TA_STYLE = True
except Exception:
    try:
        import ta  # type: ignore
        _ta_module = ta
        _PANDAS_TA_AVAILABLE = True
        _PANDAS_TA_STYLE = False
    except Exception:
        _ta_module = None
        _PANDAS_TA_AVAILABLE = False
        _PANDAS_TA_STYLE = False

# Optional macd helper and slope
try:
    from .macd import macd_histogram, slope  # type: ignore
except Exception:
    macd_histogram = None
    slope = None  # type: ignore

# Timeframe helpers
def tf_to_seconds(tf: str) -> int:
    try:
        s = str(tf)
        if s.endswith("m"):
            return int(s[:-1]) * 60
        if s.endswith("h"):
            return int(s[:-1]) * 3600
        if s == "D" or s.endswith("d"):
            try:
                if s == "D":
                    return 24 * 3600
                return int(s[:-1]) * 86400
            except Exception:
                return 24 * 3600
        return int(s) * 60
    except Exception:
        return 60

def is_candle_age_acceptable(start_at: Optional[int], now: float, max_age_sec: int) -> bool:
    try:
        if max_age_sec is None or int(max_age_sec) <= 0:
            return True
        if start_at is None:
            return True
        start_sec = float(start_at) / 1000.0 if int(start_at) > 10000000000 else float(start_at)
        age = now - start_sec
        return age <= float(max_age_sec)
    except Exception:
        return True

# Normalization helpers
def normalize_klines(raw_klines: AnyT, tf: str) -> List[Dict[str, AnyT]]:
    out: List[Dict[str, AnyT]] = []
    if not raw_klines:
        return out
    if isinstance(raw_klines, dict):
        if "list" in raw_klines and isinstance(raw_klines["list"], (list, dict)):
            raw_klines = raw_klines["list"]
        elif "result" in raw_klines and isinstance(raw_klines["result"], (list, dict)):
            raw_klines = raw_klines["result"]
        elif "data" in raw_klines and isinstance(raw_klines["data"], (list, dict)):
            raw_klines = raw_klines["data"]
    if not isinstance(raw_klines, (list, tuple)):
        if isinstance(raw_klines, dict):
            seq = [raw_klines]
        else:
            seq = [raw_klines] if raw_klines else []
    else:
        seq = raw_klines
    for item in seq:
        try:
            if isinstance(item, (list, tuple)):
                start = None
                open_p = None
                high = None
                low = None
                close = None
                vol = None
                if len(item) >= 1:
                    try:
                        start = int(item[0])
                    except Exception:
                        start = None
                if len(item) >= 2:
                    try:
                        open_p = float(item[1])
                    except Exception:
                        open_p = None
                if len(item) >= 3:
                    try:
                        high = float(item[2])
                    except Exception:
                        high = None
                if len(item) >= 4:
                    try:
                        low = float(item[3])
                    except Exception:
                        low = None
                if len(item) >= 5:
                    try:
                        close = float(item[4])
                    except Exception:
                        close = None
                if len(item) >= 6:
                    try:
                        vol = float(item[5])
                    except Exception:
                        vol = None
                if close is not None:
                    out.append({
                        "start_at": start, "open": open_p, "high": high, "low": low,
                        "close": close, "volume": vol
                    })
                continue
            if isinstance(item, dict):
                start = (item.get("start_at") or item.get("open_time") or item.get("t") or item.get("timestamp") or item.get("start") or item.get("time"))
                open_p = (item.get("open") or item.get("openPrice") or item.get("o"))
                high = (item.get("high") or item.get("h"))
                low = (item.get("low") or item.get("l"))
                close = (item.get("close") or item.get("close_price") or item.get("c") or item.get("last_price") or item.get("Close"))
                vol = (item.get("volume") or item.get("vol") or item.get("turnover") or item.get("v") or item.get("quoteAsset"))
                is_closed = item.get("isClosed")
                if is_closed is None:
                    is_closed = item.get("is_closed") or item.get("complete") or item.get("confirmed")
                try:
                    if start is not None:
                        start = int(start)
                except Exception:
                    start = None
                try:
                    if open_p is not None:
                        open_p = float(open_p)
                except Exception:
                    open_p = None
                try:
                    if high is not None:
                        high = float(high)
                except Exception:
                    high = None
                try:
                    if low is not None:
                        low = float(low)
                except Exception:
                    low = None
                try:
                    if close is not None:
                        close = float(close)
                except Exception:
                    close = None
                try:
                    if vol is not None:
                        vol = float(vol)
                except Exception:
                    vol = None
                if close is not None:
                    out.append({
                        "start_at": start, "open": open_p, "high": high, "low": low,
                        "close": close, "volume": vol, "is_closed": is_closed
                    })
                continue
        except Exception:
            continue
    return out

# Quantize helper
def quantize_qty(qty: float, step: Optional[float], min_qty: Optional[float]) -> float:
    if qty is None:
        return 0.0
    qty_d = Decimal(str(qty))
    if step is None or step <= 0:
        if min_qty and qty_d < Decimal(str(min_qty)):
            return float(Decimal(str(min_qty)))
        return float(qty_d)
    step_d = Decimal(str(step))
    mult = (qty_d / step_d).to_integral_value(rounding=ROUND_DOWN)
    quant = (mult * step_d)
    if min_qty is not None:
        min_d = Decimal(str(min_qty))
        if quant < min_d:
            quant = min_d
    try:
        quant = quant.normalize()
    except Exception:
        pass
    return float(quant)

# MACD helpers
def _is_finite_number(x: Any) -> bool:
    try:
        v = float(x)
        return math.isfinite(v)
    except Exception:
        return False

def _clean_hist(hist: Any) -> List[float]:
    out: List[float] = []
    if hist is None:
        return out
    try:
        if hasattr(hist, "tolist"):
            iterable = hist.tolist()
        else:
            iterable = list(hist) if not isinstance(hist, (str, bytes)) else [hist]
    except Exception:
        try:
            iterable = list(hist)
        except Exception:
            iterable = [hist]
    for x in iterable:
        try:
            if x is None:
                continue
            v = float(x)
            if math.isnan(v) or not math.isfinite(v):
                continue
            out.append(v)
        except Exception:
            continue
    return out

def _fallback_macd(closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[List[float], List[float], List[float]]:
    if not closes:
        return [], [], []
    def ema(values: List[float], period: int) -> List[float]:
        if not values or period < 1:
            return []
        out: List[float] = []
        alpha = 2.0 / (period + 1.0)
        if len(values) < period:
            sma_val = sum(values) / len(values)
            for _ in values:
                out.append(sma_val)
            return out
        sma_val = sum(values[:period]) / period
        out.append(sma_val)
        for v in values[period:]:
            sma_val = (float(v) * alpha) + (sma_val * (1.0 - alpha))
            out.append(sma_val)
        return out
    try:
        fast_ema = ema(closes, fast)
        slow_ema = ema(closes, slow)
        if not fast_ema or not slow_ema or len(fast_ema) < slow or len(slow_ema) < slow:
            return [], [], []
        macd_series = [f - s for f, s in zip(fast_ema, slow_ema)]
        signal_series = ema(macd_series, signal)
        if not signal_series:
            return [], [], []
        hist_series = [m - s for m, s in zip(macd_series, signal_series)]
        return macd_series, signal_series, hist_series
    except Exception:
        return [], [], []

def compute_macd_from_closes(closes: List[float], include_price: Optional[float] = None, fast: int = 12, slow: int = 26, signal: int = 9):
    data: List[float] = []
    for c in closes:
        try:
            if c is None:
                continue
            val = float(c)
            if math.isfinite(val):
                data.append(val)
        except Exception:
            continue
    min_window = slow + signal  # conservative minimum
    if len(data) < min_window:
        return None, None, []
    if include_price is not None:
        try:
            current_price = float(include_price)
            if math.isfinite(current_price):
                data[-1] = current_price
        except Exception:
            pass
    if macd_histogram is not None:
        try:
            macd_line_raw, signal_line_raw, hist_raw = macd_histogram(data)
            hist_list = _clean_hist(hist_raw)
            macd_last = None
            signal_last = None
            try:
                if hasattr(macd_line_raw, "__len__") and len(macd_line_raw):
                    macd_last = float(macd_line_raw[-1])
            except Exception:
                pass
            try:
                if hasattr(signal_line_raw, "__len__") and len(signal_line_raw):
                    signal_last = float(signal_line_raw[-1])
            except Exception:
                pass
            if hist_list:
                return macd_last, signal_last, hist_list
        except Exception:
            pass
    macd_series, signal_series, hist_series = _fallback_macd(data, fast=fast, slow=slow, signal=signal)
    if not hist_series:
        return None, None, []
    macd_last = float(macd_series[-1]) if macd_series else None
    signal_last = float(signal_series[-1]) if signal_series else None
    return macd_last, signal_last, hist_series

def _safe_last(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if hasattr(v, "iloc"):
            if len(v) == 0:
                return None
            val = v.iloc[-1]
            return float(val) if _is_finite_number(val) else None
        if hasattr(v, "tolist"):
            lst = v.tolist()
            if not lst:
                return None
            return float(lst[-1]) if _is_finite_number(lst[-1]) else None
        if isinstance(v, (list, tuple)):
            if not v:
                return None
            return float(v[-1]) if _is_finite_number(v[-1]) else None
        return float(v) if _is_finite_number(v) else None
    except Exception:
        return None

# Flip detection
def detect_flip_current_open(hist: List[float], hist_threshold: Optional[float] = None, std_mult: float = 0.05, abs_min: float = 1e-9, lookback: int = 1) -> bool:
    try:
        clean = _clean_hist(hist)
        if not clean or len(clean) < (lookback + 1):
            return False
        prev = clean[-(lookback + 1)]
        cur = clean[-1]
        if prev is None or cur is None:
            return False
        if hist_threshold is None:
            try:
                hist_std = statistics.pstdev(clean) if len(clean) >= 2 else 0.0
            except Exception:
                hist_std = 0.0
            hist_threshold = max(abs_min, abs(hist_std) * std_mult)
            hist_threshold = min(hist_threshold, 0.01)
        flip = (prev < -1e-9) and (cur > hist_threshold)
        return flip
    except Exception:
        return False

# Volume helper
def compute_24h_volume_change_from(vol_data: Optional[Dict[str, float]]) -> Optional[float]:
    try:
        if not vol_data:
            return None
        prev_vol = vol_data.get("previous", 0)
        curr_vol = vol_data.get("current", 0)
        if prev_vol <= 0:
            return None
        change = (curr_vol - prev_vol) / prev_vol
        result = min(change, 1.0)
        return result
    except Exception:
        return None

# Indicator wrappers (use if pandas/ta available)
def _safe_sma(df_close, length: int):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.sma(df_close, length=length)
        else:
            return _ta_module.trend.SMAIndicator(df_close, window=int(length)).sma_indicator()
    except Exception:
        return None

def _safe_macd(df_close, fast: int, slow: int, signal: int):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.macd(df_close, fast=fast, slow=slow, signal=signal)
        else:
            macd_obj = _ta_module.trend.MACD(df_close, window_slow=int(slow), window_fast=int(fast), window_sign=int(signal))
            try:
                import pandas as _pd
                df_macd = _pd.DataFrame({
                    "MACD": macd_obj.macd(),
                    "MACD_signal": macd_obj.macd_signal(),
                    "MACD_hist": macd_obj.macd_diff()
                }, index=df_close.index)
                return df_macd
            except Exception:
                return None
    except Exception:
        return None

def _safe_rsi(df_close, length: int):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.rsi(df_close, length=length)
        else:
            return _ta_module.momentum.RSIIndicator(df_close, window=int(length)).rsi()
    except Exception:
        return None

def _safe_stoch(df_high, df_low, df_close, k: int, d: int):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.stoch(high=df_high, low=df_low, close=df_close, k=k, d=d)
        else:
            stoch_obj = _ta_module.momentum.StochasticOscillator(high=df_high, low=df_low, close=df_close, window=int(k), smooth_window=int(d))
            try:
                import pandas as _pd
                df_st = _pd.DataFrame({
                    "STOCHk": stoch_obj.stoch(),
                    "STOCHd": stoch_obj.stoch_signal()
                }, index=df_close.index)
                return df_st
            except Exception:
                return None
    except Exception:
        return None

def _safe_adx(df_high, df_low, df_close, length: int):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.adx(high=df_high, low=df_low, close=df_close, length=length)
        else:
            adx_obj = _ta_module.trend.ADXIndicator(high=df_high, low=df_low, close=df_close, window=int(length))
            try:
                import pandas as _pd
                df_adx = _pd.DataFrame({"ADX": adx_obj.adx()}, index=df_close.index)
                return df_adx
            except Exception:
                return None
    except Exception:
        return None

def _safe_obv(df_close, df_volume):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.obv(df_close, df_volume)
        else:
            return _ta_module.volume.OnBalanceVolumeIndicator(close=df_close, volume=df_volume).on_balance_volume()
    except Exception:
        return None

def _safe_bbands(df_close, length: int, std: float):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.bbands(df_close, length=length, std=std)
        else:
            bb = _ta_module.volatility.BollingerBands(close=df_close, window=int(length), window_dev=float(std))
            try:
                import pandas as _pd
                df_bb = _pd.DataFrame({
                    "BB_bbm": bb.bollinger_mavg(),
                    "BB_bbh": bb.bollinger_hband(),
                    "BB_bbl": bb.bollinger_lband()
                }, index=df_close.index)
                return df_bb
            except Exception:
                return None
    except Exception:
        return None

# TV rating and MTF alignment omitted here for brevity (copy/paste from your original if needed)
