"""
scanner_core.py

Pure, synchronous helpers and deterministic computations extracted from scanner.py.

These functions:
- Do NOT perform network I/O
- Accept inputs (klines, closes, volume dicts, config) to be unit-testable
- Preserve behavior of original helpers (normalization, MACD wrapper, TV rating, quantize, MTF alignment)

Note: This module will attempt to import pandas_ta (preferred) or the alternative 'ta' package and will attempt to compute indicators using whichever is available. If no supported TA API is present, compute_tv_rating will return neutral.
"""
import math
import json
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional, Tuple, Callable, Any as AnyT

getcontext().prec = 28

# pandas/ta optional
try:
    import pandas as pd
    import numpy as np
except Exception:
    pd = None  # type: ignore
    np = None  # type: ignore

# Try to import pandas_ta first (preferred API: ta.sma, ta.macd, etc.)
_PANDAS_TA_AVAILABLE = False
_PANDAS_TA_STYLE = False
_ta_module = None
try:
    import pandas_ta as ta  # type: ignore
    _ta_module = ta
    _PANDAS_TA_AVAILABLE = True
    _PANDAS_TA_STYLE = True
except Exception:
    # Try the alternative 'ta' package (bukosabino) which uses class-based API
    try:
        import ta  # type: ignore
        _ta_module = ta
        _PANDAS_TA_AVAILABLE = True
        _PANDAS_TA_STYLE = False
    except Exception:
        ta = None  # type: ignore
        _ta_module = None
        _PANDAS_TA_AVAILABLE = False
        _PANDAS_TA_STYLE = False

# Import MACD helper and slope function from package (keeps same external dependency)
from .macd import macd_histogram, slope  # type: ignore


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


def normalize_klines(raw_klines: AnyT, tf: str) -> List[Dict[str, AnyT]]:
    """
    Normalize various kline shapes into list of dicts:
      {"start_at", "open", "high", "low", "close", "volume", "is_closed"(optional)}
    """
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
        seq = [raw_klines] if raw_klines else []
    else:
        seq = raw_klines

    for item in seq:
        try:
            if isinstance(item, (list, tuple)):
                # Handle list/tuple kline format
                start = int(item[0]) if len(item) >= 1 else None
                open_p = float(item[1]) if len(item) >= 2 else None
                high = float(item[2]) if len(item) >= 3 else None
                low = float(item[3]) if len(item) >= 4 else None
                close = float(item[4]) if len(item) >= 5 else None
                vol = float(item[5]) if len(item) >= 6 else None
                
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
                vol = (item.get("volume") or item.get("vol") or item.get("turnover") or item.get("v"))
                
                if close is not None:
                    out.append({
                        "start_at": int(start) if start else None, 
                        "open": float(open_p) if open_p else None, 
                        "high": float(high) if high else None, 
                        "low": float(low) if low else None,
                        "close": float(close), 
                        "volume": float(vol) if vol else None
                    })
                continue
        except Exception:
            continue
    return out


def quantize_qty(qty: float, step: Optional[float], min_qty: Optional[float]) -> float:
    if qty is None: return 0.0
    qty_d = Decimal(str(qty))
    if step is None or step <= 0:
        if min_qty and qty_d < Decimal(str(min_qty)): return float(Decimal(str(min_qty)))
        return float(qty_d)
    step_d = Decimal(str(step))
    mult = (qty_d / step_d).to_integral_value(rounding=ROUND_DOWN)
    quant = (mult * step_d)
    if min_qty is not None:
        min_d = Decimal(str(min_qty))
        if quant < min_d: quant = min_d
    return float(quant.normalize())


def compute_macd_from_closes(closes: List[float], include_price: Optional[float] = None, is_new_candle: bool = False):
    """
    Compute MACD histogram from a list of closes (floats).
    include_price: when provided, sets the last candle price.
    is_new_candle: if True, treats include_price as a brand new entry; 
                   if False, updates/overwrites the most recent entry.
    """
    data: List[float] = []
    for c in closes:
        try:
            if c is None: continue
            data.append(float(c))
        except Exception: continue

    if include_price is not None:
        current_price = float(include_price)
        # SAFEGUARD: Append if explicitly new or list is empty, otherwise overwrite
        if is_new_candle or not data:
            data.append(current_price)
        else:
            data[-1] = current_price

    macd_line, signal_line, hist = macd_histogram(data)
    try:
        hist = [None if v is None else float(v) for v in (hist or [])]
    except Exception:
        pass
    return macd_line, signal_line, hist


def detect_flip_current_open(hist: List[float], hist_threshold: float = 0.0) -> bool:
    if not hist or len(hist) < 2: return False
    prev = hist[-2]
    cur = hist[-1]
    if prev is None or cur is None: return False
    return (prev <= 0 and cur > 0)


def compute_24h_volume_change_from(vol_data: Optional[Dict[str, float]]) -> Optional[float]:
    try:
        if not vol_data: return None
        prev_vol = vol_data.get("previous", 0)
        curr_vol = vol_data.get("current", 0)
        if prev_vol <= 0: return None
        change = (curr_vol - prev_vol) / prev_vol
        return min(change, 1.0)
    except Exception:
        return None


# --- Helper wrappers for indicator functions ---
def _safe_sma(df_close, length: int):
    try:
        return _ta_module.sma(df_close, length=length) if _PANDAS_TA_STYLE else _ta_module.trend.SMAIndicator(df_close, window=int(length)).sma_indicator()
    except Exception: return None


def _safe_macd(df_close, fast: int, slow: int, signal: int):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.macd(df_close, fast=fast, slow=slow, signal=signal)
        else:
            macd_obj = _ta_module.trend.MACD(df_close, window_slow=int(slow), window_fast=int(fast), window_sign=int(signal))
            import pandas as _pd
            return _pd.DataFrame({"MACD": macd_obj.macd(), "MACD_signal": macd_obj.macd_signal(), "MACD_hist": macd_obj.macd_diff()}, index=df_close.index)
    except Exception: return None


def _safe_rsi(df_close, length: int):
    try:
        return _ta_module.rsi(df_close, length=length) if _PANDAS_TA_STYLE else _ta_module.momentum.RSIIndicator(df_close, window=int(length)).rsi()
    except Exception: return None


def _safe_stoch(df_high, df_low, df_close, k: int, d: int):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.stoch(high=df_high, low=df_low, close=df_close, k=k, d=d)
        else:
            stoch_obj = _ta_module.momentum.StochasticOscillator(high=df_high, low=df_low, close=df_close, window=int(k), smooth_window=int(d))
            import pandas as _pd
            return _pd.DataFrame({"STOCHk": stoch_obj.stoch(), "STOCHd": stoch_obj.stoch_signal()}, index=df_close.index)
    except Exception: return None


def _safe_adx(df_high, df_low, df_close, length: int):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.adx(high=df_high, low=df_low, close=df_close, length=length)
        else:
            adx_obj = _ta_module.trend.ADXIndicator(high=df_high, low=df_low, close=df_close, window=int(length))
            import pandas as _pd
            return _pd.DataFrame({"ADX": adx_obj.adx()}, index=df_close.index)
    except Exception: return None


def _safe_obv(df_close, df_volume):
    try:
        return _ta_module.obv(df_close, df_volume) if _PANDAS_TA_STYLE else _ta_module.volume.OnBalanceVolumeIndicator(close=df_close, volume=df_volume).on_balance_volume()
    except Exception: return None


def _safe_bbands(df_close, length: int, std: float):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.bbands(df_close, length=length, std=std)
        else:
            bb = _ta_module.volatility.BollingerBands(close=df_close, window=int(length), window_dev=float(std))
            import pandas as _pd
            return _pd.DataFrame({"BB_bbm": bb.bollinger_mavg(), "BB_bbh": bb.bollinger_hband(), "BB_bbl": bb.bollinger_lband()}, index=df_close.index)
    except Exception: return None


def compute_tv_rating_from(klines: List[Dict[str, AnyT]], cfg: Dict[str, AnyT], tf: Optional[str] = None, price: Optional[float] = None) -> Tuple[float, str]:
    if not cfg.get("enabled", True) or not _PANDAS_TA_AVAILABLE or pd is None or np is None:
        return 0.0, "Neutral"
    try:
        df = pd.DataFrame(klines)
        for col in ("open", "high", "low", "close", "volume"):
            if col not in df.columns: df[col] = np.nan
        df = df.dropna(subset=["close"]).copy()
        
        if price is not None: df.at[df.index[-1], "close"] = float(price)

        # Indicators
        ma_lengths = sorted(set([n for pair in cfg["indicators"]["ma_pairs"] for n in pair]))
        for l in ma_lengths:
            sma = _safe_sma(df["close"], length=int(l))
            if sma is not None: df[f"sma_{l}"] = sma
            
        macd_cfg = cfg["indicators"].get("macd", [12, 26, 9])
        mr = _safe_macd(df["close"], fast=int(macd_cfg[0]), slow=int(macd_cfg[1]), signal=int(macd_cfg[2]))
        df["macd_hist"] = mr.iloc[:, -1] if mr is not None else 0.0
        
        df["rsi"] = _safe_rsi(df["close"], length=int(cfg["indicators"].get("rsi_period", 14)))
        
        # Scoring logic...
        last = df.iloc[-1]
        scores = []
        weights = cfg.get("weights", {})
        
        for pair in cfg["indicators"]["ma_pairs"]:
            s, l = last.get(f"sma_{pair[0]}"), last.get(f"sma_{pair[1]}")
            if s and l and l != 0:
                pct = (s - l) / l
                tol = cfg["tolerance"].get("ma_pair_pct", 0.002)
                sub = float(np.tanh((pct / 0.02) * 2.0)) if abs(pct) > tol else 0.0
                scores.append((sub, weights.get("ma_pair", 1.0)))

        macd_hist = last.get("macd_hist")
        if macd_hist is not None and not pd.isna(macd_hist):
            hist_series = df["macd_hist"].dropna()
            denom = hist_series.std() if len(hist_series) > 0 and hist_series.std() != 0 else 1.0
            scores.append((float(np.tanh(macd_hist / denom)), weights.get("macd", 1.5)))

        num = sum(s * w for (s, w) in scores)
        denom = sum(abs(w) for (_, w) in scores) or 1.0
        score = float(num / denom)
        
        t = cfg.get("thresholds", {"strong_buy": 0.6, "buy": 0.25, "sell": -0.25, "strong_sell": -0.6})
        if score >= t["strong_buy"]: return score, "Strong Buy"
        if score >= t["buy"]: return score, "Buy"
        if score <= t["strong_sell"]: return score, "Strong Sell"
        if score <= t["sell"]: return score, "Sell"
        return score, "Neutral"
    except Exception:
        return 0.0, "Neutral"


def compute_mtf_alignment(get_closes_fn: Callable[[str], List[float]], price: float, mtf_tfs: List[str], mtf_slope_lookback: int = 3) -> Dict[str, AnyT]:
    tf_states = {}
    negative_tfs = []
    for tf in mtf_tfs:
        closes = get_closes_fn(tf) or []
        _, _, hist = compute_macd_from_closes(closes, include_price=price)
        hist = hist or []
        cur, prev = (hist[-1] if hist else None), (hist[-2] if len(hist) >= 2 else None)
        is_positive = cur is not None and cur > 0
        tf_states[tf] = {"cur": cur, "prev": prev, "is_positive": is_positive}
        if not is_positive: negative_tfs.append(tf)

    if not negative_tfs: return {"status": "aligned", "tfs": tf_states, "negative_tfs": []}
    return {"status": "monitoring", "tfs": tf_states, "negative_tfs": negative_tfs}
